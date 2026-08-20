package quic

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// kuSuite is the AES-128-GCM suite used for the key-update tests.
const kuSuite = tls.TLS_AES_128_GCM_SHA256

// newKUTestConn builds a Conn in the post-handshake application-receive state
// with key-update state installed, as SetReadKeys/SetWriteKeys would leave it.
// readSecret is the server→client (client read) 1-RTT secret; writeSecret is the
// client→server one.
func newKUTestConn(t *testing.T, readSecret, writeSecret []byte) *Conn {
	t.Helper()
	rk, err := KeysFromSecret(kuSuite, readSecret)
	require.NoError(t, err, "derive the client's 1-RTT read keys")
	rop, err := NewOpener(rk)
	require.NoError(t, err, "NewOpener for the client's read keys")
	wk, err := KeysFromSecret(kuSuite, writeSecret)
	require.NoError(t, err, "derive the client's 1-RTT write keys")
	wsl, err := NewSealer(wk)
	require.NoError(t, err, "NewSealer for the client's write keys")
	c := &Conn{
		dcid:               []byte("keyupdt0"),
		handshakeComplete:  true,
		handshakeConfirmed: true,
		peer:               TransportParams{InitialMaxStreamsBidi: 1, InitialMaxStreamsUni: 3},
	}
	c.keys.OneRTT = rop
	c.oneRTTSealer = wsl
	require.NoError(t, c.initAppReadKU(kuSuite, readSecret, rop.hp), "install the read key-update state")
	require.NoError(t, c.initAppWriteKU(kuSuite, writeSecret, wsl.hp), "install the write key-update state")
	return c
}

// serverGen returns a server send Sealer for read-generation n (n=0 the initial
// 1-RTT secret, n=1 after one "quic ku" ratchet, ...), all sharing generation
// 0's header-protection key — the server, like the client, does not rotate it.
func serverGen(t *testing.T, readSecret []byte, n int) *Sealer {
	t.Helper()
	secret := append([]byte(nil), readSecret...)
	for i := 0; i < n; i++ {
		secret = quicKuSecret(sha256.New, secret)
	}
	k, err := KeysFromSecret(kuSuite, secret)
	require.NoErrorf(t, err, "derive the server's generation-%d keys", n)
	if n == 0 {
		s, err := NewSealer(k)
		require.NoError(t, err, "NewSealer for the server's generation-0 keys")
		return s
	}
	gen0, err := KeysFromSecret(kuSuite, readSecret)
	require.NoError(t, err, "derive generation 0 for its header-protection key")
	hp0, err := NewSealer(gen0)
	require.NoError(t, err, "NewSealer for generation 0's header protection")
	s, err := sealerWithHP(k, hp0.hp)
	require.NoErrorf(t, err, "build the generation-%d sealer on generation 0's HP key", n)
	return s
}

// sealKP seals a short-header 1-RTT packet with the given Key Phase bit.
func sealKP(t *testing.T, s *Sealer, dcid []byte, pn uint64, keyPhase bool, frames []byte) []byte {
	t.Helper()
	const minFrames = 20
	if len(frames) < minFrames {
		frames = append(frames, make([]byte, minFrames-len(frames))...)
	}
	pnLen := 4
	hdr, pnOff := AppendShortHeader(nil, dcid, pnLen, keyPhase)
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	pkt, err := s.Seal(nil, hdr, pnOff, pnLen, pn, frames)
	require.NoErrorf(t, err, "seal a short-header packet at pn %d with key phase %v", pn, keyPhase)
	return pkt
}

// TestConformance_RFC9001_Sec62_KeyUpdateResponder drives the client's response
// to a peer-initiated key update (RFC 9001 §6.2) — exactly quic-go/Caddy's
// assured update. A phase-0 packet decrypts with the current keys; a phase-1
// packet sealed with the "quic ku"-ratcheted secret is trial-decrypted with the
// pre-derived next generation, committed (the phase flips), and the client's own
// send phase flips so its acknowledgements are readable by the server.
func TestConformance_RFC9001_Sec62_KeyUpdateResponder(t *testing.T) {
	readSecret := bytes.Repeat([]byte{0x11}, 32)
	writeSecret := bytes.Repeat([]byte{0x22}, 32)
	c := newKUTestConn(t, readSecret, writeSecret)
	s, err := c.OpenStream()
	require.NoError(t, err, "OpenStream for the stream the server writes on")
	g0 := serverGen(t, readSecret, 0)
	g1 := serverGen(t, readSecret, 1)
	p0 := sealKP(t, g0, nil, 0, false, AppendStream(nil, 0, 0, false, []byte("aaaa")))
	p1 := sealKP(t, g1, nil, 1, true, AppendStream(nil, 0, 4, true, []byte("bbbb")))

	require.NoError(t, c.recvDatagram(p0), "recv phase-0")
	phaseBefore := c.ku.phase
	require.NoError(t, c.recvDatagram(p1), "recv phase-1 (key update)")

	assert.False(t, phaseBefore, "key phase should still be 0 before any update")
	assert.True(t, c.ku.phase, "key phase should have flipped to 1 after the update committed")
	assert.True(t, c.appSendPhase(),
		"client send phase should have flipped so its ACKs use the new keys")
	var body []byte
	for _, chunk := range [][]byte{s.Recv(), s.Recv()} {
		body = append(body, chunk...)
	}
	assert.Equalf(t, "aaaabbbb", string(body),
		"reassembled %q across the key-update boundary, want %q", string(body), "aaaabbbb")
	assert.True(t, s.Finished(), "stream should be finished")
}

// TestConformance_RFC9001_Sec63_PrevKeysReordered checks that after committing a
// key update, a reordered packet from the previous generation (packet number
// below the boundary) still decrypts with the retained prev keys, and that the
// prev keys are discarded once the 3×PTO retention window elapses (§6.3).
func TestConformance_RFC9001_Sec63_PrevKeysReordered(t *testing.T) {
	readSecret := bytes.Repeat([]byte{0x33}, 32)
	writeSecret := bytes.Repeat([]byte{0x44}, 32)
	c := newKUTestConn(t, readSecret, writeSecret)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }
	s, err := c.OpenStream()
	require.NoError(t, err, "OpenStream for the stream the server writes on")
	g0 := serverGen(t, readSecret, 0)
	g1 := serverGen(t, readSecret, 1)
	// phase 0 (pn 0), then a phase-1 update at pn 5 (boundary = 5).
	require.NoError(t, c.recvDatagram(sealKP(t, g0, nil, 0, false, AppendStream(nil, 0, 0, false, []byte("aaaa")))),
		"recv the phase-0 packet")
	require.NoError(t, c.recvDatagram(sealKP(t, g1, nil, 5, true, AppendStream(nil, 0, 8, false, []byte("cccc")))),
		"recv the phase-1 update at pn 5")
	require.Truef(t, c.ku.phase && c.ku.boundary == 5,
		"post-update: phase=%v boundary=%d, want phase=1 boundary=5", c.ku.phase, c.ku.boundary)

	// A reordered previous-generation (phase 0) packet at pn 2 fills the gap.
	reordered := c.recvDatagram(sealKP(t, g0, nil, 2, false, AppendStream(nil, 0, 4, false, []byte("bbbb"))))

	require.NoError(t, reordered, "a reordered prev-generation packet must not error")
	var body []byte
	for i := 0; i < 3; i++ {
		body = append(body, s.Recv()...)
	}
	assert.Equalf(t, "aaaabbbbcccc", string(body),
		"reassembled %q, want %q (reordered prev-gen packet must decrypt)", string(body), "aaaabbbbcccc")

	// After the retention window, prev keys are dropped and a late old-gen packet
	// no longer decrypts.
	now = now.Add(10 * time.Second)
	c.discardStaleKeys()
	late := sealKP(t, g0, nil, 3, false, AppendStream(nil, 0, 12, false, []byte("dddd")))

	lateErr := c.recvDatagram(late)

	assert.Nil(t, c.ku.prev, "prev keys should be discarded after 3×PTO (§6.3)")
	require.NoError(t, lateErr, "a late old-gen packet must be dropped silently, not error")
	body = body[:0]
	for i := 0; i < 3; i++ {
		body = append(body, s.Recv()...)
	}
	assert.Emptyf(t, body,
		"late old-gen packet after discard should be dropped, but delivered %q", body)
}

// TestConformance_RFC9001_Sec64_ForgedPhaseBitBounded checks that packets with a
// flipped Key Phase bit but forged ciphertext fail with a single AEAD attempt and
// never trigger a key update (§6.4 anti-amplification of trial decryption).
func TestConformance_RFC9001_Sec64_ForgedPhaseBitBounded(t *testing.T) {
	readSecret := bytes.Repeat([]byte{0x55}, 32)
	writeSecret := bytes.Repeat([]byte{0x66}, 32)
	c := newKUTestConn(t, readSecret, writeSecret)
	_, err := c.OpenStream()
	require.NoError(t, err, "OpenStream for the stream the server writes on")
	g0 := serverGen(t, readSecret, 0)
	// Establish the current generation with a legitimate phase-0 packet.
	require.NoError(t, c.recvDatagram(sealKP(t, g0, nil, 0, false, AppendStream(nil, 0, 0, false, []byte("aaaa")))),
		"recv the legitimate phase-0 packet")

	// Forge phase-1 packets sealed with the WRONG (gen-0) keys: they trial-decrypt
	// against the next generation, fail, and must be dropped without committing.
	for i := 0; i < 8; i++ {
		forged := sealKP(t, g0, nil, uint64(10+i), true, AppendStream(nil, 0, 4, false, []byte("xxxx")))

		err := c.recvDatagram(forged)

		require.NoErrorf(t, err, "forged packet %d must be skipped, not error", i)
		require.Falsef(t, c.ku.phase,
			"a forged Key Phase bit must not trigger a key update (forged packet %d)", i)
	}
}

// TestConformance_RFC9001_Sec61_UpdateBeforeConfirmed checks that a key update
// is not accepted before the handshake is confirmed (HANDSHAKE_DONE received,
// RFC 9001 §4.1.2 / §6.1): TLS completion alone (handshakeComplete) is not enough.
func TestConformance_RFC9001_Sec61_UpdateBeforeConfirmed(t *testing.T) {
	readSecret := bytes.Repeat([]byte{0x77}, 32)
	writeSecret := bytes.Repeat([]byte{0x88}, 32)
	c := newKUTestConn(t, readSecret, writeSecret)
	c.handshakeConfirmed = false // TLS done but HANDSHAKE_DONE not yet received
	_, err := c.OpenStream()
	require.NoError(t, err, "OpenStream for the stream the server writes on")
	g1 := serverGen(t, readSecret, 1)

	// A well-formed phase-1 update packet must be dropped, not committed.
	recvErr := c.recvDatagram(sealKP(t, g1, nil, 0, true, AppendStream(nil, 0, 0, false, []byte("aaaa"))))

	require.NoError(t, recvErr, "the update must be dropped silently, not error")
	assert.False(t, c.ku.phase, "a key update before handshake confirmation must not commit (§6.1)")
}

// TestKeyUpdate_QuicKuVector pins the key-update secret derivation (RFC 9001
// §6.1): secret_{n+1} = HKDF-Expand-Label(secret_n, "quic ku", "", Hash.length),
// full-length and deterministic.
func TestKeyUpdate_QuicKuVector(t *testing.T) {
	secret := bytes.Repeat([]byte{0xab}, 32)
	want := hkdfExpandLabel(sha256.New, secret, "quic ku", 32)

	got := quicKuSecret(sha256.New, secret)

	assert.Equal(t, want, got,
		"quicKuSecret must equal HKDF-Expand-Label(secret, \"quic ku\", 32) — RFC 9001 §6.1")
	assert.Lenf(t, got, 32, "quic ku secret length = %d, want 32 (Hash.length)", len(got))
	assert.NotEqual(t, secret, got, "ratcheted secret must differ from the input")
}
