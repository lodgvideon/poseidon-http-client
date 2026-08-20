package quic

import (
	"bytes"
	"crypto/aes"
	"crypto/sha256"
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9001_AppA5_ChaCha20 pins the ChaCha20-Poly1305 short-header
// packet protection to the RFC 9001 Appendix A.5 known-answer vectors: key/iv/hp
// derivation from the traffic secret, the "quic ku" ratchet secret, the 5-byte
// header-protection mask from the RFC's sample (§5.4.4), and the full protected
// packet produced by Seal (AEAD_CHACHA20_POLY1305 + header protection).
func TestConformance_RFC9001_AppA5_ChaCha20(t *testing.T) {
	secret := unhex(t, "9ac312a7f877468ebe69422748ad00a15443f18203a07d6060f688f30f21632b")

	keys, err := KeysFromSecret(tls.TLS_CHACHA20_POLY1305_SHA256, secret)
	require.NoErrorf(t, err, "KeysFromSecret(ChaCha20): %v", err)
	sealer, err := NewSealer(keys)
	require.NoErrorf(t, err, "NewSealer(ChaCha20): %v", err)
	// Header-protection mask from the RFC's 16-byte sample (RFC 9001 §5.4.4).
	var hpKey [32]byte
	copy(hpKey[:], keys.HP)
	var mask [5]byte
	// Full protected packet via Seal: empty DCID, packet number 654360564 encoded
	// on 3 bytes (0x00bff4), a single PING frame (0x01) payload.
	hdr := unhex(t, "4200bff4") // first byte 0x42, then the 3-byte packet number

	ku := quicKuSecret(sha256.New, secret)
	(&chachaHeaderProtector{key: hpKey}).headerMask(unhex(t, "5e5cd55c41f69080575d7999c25a5bfb"), &mask)
	pkt, err := sealer.Seal(nil, hdr, 1, 3, 654360564, unhex(t, "01"))

	for _, c := range []struct {
		name string
		got  []byte
		want string
	}{
		{"key", keys.Key, "c6d98ff3441c3fe1b2182094f69caa2ed4b716b65488960a7a984979fb23e1c8"},
		{"iv", keys.IV, "e0459b3474bdd0e44a41c144"},
		{"hp", keys.HP, "25a282b9e82f06f21f488917a4fc8f1b73573685608597d0efcb076b0ab7a7a4"},
	} {
		assert.Truef(t, bytes.Equal(c.got, unhex(t, c.want)), "%s = %x, want %s", c.name, c.got, c.want)
	}
	// The "quic ku" ratchet secret (A.5's ku value), used at the next key update.
	wantKu := unhex(t, "1223504755036d556342ee9361d253421a826c9ecdf3c7148684b36b714881f9")
	assert.Truef(t, bytes.Equal(ku, wantKu), "ku secret = %x, want %x", ku, wantKu)
	wantMask := unhex(t, "aefefe7d03")
	assert.Truef(t, bytes.Equal(mask[:], wantMask), "header mask = %x, want %x", mask[:], wantMask)
	require.NoErrorf(t, err, "Seal: %v", err)
	wantPkt := unhex(t, "4cfe4189655e5cd55c41f69080575d7999c25a5bfb")
	assert.Truef(t, bytes.Equal(pkt, wantPkt), "A.5 protected packet =\n %x\nwant\n %x", pkt, wantPkt)
}

// TestChaCha20_SealOpenRoundTrip seals then opens a ChaCha20-Poly1305 1-RTT packet
// with keys derived from a traffic secret, recovering the exact packet number and
// payload and confirming header protection is applied and reversible.
func TestChaCha20_SealOpenRoundTrip(t *testing.T) {
	keys, err := KeysFromSecret(tls.TLS_CHACHA20_POLY1305_SHA256, bytes.Repeat([]byte{0x5a}, 32))
	require.NoErrorf(t, err, "KeysFromSecret: %v", err)
	sealer, err := NewSealer(keys)
	require.NoErrorf(t, err, "NewSealer: %v", err)
	opener, err := NewOpener(keys)
	require.NoErrorf(t, err, "NewOpener: %v", err)
	pn := uint64(0x1234)
	pnLen := 2
	payload := []byte("a chacha20-poly1305 protected quic payload, long enough to sample")
	hdr, pnOff := AppendShortHeader(nil, []byte("chacha00"), pnLen, false)
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	sealed, err := sealer.Seal(nil, hdr, pnOff, pnLen, pn, payload)
	require.NoErrorf(t, err, "Seal: %v", err)

	gotPN, gotPNLen, gotPayload, err := opener.Open(append([]byte(nil), sealed...), pnOff, pn-1)

	// Header protection must have altered the first byte and/or packet number.
	assert.Falsef(t, sealed[0] == hdr[0] && sealed[pnOff] == hdr[pnOff] && sealed[pnOff+1] == hdr[pnOff+1],
		"header protection appears to be a no-op")
	require.NoErrorf(t, err, "Open: %v", err)
	assert.Truef(t, gotPN == pn && gotPNLen == pnLen && bytes.Equal(gotPayload, payload),
		"Open = pn=%d pnLen=%d payload=%q; want %d/%d/%q", gotPN, gotPNLen, gotPayload, pn, pnLen, payload)
}

// TestKeysFromSecret_ChaCha20 checks the ChaCha20-Poly1305 suite now derives keys
// (32-byte AEAD + HP keys, 12-byte IV) and stamps its suite, rather than the old
// ErrCryptoSuite refusal.
func TestKeysFromSecret_ChaCha20(t *testing.T) {
	secret := bytes.Repeat([]byte{0x01}, 32)

	keys, err := KeysFromSecret(tls.TLS_CHACHA20_POLY1305_SHA256, secret)

	require.NoErrorf(t, err, "KeysFromSecret(ChaCha20): %v, want keys", err)
	assert.Falsef(t, len(keys.Key) != 32 || len(keys.IV) != 12 || len(keys.HP) != 32,
		"key/iv/hp lengths = %d/%d/%d, want 32/12/32", len(keys.Key), len(keys.IV), len(keys.HP))
	assert.Equalf(t, tls.TLS_CHACHA20_POLY1305_SHA256, keys.Suite,
		"Suite = %#x, want TLS_CHACHA20_POLY1305_SHA256", keys.Suite)
}

// TestHeaderProtection_AES_ByteIdentical proves the aesHeaderProtector wrapper
// yields exactly the first 5 bytes of the raw single-block AES-ECB encryption of
// the sample — i.e. threading AES header protection through the headerProtector
// interface did not change the AES output byte-for-byte.
func TestHeaderProtection_AES_ByteIdentical(t *testing.T) {
	block, err := aes.NewCipher(unhex(t, "9f50449e04a0e810283a1e9933adedd2")) // A.1 client hp
	require.NoError(t, err)
	sample := unhex(t, "00112233445566778899aabbccddeeff")
	var raw [16]byte
	block.Encrypt(raw[:], sample)
	var got [5]byte

	(&aesHeaderProtector{block: block}).headerMask(sample, &got)

	assert.Truef(t, bytes.Equal(got[:], raw[:5]),
		"aesHeaderProtector mask = %x, want raw ECB first 5 bytes %x", got[:], raw[:5])
}

// newKUConnChaCha builds a post-handshake Conn with ChaCha20-Poly1305 1-RTT keys
// and key-update state installed, mirroring newKUTestConn (AES) for the ChaCha
// suite so the key-update ratchet is exercised with a non-AES AEAD/HP.
func newKUConnChaCha(t *testing.T, readSecret, writeSecret []byte) *Conn {
	t.Helper()
	const suite = tls.TLS_CHACHA20_POLY1305_SHA256
	rk, err := KeysFromSecret(suite, readSecret)
	require.NoError(t, err)
	rop, err := NewOpener(rk)
	require.NoError(t, err)
	wk, err := KeysFromSecret(suite, writeSecret)
	require.NoError(t, err)
	wsl, err := NewSealer(wk)
	require.NoError(t, err)
	c := &Conn{
		dcid:               []byte("chachaku"),
		handshakeComplete:  true,
		handshakeConfirmed: true,
		peer:               TransportParams{InitialMaxStreamsBidi: 1, InitialMaxStreamsUni: 3},
	}
	c.keys.OneRTT = rop
	c.oneRTTSealer = wsl
	require.NoError(t, c.initAppReadKU(suite, readSecret, rop.hp),
		"initAppReadKU(ChaCha20): the fixture cannot stage a key update without it")
	require.NoError(t, c.initAppWriteKU(suite, writeSecret, wsl.hp),
		"initAppWriteKU(ChaCha20): the fixture cannot stage a key update without it")
	return c
}

// serverGenChaCha is serverGen for the ChaCha20-Poly1305 suite: a server send
// Sealer for read-generation n, all generations sharing generation 0's
// (un-rotated) header-protection key.
func serverGenChaCha(t *testing.T, readSecret []byte, n int) *Sealer {
	t.Helper()
	const suite = tls.TLS_CHACHA20_POLY1305_SHA256
	secret := append([]byte(nil), readSecret...)
	for i := 0; i < n; i++ {
		secret = quicKuSecret(sha256.New, secret)
	}
	k, err := KeysFromSecret(suite, secret)
	require.NoError(t, err)
	if n == 0 {
		s, err := NewSealer(k)
		require.NoError(t, err, "NewSealer(ChaCha20, generation 0)")
		return s
	}
	gen0, err := KeysFromSecret(suite, readSecret)
	require.NoError(t, err)
	hp0, err := NewSealer(gen0)
	require.NoError(t, err)
	s, err := sealerWithHP(k, hp0.hp)
	require.NoError(t, err)
	return s
}

// TestChaCha20_KeyUpdateRoundTrip drives a peer-initiated key update (RFC 9001
// §6.2) over a ChaCha20-Poly1305 connection: a phase-0 packet decrypts with the
// current keys, a phase-1 packet sealed with the "quic ku"-ratcheted secret is
// trial-decrypted with the pre-derived next generation, and the phase flips — the
// next-generation keys must themselves be ChaCha20-Poly1305.
func TestChaCha20_KeyUpdateRoundTrip(t *testing.T) {
	readSecret := bytes.Repeat([]byte{0x1a}, 32)
	writeSecret := bytes.Repeat([]byte{0x2b}, 32)
	c := newKUConnChaCha(t, readSecret, writeSecret)
	s, err := c.OpenStream()
	require.NoError(t, err)
	g0 := serverGenChaCha(t, readSecret, 0)
	g1 := serverGenChaCha(t, readSecret, 1)
	require.NoError(t, c.recvDatagram(sealKP(t, g0, nil, 0, false,
		AppendStream(nil, 0, 0, false, []byte("aaaa")))), "recv phase-0")
	require.False(t, c.ku.phase, "key phase should still be 0 before any update")

	err = c.recvDatagram(sealKP(t, g1, nil, 1, true, AppendStream(nil, 0, 4, true, []byte("bbbb"))))

	require.NoErrorf(t, err, "recv phase-1 (key update): %v", err)
	assert.True(t, c.ku.phase, "key phase should have flipped to 1 after the update committed")
	assert.True(t, c.appSendPhase(),
		"client send phase should have flipped so its ACKs use the new keys")
	var body []byte
	for _, chunk := range [][]byte{s.Recv(), s.Recv()} {
		body = append(body, chunk...)
	}
	assert.Equalf(t, "aaaabbbb", string(body),
		"reassembled %q across the ChaCha20 key-update boundary, want %q", string(body), "aaaabbbb")
}
