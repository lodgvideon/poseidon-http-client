package quic

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoErrorf(t, err, "decode the hex fixture %q", s)
	return b
}

// The Retry example from RFC 9001 Appendix A.4: the integrity tag is computed
// over the original destination connection ID 0x8394c8f03e515708.
const (
	a4RetryPacket = "ff000000010008f067a5502a4262b5746f6b656e04a265ba2eff4d829058fb3f0f2496ba"
	a4ODCID       = "8394c8f03e515708"
)

// TestConformance_RFC9001_Sec58_RetryIntegrityKAT verifies the Retry Integrity
// Tag against the RFC 9001 Appendix A.4 known-answer vector, and that a tampered
// packet or wrong original DCID fails.
func TestConformance_RFC9001_Sec58_RetryIntegrityKAT(t *testing.T) {
	pkt := mustHex(t, a4RetryPacket)
	odcid := mustHex(t, a4ODCID)
	tampered := append([]byte(nil), pkt...)
	tampered[16] ^= 0x01 // a single flipped byte in the token

	kat := verifyRetryIntegrity(odcid, pkt)
	tamperedOK := verifyRetryIntegrity(odcid, tampered)
	wrongODCID := verifyRetryIntegrity(mustHex(t, "0000000000000000"), pkt)

	require.True(t, kat, "A.4 Retry integrity tag should verify")
	assert.False(t, tamperedOK, "a tampered Retry must not verify")
	assert.False(t, wrongODCID, "a Retry must not verify against the wrong original DCID")
}

// TestConformance_RFC9000_Sec1725_RetryRekeysAndResends checks that a valid Retry
// adopts the server's connection ID, re-derives the Initial keys, stores the
// token, and re-queues the ClientHello for resend (RFC 9000 §17.2.5).
func TestConformance_RFC9000_Sec1725_RetryRekeysAndResends(t *testing.T) {
	pkt := mustHex(t, a4RetryPacket)
	odcid := mustHex(t, a4ODCID)
	client, _ := InitialKeys(odcid)
	sealer, _ := NewSealer(client)
	c := &Conn{dcid: odcid, initialSealer: sealer}
	c.keys.Initial, _ = NewOpener(client)
	// Simulate the first Initial in flight, carrying the ClientHello CRYPTO.
	c.sent[spaceInitial].onSent(0, c.clock(), true, &retransFrame{kind: retransCrypto, offset: 0, data: []byte("clienthello")})
	c.bytesInFlight = 1200
	hdr, err := ParseHeader(pkt, len(c.scid))
	require.NoError(t, err, "parse the A.4 Retry header")

	c.handleRetry(hdr, pkt)

	require.True(t, c.handledRetry, "handledRetry should be set after a valid Retry")
	assert.Equalf(t, "f067a5502a4262b5", hex.EncodeToString(c.dcid),
		"dcid = %s, want the Retry SCID f067a5502a4262b5", hex.EncodeToString(c.dcid))
	assert.Equalf(t, "f067a5502a4262b5", hex.EncodeToString(c.retrySCID),
		"retrySCID = %s, want the Retry SCID f067a5502a4262b5 (for §7.3 validation)",
		hex.EncodeToString(c.retrySCID))
	assert.Equalf(t, "token", string(c.retryToken), "retryToken = %q, want \"token\"", c.retryToken)
	assert.Empty(t, c.sent[spaceInitial].packets, "the outstanding Initial should be abandoned")
	assert.NotEmpty(t, c.retransQueue[spaceInitial], "the ClientHello should be re-queued for resend")
	// The re-derived sealer must match keys derived from the new connection ID.
	newClient, _ := InitialKeys(mustHex(t, "f067a5502a4262b5"))
	want, _ := NewSealer(newClient)
	assert.Equal(t, want.iv, c.initialSealer.iv,
		"Initial keys should be re-derived from the new connection ID")
}

// TestConformance_RFC9000_Sec17253_RetryResendCarriesToken checks that the
// Initial resent after a Retry carries the token and is sealed under the keys
// derived from the new connection ID (RFC 9000 §17.2.5.3).
func TestConformance_RFC9000_Sec17253_RetryResendCarriesToken(t *testing.T) {
	pkt := mustHex(t, a4RetryPacket)
	odcid := mustHex(t, a4ODCID)
	client, _ := InitialKeys(odcid)
	sealer, _ := NewSealer(client)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: append([]byte(nil), odcid...), initialSealer: sealer}
	c.keys.Initial, _ = NewOpener(client)
	c.sent[spaceInitial].onSent(0, c.clock(), true, &retransFrame{kind: retransCrypto, offset: 0, data: []byte("clienthello")})
	hdr, _ := ParseHeader(pkt, 0)

	c.handleRetry(hdr, pkt)
	flushErr := c.flush()

	require.NoError(t, flushErr, "flush the Initial resent after the Retry")
	require.Lenf(t, pc.writes, 1, "wrote %d packets, want 1 resent Initial", len(pc.writes))
	rh, err := ParseHeader(pc.writes[0], 0)
	require.NoError(t, err, "the resent packet must parse as a QUIC packet")
	assert.Equalf(t, PacketInitial, rh.Type, "resent packet type = %v, want Initial", rh.Type)
	assert.Equalf(t, "token", string(rh.Token), "resent Initial token = %q, want \"token\"", rh.Token)
	assert.GreaterOrEqualf(t, len(pc.writes[0]), InitialDatagramMinSize,
		"resent Initial datagram %d bytes, want >= %d (§14.1 padding)",
		len(pc.writes[0]), InitialDatagramMinSize)
	// The server unprotects the client's Initial with the CLIENT keys derived from
	// the new connection ID — the ones the client sealed with.
	newClient, _ := InitialKeys(mustHex(t, "f067a5502a4262b5"))
	opener, _ := NewOpener(newClient)
	_, _, _, openErr := opener.Open(pc.writes[0][:rh.PacketLen], rh.PNOffset, 0)
	assert.NoErrorf(t, openErr, "resent Initial should decrypt with the new keys: %v", openErr)
}

// TestConn_Retry_Discards checks the discard rules (RFC 9000 §17.2.5.2): a Retry
// with a failing tag, an empty token, an SCID equal to the Initial DCID, or a
// second Retry is ignored.
func TestConn_Retry_Discards(t *testing.T) {
	pkt := mustHex(t, a4RetryPacket)
	odcid := mustHex(t, a4ODCID)
	newConn := func() *Conn {
		client, _ := InitialKeys(odcid)
		sealer, _ := NewSealer(client)
		c := &Conn{dcid: append([]byte(nil), odcid...), initialSealer: sealer}
		c.keys.Initial, _ = NewOpener(client)
		return c
	}

	t.Run("bad_integrity_tag", func(t *testing.T) {
		c := newConn()
		bad := append([]byte(nil), pkt...)
		bad[len(bad)-1] ^= 0x01
		hdr, _ := ParseHeader(bad, 0)

		c.handleRetry(hdr, bad)

		assert.False(t, c.handledRetry, "a Retry with a bad tag must be discarded")
		assert.Equal(t, a4ODCID, hex.EncodeToString(c.dcid),
			"a Retry with a bad tag must not change the connection ID")
	})

	t.Run("second_retry", func(t *testing.T) {
		c := newConn()
		hdr, _ := ParseHeader(pkt, 0)
		c.handleRetry(hdr, pkt) // first, accepted
		firstDCID := hex.EncodeToString(c.dcid)

		c.handleRetry(hdr, pkt) // second, ignored

		assert.Equal(t, firstDCID, hex.EncodeToString(c.dcid), "a second Retry must be discarded")
	})

	// A Retry after a server Initial has been processed (gotServerCID) → ignored,
	// so an injected Retry cannot corrupt the adopted connection ID (§17.2.5.2).
	t.Run("after_server_initial", func(t *testing.T) {
		c := newConn()
		c.gotServerCID = true
		hdr, _ := ParseHeader(pkt, 0)

		c.handleRetry(hdr, pkt)

		assert.False(t, c.handledRetry, "a Retry after a server Initial must be discarded")
		assert.Equal(t, a4ODCID, hex.EncodeToString(c.dcid),
			"a Retry after a server Initial must not change the connection ID")
	})
}
