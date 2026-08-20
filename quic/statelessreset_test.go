package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec103_StatelessResetTokenParsed checks that the peer's
// stateless_reset_token transport parameter (0x02) is parsed as a 16-byte value,
// and a wrong length is a TRANSPORT_PARAMETER_ERROR (RFC 9000 §10.3, §18.2).
func TestConformance_RFC9000_Sec103_StatelessResetTokenParsed(t *testing.T) {
	var token [16]byte
	copy(token[:], "fedcba9876543210")

	tp, err := ParseTransportParams(tpBytes(tpStatelessResetToken, token[:]))
	_, errShort := ParseTransportParams(tpBytes(tpStatelessResetToken, token[:8]))

	require.NoError(t, err, "ParseTransportParams with a 16-byte stateless_reset_token")
	assert.True(t, tp.HaveStatelessResetToken,
		"stateless_reset_token should be recorded as present")
	assert.Equalf(t, token, tp.StatelessResetToken,
		"token = %x, want %x", tp.StatelessResetToken, token)
	assert.Truef(t, errShort == ErrTransportParameter,
		"an 8-byte token: err = %v, want ErrTransportParameter", errShort)
}

// TestConformance_RFC9000_Sec1031_StatelessResetDetected checks that a datagram
// that cannot be decrypted and ends in the reset token bound to the connection ID
// in use is recognized as a stateless reset and tears the connection down; that a
// non-matching datagram is only dropped; and — the §10.3.1 MUST NOT — that a token
// for a connection ID that has been retired is no longer matched.
func TestConformance_RFC9000_Sec1031_StatelessResetDetected(t *testing.T) {
	dcid := []byte("srtest00")
	keys, _ := InitialKeys(dcid)
	opener, _ := NewOpener(keys)

	newConn := func() *Conn {
		c := &Conn{pc: &closePC{}, dcid: append([]byte(nil), dcid...), handshakeComplete: true}
		c.keys.OneRTT = opener
		c.serverCIDs = map[uint64][]byte{0: append([]byte(nil), dcid...)}
		return c
	}
	// A stateless reset: a short-header-form datagram that fails AEAD and ends in tok.
	resetPkt := func(tok [16]byte) []byte {
		p := make([]byte, 40)
		p[0] = 0x40 // short header form (high bit clear), fixed bit set
		copy(p[len(p)-16:], tok[:])
		return p
	}

	var tok0, tok1 [16]byte
	copy(tok0[:], "0123456789abcdef")
	copy(tok1[:], "fedcba9876543210")

	// The token bound to the CID in use (seq 0) detects a reset and closes the conn.
	c := newConn()
	c.registerResetToken(0, tok0)

	errInUse := c.recvDatagram(resetPkt(tok0))

	assert.Truef(t, errInUse == ErrStatelessReset,
		"in-use token: recvDatagram = %v, want ErrStatelessReset", errInUse)
	assert.True(t, c.closed, "a detected stateless reset must close the connection")

	// A NEW_CONNECTION_ID that retires seq 0 and switches the CID in use to seq 1:
	// the retired seq-0 token MUST NOT be matched, but the new seq-1 token must be.
	c2 := newConn()
	c2.registerResetToken(0, tok0) // the handshake CID's token
	require.NoError(t, (&connFrameHandler{c: c2}).OnNewConnectionID(1, 1, []byte("newcid01"), &tok1),
		"OnNewConnectionID")
	require.EqualValuesf(t, 1, c2.curCIDSeq,
		"expected a switch to CID seq 1, got seq %d", c2.curCIDSeq)

	errRetired := c2.recvDatagram(resetPkt(tok0))
	closedByRetired := c2.closed
	errSeq1 := c2.recvDatagram(resetPkt(tok1))

	assert.NoErrorf(t, errRetired,
		"retired token: recvDatagram = %v, want nil (§10.3.1 MUST NOT check)", errRetired)
	assert.False(t, closedByRetired,
		"a retired CID's token must not trigger a stateless reset")
	assert.Truef(t, errSeq1 == ErrStatelessReset,
		"in-use seq-1 token: recvDatagram = %v, want ErrStatelessReset", errSeq1)

	// A datagram whose trailing bytes match no armed token is just dropped.
	c3 := newConn()
	c3.registerResetToken(0, tok0)

	errNoMatch := c3.recvDatagram(resetPkt([16]byte{}))

	assert.NoErrorf(t, errNoMatch, "non-matching datagram: recvDatagram = %v, want nil", errNoMatch)
	assert.False(t, c3.closed,
		"a non-matching undecryptable datagram must not close the connection")
}

// TestConformance_RFC9000_Sec1031_ResetTokenMatchedOverAllSixteenBytes pins that
// the stateless-reset comparison covers the WHOLE token.
//
// RFC 9000 §10.3.1 defines the check over the whole value: "The endpoint
// identifies a received datagram as a Stateless Reset by comparing the last 16
// bytes of the datagram with all stateless reset tokens associated with the
// remote address on which the datagram was received."
//
// The test above uses two non-matching tokens — an all-zero one and one that
// differs from the first byte on — and both are refused by any comparison that
// looks at even a single byte. Truncating the comparison to eight bytes left the
// whole suite green. A partial match is not a cosmetic bug: it lets an off-path
// attacker tear a connection down with a 2^64 search instead of 2^128. #853.
//
// Two near misses, one at each end, so neither a head-truncated nor a
// tail-truncated comparison can pass: each differs from the armed token in
// exactly one byte, and each must be a plain drop.
func TestConformance_RFC9000_Sec1031_ResetTokenMatchedOverAllSixteenBytes(t *testing.T) {
	dcid := []byte("srtest16")
	keys, _ := InitialKeys(dcid)
	opener, _ := NewOpener(keys)
	newConn := func() *Conn {
		c := &Conn{pc: &closePC{}, dcid: append([]byte(nil), dcid...), handshakeComplete: true}
		c.keys.OneRTT = opener
		c.serverCIDs = map[uint64][]byte{0: append([]byte(nil), dcid...)}
		return c
	}
	resetPkt := func(tok [16]byte) []byte {
		p := make([]byte, 40)
		p[0] = 0x40 // short header form (high bit clear), fixed bit set
		copy(p[len(p)-16:], tok[:])
		return p
	}
	var armed [16]byte
	copy(armed[:], "0123456789abcdef")
	lastByteDiffers, firstByteDiffers := armed, armed
	lastByteDiffers[15] ^= 0x01 // a tail-truncated comparison would accept this
	firstByteDiffers[0] ^= 0x01 // a head-truncated comparison would accept this

	for _, tc := range []struct {
		name string
		tok  [16]byte
	}{
		{"differs_only_in_the_last_byte", lastByteDiffers},
		{"differs_only_in_the_first_byte", firstByteDiffers},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newConn()
			c.registerResetToken(0, armed)
			require.NotEqualf(t, armed, tc.tok,
				"the fixture's near miss must actually differ from the armed token")

			err := c.recvDatagram(resetPkt(tc.tok))

			assert.NoErrorf(t, err,
				"a token differing from the armed one in one byte was accepted as a "+
					"stateless reset (%v): the comparison is not covering all 16 bytes, "+
					"which cuts the search an off-path attacker needs from 2^128 to 2^64", err)
			assert.Falsef(t, c.closed,
				"a near-miss token tore the connection down; any off-path party who can "+
					"guess a prefix could then kill the connection")
		})
	}

	// Control: the exact token on the same fixture MUST still be detected, or the
	// two arms above would pass for a comparison that matches nothing at all.
	t.Run("control_exact_token_still_detected", func(t *testing.T) {
		c := newConn()
		c.registerResetToken(0, armed)

		err := c.recvDatagram(resetPkt(armed))

		assert.Truef(t, err == ErrStatelessReset,
			"the armed token = %v, want ErrStatelessReset — without this arm the "+
				"near-miss cases are satisfied by a check that never matches", err)
	})
}

// TestConformance_RFC9000_Sec1031_RetiredTokenIsDeleted gates the §10.3.1 line
// itself rather than a consequence of another invariant.
//
// The retired-CID arm of TestConformance_RFC9000_Sec1031_StatelessResetDetected
// says in its comment that the retired seq-0 token MUST NOT be matched, but it
// cannot show that: isStatelessReset looks up ONLY c.resetTokens[c.curCIDSeq],
// and by then curCIDSeq is 1, so resetTokens[0] is never consulted whether or not
// it is still there. Deleting the `delete(c.resetTokens, s)` line in
// onNewConnectionID left the whole suite green — through the public surface it is
// an equivalent mutant, because switchToActiveCID chooses from serverCIDs and the
// retired sequence is deleted from that map too.
//
// RFC 9000 §10.3.1 states the obligation directly, and it is about what is
// REMEMBERED, not only about what is looked up: "An endpoint MUST NOT check for
// any stateless reset tokens associated with connection IDs it has not used or
// for connection IDs that have been retired." So assert the map, which is the
// defence-in-depth the line exists to provide. #855.
func TestConformance_RFC9000_Sec1031_RetiredTokenIsDeleted(t *testing.T) {
	var tok0, tok1 [16]byte
	copy(tok0[:], "0123456789abcdef")
	copy(tok1[:], "fedcba9876543210")
	c := &Conn{dcid: []byte("srtest00")}
	c.serverCIDs = map[uint64][]byte{0: append([]byte(nil), c.dcid...)}
	c.registerResetToken(0, tok0)
	require.Contains(t, c.resetTokens, uint64(0),
		"the fixture must arm seq 0 first, or the deletion below is unobservable")

	// A NEW_CONNECTION_ID at seq 1 whose Retire Prior To is 1 retires seq 0.
	err := (&connFrameHandler{c: c}).OnNewConnectionID(1, 1, []byte("newcid01"), &tok1)

	require.NoError(t, err, "OnNewConnectionID retiring seq 0")
	assert.NotContainsf(t, c.resetTokens, uint64(0),
		"the retired sequence 0 token is still armed (%d tokens held): §10.3.1 forbids "+
			"remembering it, and keeping it leaves the connection one lookup-scoping "+
			"change away from matching a token it must never match", len(c.resetTokens))
	assert.Containsf(t, c.resetTokens, uint64(1),
		"the newly issued sequence 1 token must be armed, or the deletion above is "+
			"just the map being emptied")
	assert.NotContains(t, c.serverCIDs, uint64(0),
		"the retired connection ID itself must also be gone from serverCIDs")
}
