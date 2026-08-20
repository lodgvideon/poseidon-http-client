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
