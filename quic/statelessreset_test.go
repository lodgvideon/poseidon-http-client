package quic

import "testing"

// TestConformance_RFC9000_Sec103_StatelessResetTokenParsed checks that the peer's
// stateless_reset_token transport parameter (0x02) is parsed as a 16-byte value,
// and a wrong length is a TRANSPORT_PARAMETER_ERROR (RFC 9000 §10.3, §18.2).
func TestConformance_RFC9000_Sec103_StatelessResetTokenParsed(t *testing.T) {
	var token [16]byte
	copy(token[:], "fedcba9876543210")

	tp, err := ParseTransportParams(tpBytes(tpStatelessResetToken, token[:]))
	if err != nil {
		t.Fatal(err)
	}
	if !tp.HaveStatelessResetToken {
		t.Fatal("stateless_reset_token should be recorded as present")
	}
	if tp.StatelessResetToken != token {
		t.Fatalf("token = %x, want %x", tp.StatelessResetToken, token)
	}

	if _, err := ParseTransportParams(tpBytes(tpStatelessResetToken, token[:8])); err != ErrTransportParameter {
		t.Fatalf("an 8-byte token: err = %v, want ErrTransportParameter", err)
	}
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
	if err := c.recvDatagram(resetPkt(tok0)); err != ErrStatelessReset {
		t.Fatalf("in-use token: recvDatagram = %v, want ErrStatelessReset", err)
	}
	if !c.closed {
		t.Fatal("a detected stateless reset must close the connection")
	}

	// A NEW_CONNECTION_ID that retires seq 0 and switches the CID in use to seq 1:
	// the retired seq-0 token MUST NOT be matched, but the new seq-1 token must be.
	c2 := newConn()
	c2.registerResetToken(0, tok0) // the handshake CID's token
	if err := (&connFrameHandler{c: c2}).OnNewConnectionID(1, 1, []byte("newcid01"), &tok1); err != nil {
		t.Fatalf("OnNewConnectionID: %v", err)
	}
	if c2.curCIDSeq != 1 {
		t.Fatalf("expected a switch to CID seq 1, got seq %d", c2.curCIDSeq)
	}
	if err := c2.recvDatagram(resetPkt(tok0)); err != nil {
		t.Fatalf("retired token: recvDatagram = %v, want nil (§10.3.1 MUST NOT check)", err)
	}
	if c2.closed {
		t.Fatal("a retired CID's token must not trigger a stateless reset")
	}
	if err := c2.recvDatagram(resetPkt(tok1)); err != ErrStatelessReset {
		t.Fatalf("in-use seq-1 token: recvDatagram = %v, want ErrStatelessReset", err)
	}

	// A datagram whose trailing bytes match no armed token is just dropped.
	c3 := newConn()
	c3.registerResetToken(0, tok0)
	if err := c3.recvDatagram(resetPkt([16]byte{})); err != nil {
		t.Fatalf("non-matching datagram: recvDatagram = %v, want nil", err)
	}
	if c3.closed {
		t.Fatal("a non-matching undecryptable datagram must not close the connection")
	}
}
