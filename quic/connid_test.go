package quic

import "testing"

// TestConformance_RFC9000_Sec511_ActiveCIDLimit checks that the server may not
// keep more than active_connection_id_limit (2) destination connection IDs active
// (RFC 9000 §5.1.1): the excess is a CONNECTION_ID_LIMIT_ERROR.
func TestConformance_RFC9000_Sec511_ActiveCIDLimit(t *testing.T) {
	c := &Conn{dcid: []byte("origcid0")}
	h := &connFrameHandler{c: c}
	if err := h.OnNewConnectionID(1, 0, []byte("newcid01"), &[16]byte{}); err != nil { // seq0+seq1 = 2, ok
		t.Fatalf("first NEW_CONNECTION_ID: %v", err)
	}
	if err := h.OnNewConnectionID(2, 0, []byte("newcid02"), &[16]byte{}); err != ErrConnectionIDLimit {
		t.Fatalf("a third active CID = %v, want ErrConnectionIDLimit", err)
	}
}

// TestConformance_RFC9000_Sec512_RetirePriorToSwitchesCID checks that an increased
// Retire Prior To retires lower-sequence connection IDs, switches the one in use
// if it was retired, and queues a RETIRE_CONNECTION_ID (RFC 9000 §5.1.2).
func TestConformance_RFC9000_Sec512_RetirePriorToSwitchesCID(t *testing.T) {
	c := &Conn{dcid: []byte("origcid0")}
	h := &connFrameHandler{c: c}
	if err := h.OnNewConnectionID(1, 1, []byte("newcid01"), &[16]byte{}); err != nil { // retire_prior_to 1 retires seq 0
		t.Fatalf("NEW_CONNECTION_ID: %v", err)
	}
	if c.curCIDSeq != 1 || string(c.dcid) != "newcid01" {
		t.Fatalf("dcid = %q seq %d, want newcid01 seq 1 (switched after the current CID was retired)", c.dcid, c.curCIDSeq)
	}
	if len(c.retransQueue[spaceApp]) == 0 {
		t.Fatal("a RETIRE_CONNECTION_ID should be queued for the retired seq 0")
	}
	if len(c.serverCIDs) != 1 {
		t.Fatalf("active CIDs = %d, want 1 (only seq 1 remains)", len(c.serverCIDs))
	}
	// A NEW_CONNECTION_ID below the retire boundary is retired, not stored, and the
	// RETIRE is not duplicated for an already-retired sequence (spam bound).
	before := len(c.retransQueue[spaceApp])
	if err := h.OnNewConnectionID(0, 0, []byte("origcid0"), &[16]byte{}); err != nil {
		t.Fatalf("NEW_CONNECTION_ID for a retired seq: %v", err)
	}
	if _, stored := c.serverCIDs[0]; stored {
		t.Fatal("a NEW_CONNECTION_ID below the retire boundary must not be stored")
	}
	if len(c.retransQueue[spaceApp]) != before {
		t.Fatal("a RETIRE for an already-retired sequence must not be re-queued")
	}
}

// TestConn_NewConnectionID_DuplicateSeqConflict checks that reusing a sequence
// number for a different connection ID is a PROTOCOL_VIOLATION, while an
// identical retransmission is accepted (RFC 9000 §19.15).
func TestConn_NewConnectionID_DuplicateSeqConflict(t *testing.T) {
	c := &Conn{dcid: []byte("origcid0")}
	h := &connFrameHandler{c: c}
	if err := h.OnNewConnectionID(1, 0, []byte("newcid01"), &[16]byte{}); err != nil {
		t.Fatal(err)
	}
	if err := h.OnNewConnectionID(1, 0, []byte("different"), &[16]byte{}); err != ErrProtocolViolation {
		t.Fatalf("sequence conflict = %v, want ErrProtocolViolation", err)
	}
	if err := h.OnNewConnectionID(1, 0, []byte("newcid01"), &[16]byte{}); err != nil {
		t.Fatalf("identical retransmit = %v, want nil", err)
	}
}

// TestConformance_RFC9000_Sec1916_RetireConnectionID checks that any
// RETIRE_CONNECTION_ID is a PROTOCOL_VIOLATION: this client provides a
// zero-length source connection ID, so it has issued nothing to retire
// (RFC 9000 §19.16).
func TestConformance_RFC9000_Sec1916_RetireConnectionID(t *testing.T) {
	h := &connFrameHandler{c: &Conn{}}
	for _, seq := range []uint64{0, 1, 5} {
		if err := h.OnRetireConnectionID(seq); err != ErrProtocolViolation {
			t.Fatalf("RETIRE_CONNECTION_ID(%d) = %v, want ErrProtocolViolation", seq, err)
		}
	}
}
