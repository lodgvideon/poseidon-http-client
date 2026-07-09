package quic

import "testing"

// TestConformance_RFC9000_Sec45_DataPastFinalSize checks that data received past a
// declared final size is a FINAL_SIZE_ERROR.
func TestConformance_RFC9000_Sec45_DataPastFinalSize(t *testing.T) {
	var r recvStream
	if err := r.receive(0, []byte("hello"), true); err != nil { // FIN → final size 5
		t.Fatal(err)
	}
	if err := r.receive(5, []byte("x"), false); err != ErrFinalSize { // byte at offset 5
		t.Fatalf("data past final size = %v, want ErrFinalSize", err)
	}
}

// TestConformance_RFC9000_Sec45_FinBelowReceived checks that a FIN whose final
// size is below data already received is a FINAL_SIZE_ERROR.
func TestConformance_RFC9000_Sec45_FinBelowReceived(t *testing.T) {
	var r recvStream
	if err := r.receive(0, make([]byte, 500), false); err != nil { // highest 500, no FIN
		t.Fatal(err)
	}
	if err := r.receive(100, nil, true); err != ErrFinalSize { // FIN claims final size 100 < 500
		t.Fatalf("FIN below received = %v, want ErrFinalSize", err)
	}
}

// TestConformance_RFC9000_Sec45_ConflictingFin checks that a second FIN with a
// different final size is a FINAL_SIZE_ERROR, while an identical retransmit is ok.
func TestConformance_RFC9000_Sec45_ConflictingFin(t *testing.T) {
	var r recvStream
	if err := r.receive(0, []byte("hello"), true); err != nil { // final size 5
		t.Fatal(err)
	}
	if err := r.receive(0, []byte("hello"), true); err != nil { // identical retransmit
		t.Fatalf("identical FIN retransmit should be accepted: %v", err)
	}
	if err := r.receive(0, []byte("hi"), true); err != ErrFinalSize { // final size 2 ≠ 5
		t.Fatalf("conflicting final size = %v, want ErrFinalSize", err)
	}
}

// TestConformance_RFC9000_Sec45_ResetFinalSizeBelow checks that a RESET_STREAM
// whose final size is below data already received is a FINAL_SIZE_ERROR.
func TestConformance_RFC9000_Sec45_ResetFinalSizeBelow(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, _ := c.OpenStream()
	h := &connFrameHandler{c: c}
	if err := h.OnStream(s.ID(), 0, false, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if err := h.OnResetStream(s.ID(), 0, 50); err != ErrFinalSize { // 50 < the 100 received
		t.Fatalf("reset final size below received = %v, want ErrFinalSize", err)
	}
}

// TestConformance_RFC9000_Sec45_ResetFinalSizePastLimit checks that a RESET_STREAM
// final size past the per-stream limit is a FLOW_CONTROL_ERROR.
func TestConformance_RFC9000_Sec45_ResetFinalSizePastLimit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, _ := c.OpenStream()
	h := &connFrameHandler{c: c}
	if err := h.OnResetStream(s.ID(), 0, DefaultStreamRecvWindow+1); err != ErrFlowControl {
		t.Fatalf("reset final size past limit = %v, want ErrFlowControl", err)
	}
}

// TestConformance_RFC9000_Sec35_ResetAfterCompleteIgnored checks that a
// RESET_STREAM for a stream that has already been fully received (a clean FIN
// with all data) has no effect (RFC 9000 §3.5): the receive side stays complete,
// not reset, so a valid response is not discarded.
func TestConformance_RFC9000_Sec35_ResetAfterCompleteIgnored(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, _ := c.OpenStream()
	h := &connFrameHandler{c: c}
	if err := h.OnStream(s.ID(), 0, true, []byte("hello")); err != nil { // FIN → complete, final size 5
		t.Fatal(err)
	}
	if !s.recv.complete() {
		t.Fatal("stream should be complete after the FIN")
	}
	if err := h.OnResetStream(s.ID(), 42, 5); err != nil { // a late reset, code 42
		t.Fatal(err)
	}
	if s.recvReset {
		t.Fatal("a RESET_STREAM after a complete receive must be ignored (§3.5)")
	}
	if s.ResetCode() != 0 {
		t.Fatalf("reset code = %d, want 0 (reset ignored)", s.ResetCode())
	}
}

// TestConn_ResetFinalSize_CreditsConn checks that a RESET_STREAM's final size is
// credited to the connection receive accounting (RFC 9000 §4.5).
func TestConn_ResetFinalSize_CreditsConn(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, _ := c.OpenStream()
	h := &connFrameHandler{c: c}
	if err := h.OnResetStream(s.ID(), 0, 1000); err != nil {
		t.Fatal(err)
	}
	if c.connRecvTotal != 1000 {
		t.Fatalf("connRecvTotal = %d, want 1000 (final size credited to conn FC)", c.connRecvTotal)
	}
	if !s.recvReset {
		t.Fatal("recvReset should be set")
	}
}
