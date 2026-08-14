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

// TestConformance_RFC9000_Sec45_ResetFinalSizeBelowUnderNoFlowControl checks that
// the rule above holds with receive flow control disabled: a RESET_STREAM final
// size below data already received is still a FINAL_SIZE_ERROR, and one equal to
// the data received is still accepted (RFC 9000 §4.5). The final-size rule is not
// a flow-control rule, so the connection's receive limit does not gate it.
func TestConformance_RFC9000_Sec45_ResetFinalSizeBelowUnderNoFlowControl(t *testing.T) {
	// connRecvMax is left at 0 — the disabled sentinel — so no byte on this
	// connection is charged to receive flow control.
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	s, _ := c.OpenStream()
	h := &connFrameHandler{c: c}
	if err := h.OnStream(s.ID(), 0, false, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if err := h.OnResetStream(s.ID(), 0, 50); err != ErrFinalSize { // 50 < the 100 received
		t.Fatalf("reset final size below received = %v, want ErrFinalSize", err)
	}

	// A final size equal to the data received names that same last byte and is legal.
	c2 := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	s2, _ := c2.OpenStream()
	h2 := &connFrameHandler{c: c2}
	if err := h2.OnStream(s2.ID(), 0, false, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if err := h2.OnResetStream(s2.ID(), 0, 100); err != nil {
		t.Fatalf("reset final size equal to received = %v, want nil", err)
	}
}

// TestConformance_RFC9000_Sec45_SecondResetBelowFirst checks that a final size
// learned from a RESET_STREAM is itself fixed: a later RESET_STREAM declaring a
// smaller one is a FINAL_SIZE_ERROR, while one equal to it — an ordinary
// retransmit, since RESET_STREAM is resent until acknowledged — is accepted
// (RFC 9000 §4.5).
//
// Where the accept/reject edge sits comes straight from §4.5's definition: the
// final size is "one higher than the offset of the byte with the largest offset
// sent on the stream", and an endpoint "MUST NOT send data on a stream at or
// beyond the final size". So a final size equal to the high-water mark names
// that same last byte and changes nothing, while one byte below it would put the
// peer's own last byte AT the final size, which §4.5 forbids. The table pins
// both sides of that edge: a check written with an off-by-one still rejects a
// wildly low final size, so the far-below case alone cannot tell a correct
// comparison from a shifted one.
//
// Every case runs with receive flow control enabled and disabled, because the
// rule is a final-size rule and does not depend on flow control — and because
// the two modes reach the check with different state: chargeRecv returns early
// when connRecvMax is the disabled sentinel, so s.recvHighest stays 0 there and
// only the s.recv.highest the check actually reads has advanced.
func TestConformance_RFC9000_Sec45_SecondResetBelowFirst(t *testing.T) {
	const first = 1000 // the final size the first RESET_STREAM fixes
	for _, tc := range []struct {
		name        string
		connRecvMax uint64
		second      uint64
		want        error
	}{
		{"FlowControlOn/Equal", DefaultConnRecvWindow, first, nil},
		{"FlowControlOn/OneByteBelow", DefaultConnRecvWindow, first - 1, ErrFinalSize},
		{"FlowControlOn/FarBelow", DefaultConnRecvWindow, first / 2, ErrFinalSize},
		{"FlowControlOff/Equal", 0, first, nil}, // 0 is the disabled sentinel
		{"FlowControlOff/OneByteBelow", 0, first - 1, ErrFinalSize},
		{"FlowControlOff/FarBelow", 0, first / 2, ErrFinalSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: tc.connRecvMax}
			s, _ := c.OpenStream()
			h := &connFrameHandler{c: c}
			if err := h.OnResetStream(s.ID(), 0, first); err != nil {
				t.Fatalf("first RESET_STREAM (final size %d) = %v, want nil", first, err)
			}
			if err := h.OnResetStream(s.ID(), 0, tc.second); err != tc.want {
				t.Fatalf("second RESET_STREAM final size %d after %d = %v, want %v",
					tc.second, first, err, tc.want)
			}
		})
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
