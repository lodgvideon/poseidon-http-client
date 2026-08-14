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

// resetFirst is the final size the first RESET_STREAM fixes in the three tests
// below. They share one shape: a RESET_STREAM fixes a final size, then a second
// frame — RESET_STREAM, STREAM with FIN, or plain STREAM data — tries to
// contradict it.
const resetFirst = 1000

// finalSizeChangeCases is the accept/reject edge for a second declaration of an
// already-known final size, shared by the RESET_STREAM and the STREAM-with-FIN
// tests below because RFC 9000 §4.5 states the rule once for both carriers.
//
// Equal is accepted: RESET_STREAM and a FIN-bearing STREAM frame are both
// retransmitted until acknowledged, so a repeat naming the same size is ordinary
// and indicates no change. Both one-byte edges are pinned, not just the far
// values: a comparison written with an off-by-one still rejects a wildly wrong
// final size, so the far cases alone cannot tell a correct check from a shifted
// one.
var finalSizeChangeCases = []struct {
	name   string
	second uint64
	want   error
}{
	{"Equal", resetFirst, nil},
	{"OneByteAbove", resetFirst + 1, ErrFinalSize},
	{"FarAbove", resetFirst * 2, ErrFinalSize},
	{"OneByteBelow", resetFirst - 1, ErrFinalSize},
	{"FarBelow", resetFirst / 2, ErrFinalSize},
}

// flowControlModes runs each final-size case with the connection receive limit
// enabled and disabled. §4.5's rule is a final-size rule, not a flow-control
// one, so the connection's receive limit must not gate it — and the two modes
// reach the checks with different state, since chargeRecv returns early when
// connRecvMax is the disabled sentinel.
var flowControlModes = []struct {
	name        string
	connRecvMax uint64
}{
	{"FlowControlOn", DefaultConnRecvWindow},
	{"FlowControlOff", 0}, // 0 is the disabled sentinel
}

// resetPrefixModes varies how much of the stream had already arrived when the
// first RESET_STREAM fixed the final size.
//
// AllDataBefore is the case that pins the separation between recvStream's fin
// and finalKnown flags. With every byte the final size names already received
// contiguously, complete() — fin && base+len(data) == finalSize && no pending —
// is one flag short of true, and the only thing holding it false is that a
// RESET_STREAM sets finalKnown but NOT fin. OnResetStream drops a reset outright
// when complete() is true — RFC 9000 §3.2 allows that: "It is possible that all
// stream data has already been received when a RESET_STREAM is received (that
// is, in the 'Data Recvd' state) ... An implementation is free to manage this
// situation as it chooses." So a reset that also set fin would send the second,
// contradicting RESET_STREAM straight down that early return and SWALLOW the
// §4.5 FINAL_SIZE_ERROR — the very error the rule exists to raise, lost through
// a different path. NoDataBefore cannot express that: the contiguous prefix is
// zero there, so complete() stays false whatever fin holds.
var resetPrefixModes = []struct {
	name   string
	prefix uint64 // bytes received at offset 0, without FIN, before the reset
}{
	{"NoDataBefore", 0},
	{"AllDataBefore", resetFirst},
}

// resetStreamAt is a Conn with one open stream whose peer has sent prefix bytes
// and then RESET_STREAM with final size resetFirst, ready for a contradicting
// frame. prefix must not exceed resetFirst — a reset below the data already
// received is a different rule, tested by ResetFinalSizeBelow.
func resetStreamAt(t *testing.T, connRecvMax, prefix uint64) (*Stream, *connFrameHandler) {
	t.Helper()
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: connRecvMax}
	s, _ := c.OpenStream()
	h := &connFrameHandler{c: c}
	if prefix > 0 {
		if err := h.OnStream(s.ID(), 0, false, make([]byte, prefix)); err != nil {
			t.Fatalf("%d bytes before the reset = %v, want nil", prefix, err)
		}
	}
	if err := h.OnResetStream(s.ID(), 0, resetFirst); err != nil {
		t.Fatalf("first RESET_STREAM (final size %d) = %v, want nil", resetFirst, err)
	}
	return s, h
}

// TestConformance_RFC9000_Sec45_ResetFinalSizeIsFixed checks that a final size
// learned from a RESET_STREAM is fixed in BOTH directions: a later RESET_STREAM
// declaring either a larger or a smaller final size is a FINAL_SIZE_ERROR, while
// one naming the same size is accepted.
//
// RFC 9000 §4.5: "Once a final size for a stream is known, it cannot change.  If
// a RESET_STREAM or STREAM frame is received indicating a change in the final
// size for the stream, an endpoint SHOULD respond with an error of type
// FINAL_SIZE_ERROR". "Cannot change" is symmetric. Treating a known final size
// as a floor — rejecting only a smaller second value — leaves the growing case
// accepted, which is the direction that inflates the stream's flow-control
// charge and lets a peer keep a supposedly finished stream alive.
//
// Every case also runs with the stream's whole prefix already received before
// the reset (resetPrefixModes). That mode is what keeps the error observable at
// all: in it, a reset that recorded the FIN flag along with the final size would
// make the receive side look complete, and the second RESET_STREAM would be
// dropped at OnResetStream's already-complete early return instead of raising
// the §4.5 error. The observable asserted here is the error the connection
// raises, not the flags behind it, so a future path that reaches the same early
// return some other way fails here too.
func TestConformance_RFC9000_Sec45_ResetFinalSizeIsFixed(t *testing.T) {
	for _, fc := range flowControlModes {
		for _, pm := range resetPrefixModes {
			for _, tc := range finalSizeChangeCases {
				t.Run(fc.name+"/"+pm.name+"/"+tc.name, func(t *testing.T) {
					s, h := resetStreamAt(t, fc.connRecvMax, pm.prefix)
					if err := h.OnResetStream(s.ID(), 0, tc.second); err != tc.want {
						t.Fatalf("second RESET_STREAM final size %d after %d (%d bytes received first) = %v, want %v",
							tc.second, resetFirst, pm.prefix, err, tc.want)
					}
				})
			}
		}
	}
}

// TestConformance_RFC9000_Sec45_FinAfterResetFinalSize checks that a final size
// learned from a RESET_STREAM also binds the STREAM path: a later STREAM frame
// carrying a FIN that names a different final size — larger or smaller — is a
// FINAL_SIZE_ERROR, while a FIN naming the same size is accepted.
//
// §4.5 names both carriers in a single sentence — "If a RESET_STREAM or STREAM
// frame is received indicating a change in the final size" — so the rule does
// not depend on which frame type fixed the size or which one contradicts it.
// Each of the four combinations has to hold on its own; this is the one where
// the two carriers differ.
func TestConformance_RFC9000_Sec45_FinAfterResetFinalSize(t *testing.T) {
	for _, fc := range flowControlModes {
		for _, tc := range finalSizeChangeCases {
			t.Run(fc.name+"/"+tc.name, func(t *testing.T) {
				s, h := resetStreamAt(t, fc.connRecvMax, 0)
				// A zero-length STREAM frame with FIN at offset n declares final size n.
				if err := h.OnStream(s.ID(), tc.second, true, nil); err != tc.want {
					t.Fatalf("FIN declaring final size %d after RESET_STREAM %d = %v, want %v",
						tc.second, resetFirst, err, tc.want)
				}
			})
		}
	}
}

// TestConformance_RFC9000_Sec45_DataAfterResetFinalSize checks that a final size
// learned from a RESET_STREAM makes later STREAM data at or beyond it a
// FINAL_SIZE_ERROR, while data ending at or below it is still accepted — a peer
// may legitimately retransmit, after the reset, bytes it sent before it.
//
// RFC 9000 §4.5: "A receiver SHOULD treat receipt of data at or beyond the final
// size as an error of type FINAL_SIZE_ERROR, even after a stream is closed." The
// boundary is the last byte's offset, not the frame's end offset: a frame ending
// exactly AT the final size carries its last byte at finalSize-1 and is legal,
// while one byte at offset finalSize is not.
func TestConformance_RFC9000_Sec45_DataAfterResetFinalSize(t *testing.T) {
	for _, fc := range flowControlModes {
		for _, tc := range []struct {
			name   string
			offset uint64
			n      int
			want   error
		}{
			{"WhollyBelow", 0, 100, nil},                    // bytes 0..99
			{"EndsAtFinalSize", resetFirst - 100, 100, nil}, // last byte at finalSize-1
			{"AtFinalSize", resetFirst, 1, ErrFinalSize},    // one byte AT the final size
			{"BeyondFinalSize", resetFirst + 50, 1, ErrFinalSize},
		} {
			t.Run(fc.name+"/"+tc.name, func(t *testing.T) {
				s, h := resetStreamAt(t, fc.connRecvMax, 0)
				if err := h.OnStream(s.ID(), tc.offset, false, make([]byte, tc.n)); err != tc.want {
					t.Fatalf("%d bytes at offset %d after RESET_STREAM final size %d = %v, want %v",
						tc.n, tc.offset, resetFirst, err, tc.want)
				}
			})
		}
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
