package quic

import (
	"testing"
	"time"
)

const ms = time.Millisecond

func TestRTTStats_FirstSample(t *testing.T) {
	var r rttStats
	r.update(100*ms, 0)
	if r.minRTT != 100*ms || r.smoothedRTT != 100*ms || r.rttvar != 50*ms {
		t.Fatalf("first sample = %+v, want smoothed=100ms rttvar=50ms", r)
	}
}

// TestRTTStats_Subsequent checks the §5.3 smoothing: rttvar = 3/4·rttvar +
// 1/4·|srtt-adjusted|, smoothed = 7/8·srtt + 1/8·adjusted.
func TestRTTStats_Subsequent(t *testing.T) {
	var r rttStats
	r.update(100*ms, 0)
	r.update(120*ms, 0)
	if r.minRTT != 100*ms {
		t.Fatalf("minRTT = %v, want 100ms", r.minRTT)
	}
	if want := (7*100*ms + 120*ms) / 8; r.smoothedRTT != want { // 102.5ms
		t.Fatalf("smoothedRTT = %v, want %v", r.smoothedRTT, want)
	}
	if want := (3*50*ms + 20*ms) / 4; r.rttvar != want { // 42.5ms
		t.Fatalf("rttvar = %v, want %v", r.rttvar, want)
	}
}

// TestRTTStats_AckDelay checks that the peer's ack delay is subtracted, but not
// below min_rtt (§5.3).
func TestRTTStats_AckDelay(t *testing.T) {
	var r rttStats
	r.update(100*ms, 0)
	r.update(120*ms, 10*ms) // 120 >= min(100)+10, so adjusted = 110ms
	if want := (7*100*ms + 110*ms) / 8; r.smoothedRTT != want {
		t.Fatalf("smoothedRTT = %v, want %v (ack delay subtracted)", r.smoothedRTT, want)
	}
}

func TestSentSpace_Ack(t *testing.T) {
	base := time.Unix(100, 0)
	var s sentSpace
	s.onSent(5, base, true, nil)
	s.onSent(6, base.Add(10*ms), true, nil)

	sendTime, ok := s.ack(5, 6)
	if !ok || !sendTime.Equal(base.Add(10*ms)) {
		t.Fatalf("ack = (%v, %v), want the largest's (pn 6) send time", sendTime, ok)
	}
	if len(s.packets) != 0 {
		t.Fatalf("acked packets not removed: %d left", len(s.packets))
	}
	// A duplicate ACK of an already-largest packet yields no new RTT sample.
	s.onSent(6, base.Add(10*ms), true, nil)
	if _, ok := s.ack(6, 6); ok {
		t.Fatal("duplicate largest ack must not resample RTT")
	}
}

// TestConnFrameHandler_OnAck_UpdatesRTT drives the ACK path end to end: record
// sends, advance an injected clock, feed an ACK, and check the RTT sample and
// that the acked packets are cleared.
func TestConnFrameHandler_OnAck_UpdatesRTT(t *testing.T) {
	base := time.Unix(200, 0)
	tm := base
	c := &Conn{now: func() time.Time { return tm }}
	c.sent[spaceApp].onSent(10, base, true, nil)
	c.sent[spaceApp].onSent(11, base, true, nil)

	tm = base.Add(30 * ms)
	h := &connFrameHandler{c: c, space: spaceApp}
	if err := h.OnAck(11, 0, 1); err != nil { // acks [10, 11]
		t.Fatal(err)
	}
	if !c.rtt.haveSample || c.rtt.latestRTT != 30*ms {
		t.Fatalf("rtt = %+v, want latestRTT=30ms", c.rtt)
	}
	if len(c.sent[spaceApp].packets) != 0 {
		t.Fatalf("acked packets not cleared: %d left", len(c.sent[spaceApp].packets))
	}
}

// TestConnFrameHandler_OnAckRange_Gap verifies an ACK with a gap acknowledges
// the two ranges and leaves the gapped packet in flight (RFC 9000 §19.3).
func TestConnFrameHandler_OnAckRange_Gap(t *testing.T) {
	base := time.Unix(300, 0)
	c := &Conn{now: func() time.Time { return base }}
	for pn := uint64(8); pn <= 11; pn++ {
		c.sent[spaceApp].onSent(pn, base, true, nil)
	}
	h := &connFrameHandler{c: c, space: spaceApp}
	// Largest 11, first range 0 (just 11); then gap 0 (skip 10), length 1 (8..9).
	if err := h.OnAck(11, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := h.OnAckRange(0, 1); err != nil {
		t.Fatal(err)
	}
	left := c.sent[spaceApp].packets
	if len(left) != 1 {
		t.Fatalf("in-flight = %d, want 1 (the gapped pn 10)", len(left))
	}
	if _, ok := left[10]; !ok {
		t.Fatalf("pn 10 should remain in flight, got %v", left)
	}
}
