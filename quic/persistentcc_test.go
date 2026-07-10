package quic

import (
	"testing"
	"time"
)

// pcTestConn builds a Conn whose RTT is sampled (smoothed 100 ms, rttvar 25 ms →
// base PTO 200 ms, persistent congestion duration 600 ms) with the first RTT
// sample well in the past, so every test packet counts toward §7.6.
func pcTestConn(base time.Time) *Conn {
	c := &Conn{cwnd: 20000, ssthresh: ^uint64(0), now: func() time.Time { return base }}
	c.rtt.smoothedRTT = 100 * time.Millisecond
	c.rtt.rttvar = 25 * time.Millisecond
	c.rtt.haveSample = true
	c.firstRTTSample = base.Add(-10 * time.Second)
	return c
}

// sendLostFlight records ack-eliciting app-space packets at the given send-time
// offsets from base, all below an already-acknowledged packet number so loss
// detection declares every one lost by the packet threshold.
func sendLostFlight(c *Conn, base time.Time, offsets ...time.Duration) {
	s := &c.sent[spaceApp]
	s.largestAckedPN, s.haveLargestAcked = 100, true
	for i, off := range offsets {
		s.onSent(uint64(i), base.Add(off), true, nil)
	}
}

// TestConformance_RFC9002_Sec76_PersistentCongestionCollapse checks that when the
// lost ack-eliciting packets span longer than the persistent congestion duration,
// the window collapses to kMinimumWindow, recovery start is cleared, and min_rtt
// is armed for reset (RFC 9002 §7.6.1, §5.2).
func TestConformance_RFC9002_Sec76_PersistentCongestionCollapse(t *testing.T) {
	base := time.Unix(2000, 0)
	c := pcTestConn(base)
	// Span 700 ms > 600 ms duration.
	sendLostFlight(c, base, -800*time.Millisecond, -600*time.Millisecond,
		-400*time.Millisecond, -200*time.Millisecond, -100*time.Millisecond)

	c.detectLost(spaceApp)

	if c.cwnd != kMinimumWindow {
		t.Fatalf("cwnd = %d, want kMinimumWindow %d (persistent congestion collapse)", c.cwnd, kMinimumWindow)
	}
	if !c.recoveryStart.IsZero() {
		t.Fatal("recoveryStart must be cleared to zero on persistent congestion (§7.6.1)")
	}
	if !c.rtt.resetMin {
		t.Fatal("min_rtt must be armed for reset after persistent congestion (§5.2)")
	}
}

// TestConformance_RFC9002_Sec76_ShortSpanHalvesOnly checks that a loss whose
// ack-eliciting packets span less than the persistent congestion duration only
// halves the window (the ordinary recovery response), leaving recovery start set.
func TestConformance_RFC9002_Sec76_ShortSpanHalvesOnly(t *testing.T) {
	base := time.Unix(2000, 0)
	c := pcTestConn(base)
	// Span 300 ms < 600 ms duration.
	sendLostFlight(c, base, -300*time.Millisecond, -150*time.Millisecond, 0)

	c.detectLost(spaceApp)

	if c.cwnd != 10000 {
		t.Fatalf("cwnd = %d, want 10000 (halved, not collapsed)", c.cwnd)
	}
	if c.recoveryStart != base {
		t.Fatal("recoveryStart should be set (ordinary recovery, no collapse)")
	}
	if c.rtt.resetMin {
		t.Fatal("min_rtt must not be reset without persistent congestion")
	}
}

// TestConformance_RFC9002_Sec76_AckedPacketInSpanNoCollapse checks that an
// acknowledgement inside the lost span breaks the unbroken loss: even though the
// lost packets span longer than the persistent congestion duration, the window is
// only halved, not collapsed (RFC 9002 §7.6.1, condition 1). This guards the case
// where frameless packets (grants, PING probes) survive as false span boundaries
// while a real packet between them was acknowledged.
func TestConformance_RFC9002_Sec76_AckedPacketInSpanNoCollapse(t *testing.T) {
	base := time.Unix(2000, 0)
	c := pcTestConn(base)
	// Lost ack-eliciting packets span 700 ms > 600 ms duration...
	sendLostFlight(c, base, -800*time.Millisecond, -100*time.Millisecond)
	// ...but a packet acknowledged 400 ms before base sits strictly inside that
	// span, so the loss is not unbroken.
	c.sent[spaceApp].ackedElicit = []time.Time{base.Add(-400 * time.Millisecond)}

	c.detectLost(spaceApp)

	if c.cwnd != 10000 {
		t.Fatalf("cwnd = %d, want 10000 (an acked packet in the span blocks collapse)", c.cwnd)
	}
	if !c.recoveryStart.Equal(base) {
		t.Fatal("recoveryStart should be set (ordinary halving, no collapse)")
	}
}

// TestConformance_RFC9002_Sec76_RequiresRTTSample checks that persistent
// congestion is not established for packets sent before the first RTT sample: with
// no sample recorded, a long lost span only halves the window (RFC 9002 §7.6.1).
func TestConformance_RFC9002_Sec76_RequiresRTTSample(t *testing.T) {
	base := time.Unix(2000, 0)
	c := pcTestConn(base)
	c.firstRTTSample = time.Time{} // no sample yet
	sendLostFlight(c, base, -800*time.Millisecond, -100*time.Millisecond)

	c.detectLost(spaceApp)

	if c.cwnd != 10000 {
		t.Fatalf("cwnd = %d, want 10000 (no collapse without an RTT sample)", c.cwnd)
	}
}

// TestConn_PersistentCongestion_ResetsMinRTT checks that after persistent
// congestion arms the reset, the next RTT sample becomes the new min_rtt even
// though it is larger, and subsequent smaller samples lower it as usual (§5.2).
func TestConn_PersistentCongestion_ResetsMinRTT(t *testing.T) {
	r := rttStats{haveSample: true, minRTT: 50 * time.Millisecond, smoothedRTT: 100 * time.Millisecond, rttvar: 10 * time.Millisecond}
	r.resetMin = true

	r.update(200*time.Millisecond, 0)
	if r.minRTT != 200*time.Millisecond {
		t.Fatalf("min_rtt = %v, want 200ms (reset adopts the larger sample)", r.minRTT)
	}
	if r.resetMin {
		t.Fatal("resetMin must be consumed after one sample")
	}
	r.update(80*time.Millisecond, 0)
	if r.minRTT != 80*time.Millisecond {
		t.Fatalf("min_rtt = %v, want 80ms (normal lowering resumes)", r.minRTT)
	}
}
