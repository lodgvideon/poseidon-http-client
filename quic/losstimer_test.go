package quic

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestConformance_RFC9002_Sec612_EarliestLossTime checks that the loss-detection
// timer covers the earliest ack-eliciting packet that has a later acknowledged
// packet, and excludes packets above the largest acknowledged (RFC 9002 §6.1.2).
func TestConformance_RFC9002_Sec612_EarliestLossTime(t *testing.T) {
	base := time.Unix(5000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.smoothedRTT, c.rtt.haveSample = 40*time.Millisecond, true // lossDelay = 45ms
	s := &c.sent[spaceApp]
	s.haveLargestAcked, s.largestAckedPN = true, 5
	s.onSent(2, base.Add(-10*time.Millisecond), true, nil) // earliest eligible
	s.onSent(4, base.Add(-5*time.Millisecond), true, nil)
	s.onSent(9, base, true, nil) // above the largest acknowledged: not eligible

	lt, sp, ok := c.earliestLossTime()
	if !ok || sp != spaceApp {
		t.Fatalf("earliestLossTime ok=%v sp=%d, want true/app", ok, sp)
	}
	if want := base.Add(-10 * time.Millisecond).Add(45 * time.Millisecond); !lt.Equal(want) {
		t.Fatalf("loss time = %v, want %v (earliest eligible packet + lossDelay)", lt, want)
	}
}

// TestConformance_RFC9002_Sec62_LossTimePriorityOverPTO checks that a pending
// time-threshold loss sets the loss-detection timer ahead of the probe timeout
// (RFC 9002 §6.2).
func TestConformance_RFC9002_Sec62_LossTimePriorityOverPTO(t *testing.T) {
	base := time.Unix(5000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.smoothedRTT, c.rtt.rttvar, c.rtt.haveSample = 40*time.Millisecond, 5*time.Millisecond, true
	s := &c.sent[spaceApp]
	s.haveLargestAcked, s.largestAckedPN = true, 5
	s.onSent(2, base.Add(-10*time.Millisecond), true, nil)

	dl, isLoss := c.lossDetectionDeadline()
	if !isLoss {
		t.Fatal("a pending time-threshold loss must take priority over the PTO")
	}
	if want := base.Add(35 * time.Millisecond); !dl.Equal(want) {
		t.Fatalf("deadline = %v, want %v (loss time, not PTO)", dl, want)
	}
}

// TestConformance_RFC9002_Sec62_PTOWhenNoLossTime checks that with no later
// acknowledged packet — a fully unacknowledged tail — no loss timer is armed and
// the probe timeout governs instead (RFC 9002 §6.2).
func TestConformance_RFC9002_Sec62_PTOWhenNoLossTime(t *testing.T) {
	base := time.Unix(5000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.smoothedRTT, c.rtt.rttvar, c.rtt.haveSample = 40*time.Millisecond, 5*time.Millisecond, true
	c.sent[spaceApp].onSent(0, base, true, nil) // haveLargestAcked stays false

	dl, isLoss := c.lossDetectionDeadline()
	if isLoss {
		t.Fatal("no later acknowledged packet → no loss timer")
	}
	if want := base.Add(c.ptoPeriod()); !dl.Equal(want) {
		t.Fatalf("deadline = %v, want the PTO %v", dl, want)
	}
}

// TestConformance_RFC9002_Sec612_LossTimerDeclaresLost drives readWithPTO with a
// reordered flight whose earliest packet has already crossed the time threshold:
// the loss-detection timer must declare it lost (running loss detection) rather
// than treating the expiry as a probe timeout (RFC 9002 §6.1.2, §6.2).
func TestConformance_RFC9002_Sec612_LossTimerDeclaresLost(t *testing.T) {
	client, server := net.Pipe() // honors deadlines; server never writes
	defer client.Close()
	defer server.Close()
	c := &Conn{pc: client}                // real clock (now == nil)
	c.rtt.update(10*time.Millisecond, 0)  // smoothed 10ms → lossDelay ≈ 11ms
	// Packet 5 acknowledged, packet 0 still pending and sent 50 ms ago (well past
	// the loss delay): a reordered loss the timer must catch without a probe.
	c.sent[spaceApp].onSent(0, time.Now().Add(-50*time.Millisecond), true, nil)
	c.sent[spaceApp].haveLargestAcked, c.sent[spaceApp].largestAckedPN = true, 5

	// The loss timer fires immediately (the packet is already past threshold), runs
	// loss detection, then idles; cancel unblocks that idle read via the watchdog.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(60 * time.Millisecond)
		cancel()
	}()
	if _, err := c.readWithPTO(ctx, make([]byte, 2048)); err != context.Canceled {
		t.Fatalf("readWithPTO = %v, want context.Canceled (idle after the loss fired)", err)
	}
	if _, stillSent := c.sent[spaceApp].packets[0]; stillSent {
		t.Fatal("the reordered packet past the time threshold should have been declared lost")
	}
	if c.ptoCount != 0 {
		t.Fatalf("ptoCount = %d, a time-threshold loss must not be treated as a probe timeout", c.ptoCount)
	}
}
