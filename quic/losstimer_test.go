package quic

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.Truef(t, ok, "earliestLossTime ok=%v, want true — an eligible packet is in flight", ok)
	require.Equalf(t, spaceApp, sp, "earliestLossTime space = %d, want the application space", sp)
	want := base.Add(-10 * time.Millisecond).Add(45 * time.Millisecond)
	assert.Truef(t, lt.Equal(want),
		"loss time = %v, want %v (earliest eligible packet + lossDelay)", lt, want)
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

	require.True(t, isLoss, "a pending time-threshold loss must take priority over the PTO")
	want := base.Add(35 * time.Millisecond)
	assert.Truef(t, dl.Equal(want), "deadline = %v, want %v (loss time, not PTO)", dl, want)
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

	assert.False(t, isLoss, "no later acknowledged packet → no loss timer")
	want := base.Add(c.ptoPeriod())
	assert.Truef(t, dl.Equal(want), "deadline = %v, want the PTO %v", dl, want)
}

// TestConformance_RFC9002_Sec612_LossTimerDeclaresLost drives readWithPTO with a
// reordered flight whose earliest packet has already crossed the time threshold:
// the loss-detection timer must declare it lost (running loss detection) rather
// than treating the expiry as a probe timeout (RFC 9002 §6.1.2, §6.2).
func TestConformance_RFC9002_Sec612_LossTimerDeclaresLost(t *testing.T) {
	client, server := net.Pipe() // honors deadlines; server never writes
	defer client.Close()
	defer server.Close()
	// handshakeComplete: a reordered application-space loss is a post-handshake
	// scenario, so the §6.2.2.1 anti-deadlock PTO is not in play.
	c := &Conn{pc: client, handshakeComplete: true} // real clock (now == nil)
	c.rtt.update(10*time.Millisecond, 0)            // smoothed 10ms → lossDelay ≈ 11ms
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

	_, err := c.readWithPTO(ctx, make([]byte, 2048))

	assert.Equalf(t, context.Canceled, err,
		"readWithPTO = %v, want context.Canceled (idle after the loss fired)", err)
	_, stillSent := c.sent[spaceApp].packets[0]
	assert.False(t, stillSent,
		"the reordered packet past the time threshold should have been declared lost")
	assert.Zerof(t, c.ptoCount,
		"ptoCount = %d, a time-threshold loss must not be treated as a probe timeout", c.ptoCount)
}
