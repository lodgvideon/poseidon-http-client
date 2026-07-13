package quic

import "time"

// Congestion-control constants (RFC 9002 §7.2). max_datagram_size is
// maxDatagramSize (1200), defined in send.go.
const (
	// kInitialWindow is the initial congestion window: min(10·mds, max(2·mds,
	// 14720)) = 12000 bytes for a 1200-byte datagram.
	kInitialWindow uint64 = 12000
	// kMinimumWindow is the floor a reduced congestion window cannot go below.
	kMinimumWindow uint64 = 2 * maxDatagramSize
	// kPersistentCongestionThreshold multiplies the base PTO to give the span of
	// unbroken loss that establishes persistent congestion (RFC 9002 §7.6.1).
	kPersistentCongestionThreshold uint64 = 3
)

// onPacketSent adds an ack-eliciting packet of size bytes to the in-flight total
// (RFC 9002 §7.3, OnPacketSent). Non-ack-eliciting packets (pure ACKs) are not in
// flight and are ignored. It is called from the sealing sites once the on-wire
// length is known, and is a no-op when congestion control is disabled (cwnd == 0,
// the sentinel for hand-built test connections).
//
// Initial and Handshake packets count too; their bytes are removed from the
// total when their packet-number space is discarded on key discard (discardSpace,
// RFC 9002 §6.4 / RFC 9001 §4.9), so an unacknowledged handshake packet cannot
// leave a permanent phantom floor in the in-flight total.
func (c *Conn) onPacketSent(sp int, pn uint64, ackEliciting bool, size int) {
	if !ackEliciting {
		return
	}
	if p, ok := c.sent[sp].packets[pn]; ok {
		p.size = size
		c.sent[sp].packets[pn] = p
	}
	c.bytesInFlight += uint64(size)
}

// onPacketAcked removes a newly acknowledged packet from the in-flight total and
// grows the congestion window (RFC 9002 §7.3.1–§7.3.2): additively in slow start
// (cwnd < ssthresh), or by one max_datagram_size per window of bytes acknowledged
// in congestion avoidance. A packet sent during the current recovery episode, or
// while the sender was application-limited rather than congestion-limited (§7.8),
// does not grow the window.
func (c *Conn) onPacketAcked(p sentPacket, cwndLimited bool) {
	c.removeInFlight(p.size)
	if c.cwnd == 0 {
		return // congestion control disabled (test sentinel)
	}
	if !p.timeSent.After(c.recoveryStart) {
		return // sent at or before the recovery episode start: no growth (§7.3.2)
	}
	if !cwndLimited {
		return // the flight was application-limited: do not grow the window (§7.8)
	}
	if c.cwnd < c.ssthresh {
		c.cwnd += uint64(p.size) // slow start
		return
	}
	// Congestion avoidance: raise cwnd by one max_datagram_size per congestion
	// window of bytes acknowledged. RFC 9002 §7.3.3's per-ack `cwnd += mds·acked /
	// cwnd` truncates to zero once cwnd exceeds mds² (~1.44 MB), freezing the
	// window; a byte accumulator realizes the same rate without that truncation.
	c.ccBytesAcked += uint64(p.size)
	if c.ccBytesAcked >= c.cwnd {
		c.ccBytesAcked -= c.cwnd
		c.cwnd += uint64(maxDatagramSize)
	}
}

// onCongestionEvent halves the congestion window on the first loss of a recovery
// episode (RFC 9002 §7.3.1). sentTime is the send time of the newest lost packet;
// a loss whose newest packet was sent at or before the current recovery start is
// part of an episode already handled, so cwnd is reduced at most once per episode.
func (c *Conn) onCongestionEvent(sentTime time.Time) {
	if c.cwnd == 0 {
		return // congestion control disabled (test sentinel)
	}
	if !sentTime.After(c.recoveryStart) {
		return // already in recovery for this episode
	}
	c.recoveryStart = c.clock()
	c.ssthresh = c.cwnd / 2
	c.cwnd = c.ssthresh
	if c.cwnd < kMinimumWindow {
		c.cwnd = kMinimumWindow
	}
	c.ccBytesAcked = 0
}

// persistentCongestionDuration is the span of unbroken loss that establishes
// persistent congestion (RFC 9002 §7.6.2): the base (un-backed-off) PTO times
// kPersistentCongestionThreshold. Unlike the §6.2 PTO, this duration includes
// max_ack_delay irrespective of the packet-number space, so it is added here for
// the spaces where ptoBase omits it (before the handshake completes).
func (c *Conn) persistentCongestionDuration() time.Duration {
	base := c.ptoBase()
	if !c.handshakeComplete {
		base += c.peer.MaxAckDelay
	}
	return base * time.Duration(kPersistentCongestionThreshold)
}

// onPersistentCongestion collapses the congestion window to kMinimumWindow when
// every ack-eliciting packet sent across a span longer than the persistent
// congestion duration is lost (RFC 9002 §7.6.1). Recovery start is cleared so the
// next acknowledgement restarts slow start from the floor, and min_rtt is reset on
// the next RTT sample (§5.2) so an obsolete low estimate cannot keep loss
// detection over-aggressive after the path has changed.
func (c *Conn) onPersistentCongestion() {
	if c.cwnd == 0 {
		return // congestion control disabled (test sentinel)
	}
	c.cwnd = kMinimumWindow
	c.recoveryStart = time.Time{} // §7.6.1: congestion_recovery_start_time = 0
	c.ccBytesAcked = 0
	c.rtt.resetMin = true
}

// cwndLimited reports whether a flight of priorInFlight bytes (the bytes in
// flight before the acknowledgement was processed) was limited by the congestion
// window rather than by the application (RFC 9002 §7.8). Only acknowledgements of
// congestion-limited flights grow the window, so an application that sends less
// than a window cannot inflate it. In slow start a more-than-half-full window
// still counts as limited, so growth is not stalled when acknowledgements arrive
// one packet at a time and the window has already partly drained.
func (c *Conn) cwndLimited(priorInFlight uint64) bool {
	if priorInFlight >= c.cwnd {
		return true
	}
	return c.cwnd < c.ssthresh && priorInFlight > c.cwnd/2 // slow start
}

// removeInFlight subtracts a packet's size from the in-flight total, guarding
// against underflow (test packets carry size 0, and this defends any drift).
//
// Freeing in-flight bytes opens congestion-window headroom, so it flags the
// receive burst as window-growing (docs/HTTP3_DESIGN.md §3.3, INV-5):
// maybeBroadcastSendWindow then wakes senders parked on blockCong, which have no
// other frame-driven wake. This is the single choke point through which every
// flight-freeing site passes — onPacketAcked, detectLost, discardSpace, and
// queueOldestProbe — so the flag is set once here rather than at each call site.
func (c *Conn) removeInFlight(size int) {
	if c.bytesInFlight >= uint64(size) {
		c.bytesInFlight -= uint64(size)
	} else {
		c.bytesInFlight = 0
	}
	c.sendWindowGrew = true
}

// pacingRefillDelay estimates how long until the pacing token bucket refills
// enough to admit one more datagram (RFC 9002 §7.7). WaitSendable arms a timer
// for this long when a send is pacing-limited (blockPace), because the bucket
// refills on wall-clock time with no inbound event to wake the sender. The
// estimate is the time to accrue one maxDatagramSize of credit at the pacing rate
// N·cwnd/smoothed_rtt (N = 5/4). It is kept slightly generous so a low estimate
// only costs an early recheck, never a stall (docs/HTTP3_DESIGN.md §7, residual
// risk 1). With pacing disabled (cwnd == 0) or no RTT sample it falls back to a
// short poll interval, so a blocked waiter still makes progress.
func (c *Conn) pacingRefillDelay() time.Duration {
	if c.cwnd == 0 || c.rtt.smoothedRTT <= 0 {
		return time.Millisecond
	}
	rate := 5 * c.cwnd / 4 // bytes per smoothed-RTT
	if rate == 0 {
		return time.Millisecond
	}
	d := time.Duration(uint64(maxDatagramSize) * uint64(c.rtt.smoothedRTT) / rate)
	if d <= 0 {
		return time.Millisecond
	}
	return d
}

// pacingCredit returns the bytes the pacer will admit right now (RFC 9002 §7.7).
// A token bucket refills from elapsed wall-clock time at rate N·cwnd/smoothed_rtt
// (N = 1.25, applied as 5/4) and is capped at the initial congestion window, the
// burst limit §7.7 recommends. Congestion control disabled (cwnd == 0) or no RTT
// sample yet means no pacing — a full initial-window burst is allowed, which is
// itself §7.7/§7.2's initial-window burst allowance.
func (c *Conn) pacingCredit() uint64 {
	if c.cwnd == 0 || c.rtt.smoothedRTT <= 0 {
		return kInitialWindow
	}
	now := c.clock()
	if c.pacingLast.IsZero() {
		c.pacingLast = now
		c.pacingBudget = kInitialWindow // a fresh sender may burst the initial window
	}
	elapsed := now.Sub(c.pacingLast)
	if elapsed <= 0 {
		return c.pacingBudget
	}
	if elapsed >= time.Second {
		// Beyond a second the bucket is certainly full; short-circuit so the refill
		// multiply below cannot overflow on a large window, and drop the surplus time.
		c.pacingBudget, c.pacingLast = kInitialWindow, now
		return c.pacingBudget
	}
	rate := 5 * c.cwnd / 4 // N·cwnd bytes per smoothed-RTT (N = 1.25 as 5/4)
	add := rate * uint64(elapsed) / uint64(c.rtt.smoothedRTT)
	if add == 0 {
		// Sub-byte elapsed: leave pacingLast so this time accumulates into a later
		// refill. Advancing it here would discard the time and let a fast retry loop
		// starve the bucket to a standstill.
		return c.pacingBudget
	}
	// Advance pacingLast only by the time that produced `add` whole bytes, carrying
	// the sub-byte remainder forward so the long-run rate stays N·cwnd/smoothed_rtt.
	consumed := time.Duration(add * uint64(c.rtt.smoothedRTT) / rate)
	c.pacingLast = c.pacingLast.Add(consumed)
	c.pacingBudget = min(kInitialWindow, c.pacingBudget+add)
	return c.pacingBudget
}

// pacingOnSend debits size bytes from the pacing bucket after a paced packet is
// sent (RFC 9002 §7.7). It is a no-op on the unpaced path, where the bucket is
// never consulted.
func (c *Conn) pacingOnSend(size uint64) {
	if c.pacingBudget <= size {
		c.pacingBudget = 0
		return
	}
	c.pacingBudget -= size
}
