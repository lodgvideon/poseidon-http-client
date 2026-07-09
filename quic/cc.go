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
// in congestion avoidance. A packet sent during the current recovery episode does
// not grow the window.
func (c *Conn) onPacketAcked(p sentPacket) {
	c.removeInFlight(p.size)
	if c.cwnd == 0 {
		return // congestion control disabled (test sentinel)
	}
	if !p.timeSent.After(c.recoveryStart) {
		return // sent at or before the recovery episode start: no growth (§7.3.2)
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
//
// NOTE (RFC 9002 §7.6, deferred): persistent congestion — collapsing cwnd to
// kMinimumWindow when every packet across a period longer than
// kPersistentCongestionThreshold·PTO is lost — is not implemented. This client
// mostly receives (its send flight is a small request), the probe timeout already
// recovers a fully lost tail, and the omission only forgoes a faster collapse in
// a total outage. Recording earliest/latest lost send times in detectLost would
// add it.
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

// removeInFlight subtracts a packet's size from the in-flight total, guarding
// against underflow (test packets carry size 0, and this defends any drift).
func (c *Conn) removeInFlight(size int) {
	if c.bytesInFlight >= uint64(size) {
		c.bytesInFlight -= uint64(size)
	} else {
		c.bytesInFlight = 0
	}
}
