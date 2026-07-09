package quic

import "time"

// Probe-timeout constants (RFC 9002 §6.2).
const (
	kInitialRtt   = 333 * time.Millisecond // §6.2.2 initial RTT before any sample
	maxAckDelay   = 25 * time.Millisecond  // assumed peer max_ack_delay (application space)
	maxPTOBackoff = 8                      // give up after this many doublings
	idleTimeout   = 10 * time.Second       // give-up bound when nothing is in flight to probe
)

// ptoPeriod is the current probe timeout (RFC 9002 §6.2.1): smoothed_rtt +
// max(4*rttvar, granularity) + max_ack_delay, doubled per prior probe. Before
// any RTT sample it is 2*kInitialRtt (§6.2.2).
func (c *Conn) ptoPeriod() time.Duration {
	var base time.Duration
	if !c.rtt.haveSample {
		base = 2 * kInitialRtt
	} else {
		v := 4 * c.rtt.rttvar
		if v < kGranularity {
			v = kGranularity
		}
		base = c.rtt.smoothedRTT + v
		if c.handshakeComplete {
			base += maxAckDelay // only the application space carries ack delay
		}
	}
	return base << c.ptoCount
}

// hasInFlight reports whether any ack-eliciting packet is unacknowledged in any
// space — the condition under which the probe timer runs (RFC 9002 §6.2.1).
func (c *Conn) hasInFlight() bool {
	for sp := 0; sp < numSpaces; sp++ {
		for _, p := range c.sent[sp].packets {
			if p.ackEliciting {
				return true
			}
		}
	}
	return false
}

// onPTO handles a probe-timeout expiry (RFC 9002 §6.2.4): it re-queues the
// oldest unacknowledged ack-eliciting packet's frames in each space as a probe
// (which elicits an ACK and, once one arrives, drives loss detection), and backs
// off the timer.
func (c *Conn) onPTO() {
	for sp := 0; sp < numSpaces; sp++ {
		c.queueOldestProbe(sp)
	}
	if c.ptoCount < maxPTOBackoff {
		c.ptoCount++
	}
}

// queueOldestProbe re-queues the frames of the oldest unacknowledged
// ack-eliciting packet in space sp for resend, and drops that packet from the
// in-flight set (the data is now tracked under the retransmit's new number).
func (c *Conn) queueOldestProbe(sp int) {
	var oldestPN uint64
	var oldest sentPacket
	found := false
	for pn, p := range c.sent[sp].packets {
		if !p.ackEliciting || len(p.frames) == 0 {
			continue
		}
		if !found || p.timeSent.Before(oldest.timeSent) {
			oldest, oldestPN, found = p, pn, true
		}
	}
	if !found {
		return
	}
	c.retransQueue[sp] = append(c.retransQueue[sp], oldest.frames...)
	c.removeInFlight(oldest.size) // abandoned packet leaves flight; the resend re-counts (RFC 9002 §7)
	delete(c.sent[sp].packets, oldestPN)
}

// isTimeout reports whether err is a deadline-exceeded (timeout) error, without
// importing net (any net.Error satisfies this interface).
func isTimeout(err error) bool {
	t, ok := err.(interface{ Timeout() bool })
	return ok && t.Timeout()
}

// readWithPTO reads one datagram, bounding the wait by the probe timeout when
// the transport exposes SetReadDeadline. On a PTO expiry with data in flight it
// sends a probe, backs off, and retries; otherwise a timeout is returned to the
// caller. It preserves the plain-Read behavior for transports without deadlines.
func (c *Conn) readWithPTO(buf []byte) (int, error) {
	dl, hasDeadline := c.pc.(interface{ SetReadDeadline(time.Time) error })
	for {
		if hasDeadline {
			wait := idleTimeout
			if c.hasInFlight() {
				wait = c.ptoPeriod()
			}
			_ = dl.SetReadDeadline(c.clock().Add(wait))
		}
		n, err := c.pc.Read(buf)
		if err == nil {
			return n, nil
		}
		if !hasDeadline || !isTimeout(err) || !c.hasInFlight() || c.ptoCount >= maxPTOBackoff {
			return 0, err // real error, unprobeable, nothing to probe, or gave up
		}
		c.onPTO()
		if err := c.flush(); err != nil {
			return 0, err
		}
	}
}
