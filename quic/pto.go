package quic

import (
	"context"
	"time"
)

// pastDeadline is a fixed instant in the past. Setting it as the read deadline
// unblocks an in-flight Read immediately (used to interrupt on context cancel).
var pastDeadline = time.Unix(1, 0)

// Probe-timeout constants (RFC 9002 §6.2).
const (
	kInitialRtt   = 333 * time.Millisecond // §6.2.2 initial RTT before any sample
	maxPTOBackoff = 8                      // give up after this many doublings
	idleTimeout   = 10 * time.Second       // give-up bound when nothing is in flight to probe
)

// ptoBase is the un-backed-off probe timeout (RFC 9002 §6.2.1): smoothed_rtt +
// max(4*rttvar, granularity) + max_ack_delay. Before any RTT sample it is
// 2*kInitialRtt (§6.2.2). ptoPeriod applies the exponential backoff; the
// persistent-congestion duration (§7.6.2) multiplies this base by a threshold.
func (c *Conn) ptoBase() time.Duration {
	if !c.rtt.haveSample {
		return 2 * kInitialRtt
	}
	v := 4 * c.rtt.rttvar
	if v < kGranularity {
		v = kGranularity
	}
	base := c.rtt.smoothedRTT + v
	if c.handshakeComplete {
		base += c.peer.MaxAckDelay // only the application space carries ack delay
	}
	return base
}

// ptoPeriod is the current probe timeout: the base PTO doubled per prior probe
// (RFC 9002 §6.2.1).
func (c *Conn) ptoPeriod() time.Duration {
	return c.ptoBase() << c.ptoCount
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
	queued := false
	for sp := 0; sp < numSpaces; sp++ {
		if c.queueOldestProbe(sp) {
			queued = true
		}
	}
	// A PTO MUST send at least one ack-eliciting packet (RFC 9002 §6.2.4). If no
	// space had retransmittable frames to resend — the only in-flight ack-eliciting
	// data is a lone *_BLOCKED or PING, which carry no retransmit descriptor — send
	// a fresh PING so the probe still elicits an ACK, rather than backing off
	// without probing.
	if !queued && c.hasInFlight() {
		c.probePending = true
	}
	if c.ptoCount < maxPTOBackoff {
		c.ptoCount++
	}
}

// queueOldestProbe re-queues the frames of the oldest unacknowledged
// ack-eliciting packet in space sp for resend, and drops that packet from the
// in-flight set (the data is now tracked under the retransmit's new number). It
// reports whether it queued a probe.
func (c *Conn) queueOldestProbe(sp int) bool {
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
		return false
	}
	c.retransQueue[sp] = append(c.retransQueue[sp], oldest.frames...)
	c.removeInFlight(oldest.size) // abandoned packet leaves flight; the resend re-counts (RFC 9002 §7)
	delete(c.sent[sp].packets, oldestPN)
	return true
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
func (c *Conn) readWithPTO(ctx context.Context, buf []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	dl, hasDeadline := c.pc.(interface{ SetReadDeadline(time.Time) error })
	if hasDeadline && ctx.Done() != nil {
		// Watchdog: on context cancel or deadline, poke the read deadline into the
		// past to unblock the in-flight Read. SetReadDeadline is safe to call
		// concurrently with Read (net.Conn contract), and the goroutine touches no
		// other Conn state, so it does not race the single-goroutine engine. It is
		// torn down on every return via the deferred close(stop).
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				_ = dl.SetReadDeadline(pastDeadline)
			case <-stop:
			}
		}()
	}
	for {
		if hasDeadline {
			wait := idleTimeout
			if c.hasInFlight() {
				wait = c.ptoPeriod()
			}
			deadline := c.clock().Add(wait)
			if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
				deadline = d // honor a nearer context deadline
			}
			_ = dl.SetReadDeadline(deadline)
		}
		// Re-check after arming: a cancel between arming and Read would otherwise
		// be masked by the freshly-armed future deadline (deadline-clobber race).
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n, err := c.pc.Read(buf)
		if err == nil {
			return n, nil
		}
		// A context cancel/deadline is a terminal exit — never a PTO retry, which
		// would re-arm a future deadline and block again (the watchdog fires once).
		if e := ctx.Err(); e != nil {
			return 0, e
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
