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
	// handshakeTimeout bounds the whole handshake. The anti-deadlock PTO (§6.2.2.1)
	// keeps probing while the handshake is incomplete, and a server that
	// acknowledges each probe but never completes would otherwise reset the backoff
	// and idle timers forever; this caps that.
	handshakeTimeout = 10 * time.Second
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
	if c.handshakeConfirmed {
		// Only the Application space carries max_ack_delay, and that space has no PTO
		// timer until the handshake is confirmed (RFC 9002 §6.2.1).
		base += c.peer.MaxAckDelay
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
		if !c.ptoArmed(sp) {
			continue
		}
		for _, p := range c.sent[sp].packets {
			if p.ackEliciting {
				return true
			}
		}
	}
	return false
}

// ptoArmed reports whether space sp participates in the probe timer. RFC 9002
// §6.2.1: "An endpoint MUST NOT set its PTO timer for the Application Data packet
// number space until the handshake is confirmed." Between TLS completion and
// HANDSHAKE_DONE the server may not hold 1-RTT read keys yet, so a 1-RTT probe is
// guaranteed-spurious retransmission.
func (c *Conn) ptoArmed(sp int) bool {
	return sp != spaceApp || c.handshakeConfirmed
}

// onPTO handles a probe-timeout expiry (RFC 9002 §6.2.4): it re-queues the
// oldest unacknowledged ack-eliciting packet's frames in each space as a probe
// (which elicits an ACK and, once one arrives, drives loss detection), and backs
// off the timer. Assumes c.mu is held (called from the receive path).
func (c *Conn) onPTO() {
	queued := false
	for sp := 0; sp < numSpaces; sp++ {
		if !c.ptoArmed(sp) {
			continue
		}
		if c.queueOldestProbe(sp) {
			queued = true
		}
	}
	// A PTO MUST send at least one ack-eliciting packet (RFC 9002 §6.2.4). If no
	// space had retransmittable frames to resend — the only in-flight ack-eliciting
	// data is a lone *_BLOCKED or PING, which carry no retransmit descriptor — send
	// a fresh PING so the probe still elicits an ACK, rather than backing off
	// without probing. Before the handshake completes the probe is an Initial or
	// Handshake PING to unblock an anti-amplification-limited server (§6.2.2.1),
	// even with nothing in flight; afterward it is an application-space PING.
	if queued {
		// The probe rides the retransmit queue; let one packet past the congestion
		// window on this PTO, since a PTO probe is exempt from cwnd (RFC 9002 §7) and
		// a PTO MUST send at least one ack-eliciting packet (§6.2.4).
		c.ptoExempt = true
	} else {
		if c.handshakeAntiDeadlock() {
			c.handshakeProbe = true
		} else if c.hasInFlight() {
			c.probePending = true
		}
	}
	if c.ptoCount < maxPTOBackoff {
		c.ptoCount++
	}
}

// handshakeAntiDeadlock reports whether the client must keep a probe timer running
// even with no ack-eliciting packet in flight: until its handshake completes, an
// anti-amplification-limited server may be blocked waiting for the client, so the
// client probes to unblock it (RFC 9002 §6.2.2.1).
func (c *Conn) handshakeAntiDeadlock() bool {
	return !c.handshakeComplete
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
		if !p.ackEliciting || !p.hasFrame {
			continue
		}
		if !found || p.timeSent.Before(oldest.timeSent) {
			oldest, oldestPN, found = p, pn, true
		}
	}
	if !found {
		return false
	}
	c.retransQueue[sp] = append(c.retransQueue[sp], oldest.frame) // found implies hasFrame
	c.removeInFlight(oldest.size)                                 // abandoned packet leaves flight; the resend re-counts (RFC 9002 §7)
	delete(c.sent[sp].packets, oldestPN)
	return true
}

// isTimeout reports whether err is a deadline-exceeded (timeout) error, without
// importing net (any net.Error satisfies this interface).
func isTimeout(err error) bool {
	t, ok := err.(interface{ Timeout() bool })
	return ok && t.Timeout()
}

// lossDetectionDeadline returns when the loss-detection timer next expires and
// whether it is a time-threshold loss timer (RFC 9002 §6.2). A pending
// time-threshold loss takes priority over the probe timeout; with neither a
// pending loss nor anything in flight it is the negotiated idle timeout, or a
// fixed give-up bound when no idle timeout is in effect.
func (c *Conn) lossDetectionDeadline() (time.Time, bool) {
	if lt, _, ok := c.earliestLossTime(); ok {
		return lt, true
	}
	if c.hasInFlight() || c.handshakeAntiDeadlock() {
		return c.clock().Add(c.ptoPeriod()), false
	}
	if idleDL, ok := c.idleDeadline(); ok {
		return idleDL, false
	}
	return c.clock().Add(idleTimeout), false
}

// effectiveIdle is the negotiated idle timeout: the smaller of the two endpoints'
// advertised max_idle_timeout values, where zero (absent or explicit) means that
// endpoint imposes none (RFC 9000 §10.1). Zero means no idle timeout is in effect.
func effectiveIdle(local, peer time.Duration) time.Duration {
	switch {
	case local == 0:
		return peer
	case peer == 0:
		return local
	case local < peer:
		return local
	default:
		return peer
	}
}

// idleDeadline returns the instant the connection is idle-closed (RFC 9000 §10.1)
// and whether an idle timeout is in effect. The period is floored at three PTOs so
// a run of lost probes cannot trip an idle close during loss recovery.
func (c *Conn) idleDeadline() (time.Time, bool) {
	eff := effectiveIdle(c.localMaxIdle, c.peer.MaxIdleTimeout)
	if eff == 0 {
		return time.Time{}, false
	}
	if floor := 3 * c.ptoPeriod(); eff < floor {
		eff = floor
	}
	return c.lastActivity.Add(eff), true
}

// idleClose silently closes the connection after the idle timeout (RFC 9000
// §10.1): no CONNECTION_CLOSE is sent — the state is discarded — and
// ErrIdleTimeout is returned so the caller learns why the read ended.
func (c *Conn) idleClose() error {
	// Latch the single-close state so a blocked WaitReadable / WaitSendable wakes
	// with ErrIdleTimeout (docs/HTTP3_DESIGN.md §3.3, F5).
	c.terminateLocked(ErrIdleTimeout)
	c.closed = true
	_ = c.pc.Close()
	return ErrIdleTimeout
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
			deadline, _ := c.lossDetectionDeadline()
			if idleDL, ok := c.idleDeadline(); ok && idleDL.Before(deadline) {
				deadline = idleDL // idle timeout may be nearer than a probe (§10.1)
			}
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
		if !hasDeadline || !isTimeout(err) {
			return 0, err // real error, or a transport without deadlines
		}
		// The connection is silently closed once it has been idle for the negotiated
		// max_idle_timeout (RFC 9000 §10.1), checked before probing so a due idle
		// close is not masked by a probe.
		if idleDL, ok := c.idleDeadline(); ok && !idleDL.After(c.clock()) {
			return 0, c.idleClose()
		}
		// A time-threshold loss timer expiry runs loss detection; otherwise the
		// probe timeout resends (RFC 9002 §6.2). With neither a pending loss nor a
		// probe to send, the idle bound elapsed with nothing to do.
		switch lt, sp, ok := c.earliestLossTime(); {
		case ok && !lt.After(c.clock()):
			c.detectLost(sp)
		case (c.hasInFlight() || c.handshakeAntiDeadlock()) && c.ptoCount < maxPTOBackoff:
			c.onPTO()
		default:
			return 0, err // idle timeout or probe backoff exhausted
		}
		if err := c.flush(); err != nil {
			return 0, err
		}
	}
}
