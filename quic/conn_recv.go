package quic

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// Establish sends the client's Initial flight, then reads datagrams and drives
// the handshake — feeding inbound CRYPTO to TLS, installing keys, and sending
// the client's responding flights and acknowledgements — until the handshake
// completes (RFC 9000 §7, RFC 9001 §4). The whole handshake is bounded by
// handshakeTimeout so an anti-deadlock PTO (§6.2.2.1) cannot probe a server that
// acknowledges but never completes forever; the caller's ctx deadline, if nearer,
// still applies.
func (c *Conn) Establish(ctx context.Context) error {
	// Hold c.mu for the whole handshake so its calls to fail (and the send/recv
	// helpers) are unconditionally assume-held internals — one convention with no
	// pre-reader special case. Establish runs before any concurrency, so holding
	// the lock across the handshake reads costs nothing.
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	if err := c.sendInitialFlight(ctx); err != nil {
		return err
	}
	buf := make([]byte, 2048)
	for !c.handshakeComplete && !c.closed {
		n, err := c.readWithPTO(ctx, buf)
		if err != nil {
			return err
		}
		if err := c.recvDatagram(buf[:n]); err != nil {
			return c.fail(err) // signal a protocol violation with CONNECTION_CLOSE
		}
		if err := c.flush(); err != nil {
			return err
		}
	}
	if !c.handshakeComplete {
		return ErrHandshakeClosed // loop exited on c.closed before completing
	}
	return nil
}

// Poll reads a datagram (or, on a GRO-capable transport, a whole coalesced burst)
// and processes it — dispatching frames to open streams and sending
// acknowledgements — for driving receive after the handshake completes (RFC 9000
// §13). It runs on the connection's reader goroutine.
//
// The one structural change from the single-goroutine era (docs/HTTP3_DESIGN.md
// §3.2): the blocking read (c.readPacket) runs with c.mu RELEASED, so a concurrent
// Send can seal and write a request while the reader is parked. The sequence is:
// lock → leading flush (retransmits) → publish+arm the read deadline → arm→recheck
// ctx → UNLOCK → blocking read → relock → timestamp after reacquiring → process.
// Every error return latches terminateLocked so a blocked Do wakes.
//
// POSTCONDITION: Poll never returns holding c.mu — serviceControl and the H3
// control servicing on the reader goroutine rely on this.
func (c *Conn) Poll(ctx context.Context) error {
	c.mu.Lock()
	// Leading flush: retransmits queued by reader-side detectLost, and any credit
	// grants, must go out before we park (the original leading flush).
	if err := c.flush(); err != nil {
		c.terminateLocked(err)
		c.mu.Unlock()
		return err
	}
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.pollBuf == nil {
		// Enlarged to a GRO burst when the transport batches receive, else the
		// single-datagram size (quic/gro.go).
		c.pollBuf = make([]byte, c.pollBufLen())
	}
	// One connection-lifetime watchdog: on ctx (connCtx) cancel it pokes the read
	// deadline into the past to unblock the parked Read (§3.1, INV-4).
	c.ensureReadWatchdog(ctx)
	// Compute and publish the read deadline under the lock before arming it, so a
	// Do-side send can legally shorten it against the blocked Read (§4 INV-4). The
	// loss/idle/ctx deadline is retained separately (armedLossDeadline) so a deferred
	// ACK, which may pull the read deadline nearer, does not disguise itself as a
	// loss/PTO expiry (RFC 9000 §13.2.1; see handleExpiry).
	base := c.computeReadDeadline(ctx)
	c.armedLossDeadline = base
	dl := base
	c.armedForAck = false
	if !c.ackDeadline.IsZero() && c.ackDeadline.Before(dl) {
		dl = c.ackDeadline // wake to flush the deferred ACK within max_ack_delay
		c.armedForAck = true
	}
	c.armedReadDeadline = dl
	c.setReadDeadline(dl)
	// Arm→recheck guard: a cancel that fired before we armed must not be masked by
	// the freshly-armed future deadline (the deadline-clobber race).
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	// BLOCKING, UNLOCKED — a Do may seal+send here. On a GRO-capable transport this
	// returns a whole coalesced burst (segSize>0) in one syscall; else one datagram.
	n, segSize, err := c.readPacket()

	c.mu.Lock()
	now := c.clock() // timestamp AFTER reacquiring, so a Do's hold time does not skew RTT
	perr := c.afterReadLocked(ctx, n, segSize, err, now)
	if perr != nil {
		// Catch-all latch: every teardown funnels through terminateLocked so a
		// blocked WaitReadable / WaitSendable wakes (§3.3). First-error-wins, so a
		// graceful Close that latched before connCancel is preserved over ctx.Err().
		c.terminateLocked(perr)
	}
	c.mu.Unlock()
	return perr
}

// afterReadLocked processes the outcome of the unlocked read. err is the read
// error (nil on data), n the bytes read into c.pollBuf, and segSize the GRO
// segment size (0 for a single datagram; >0 when c.pollBuf[:n] holds several
// coalesced datagrams of segSize bytes). Assumes c.mu is held.
func (c *Conn) afterReadLocked(ctx context.Context, n, segSize int, err error, now time.Time) error {
	// A ctx (connCtx) cancel unblocked the Read via the watchdog's past deadline;
	// surface it as terminal, never as a PTO retry (which would re-arm and block).
	if e := ctx.Err(); e != nil {
		return e
	}
	if err != nil {
		if !isTimeout(err) {
			return err // a real I/O error ends the connection
		}
		return c.handleExpiry(now, err) // deadline expiry: PTO / loss / idle
	}
	// A GRO read hands back several coalesced datagrams; recvGRO splits them and
	// feeds each through the same recvDatagram the single-read path uses. A non-GRO
	// read (segSize 0) is one recvDatagram, unchanged.
	if e := c.recvGRO(n, segSize); e != nil {
		return c.fail(e)
	}
	if c.peerClose != nil {
		return c.peerClose // a received CONNECTION_CLOSE ended the connection (§10.2.2)
	}
	c.discardStaleKeys() // drop a superseded key-update generation past its window (§6.3)
	// Drain datagrams already buffered in the socket without blocking, so a
	// server's response burst is processed and acknowledged as one batch rather
	// than one datagram per Poll (RFC 9002 §7).
	if e := c.drainBuffered(); e != nil {
		return e
	}
	if c.peerClose != nil {
		return c.peerClose // a CONNECTION_CLOSE arrived in the drained burst (§10.2.2)
	}
	// End of the receive burst: if it freed congestion-window or connection credit,
	// wake senders parked on blockCong/blockConn in one O(n) sweep (§3.3, INV-5).
	c.maybeBroadcastSendWindow()
	// Decide the Application-space ACK cadence for this burst (RFC 9000 §13.2.1):
	// send now on an immediate trigger, else arm the max_ack_delay deferral so the
	// owed ACK rides the next outbound packet or fires on the timer. The flush below
	// emits it only if now due.
	c.updateAppAckDeferral(now)
	return c.flush()
}

// handleExpiry handles a read-deadline expiry (the moved readWithPTO expiry
// branch, §3.2): an idle close if due, else one loss-detection or PTO step, then a
// flush. Returning nil lets the reader re-poll (re-arm + read) for the next step;
// a non-nil error (idle close, or the give-up timeout) ends the connection. Assumes
// c.mu is held; now is the post-reacquire timestamp.
func (c *Conn) handleExpiry(now time.Time, timeout error) error {
	// The read deadline may have been pulled nearer by a deferred Application-space
	// ACK (RFC 9000 §13.2.1), so a wake no longer implies a loss/PTO/idle event is
	// due. When the reader armed FOR the ACK deadline (armedForAck, captured at arm
	// time so a concurrent piggyback that clears ackDeadline cannot flip it), run the
	// loss/PTO/idle machinery only once its own (pre-ACK) deadline has genuinely
	// elapsed; otherwise this is an ACK-only wake and the trailing flush sends the
	// owed ACK without touching ptoCount — a deferred ACK must never provoke a
	// spurious PTO probe or retransmit. When armed for the loss/idle deadline, a
	// timeout means that event is due (unchanged pre-deferral behavior).
	lossIdleDue := !c.armedForAck || !c.armedLossDeadline.After(now)
	if lossIdleDue {
		// Idle close first, gated on now >= idleDeadline, so an early PTO wake (a
		// Do-side re-arm shortened the deadline) never mis-fires an idle close before
		// probing.
		if idleDL, ok := c.idleDeadline(); ok && !idleDL.After(now) {
			return c.idleClose()
		}
		switch lt, sp, ok := c.earliestLossTime(); {
		case ok && !lt.After(now):
			c.detectLost(sp)
		case (c.hasInFlight() || c.handshakeAntiDeadlock()) && c.ptoCount < maxPTOBackoff:
			c.onPTO()
		default:
			// Nothing to probe. If an ACK is not owed either, the idle bound elapsed
			// with nothing to do / the backoff is exhausted → surface the timeout to
			// end the connection; otherwise fall through to flush the owed ACK.
			if !c.ackDue(spaceApp) {
				return timeout
			}
		}
	}
	return c.flush()
}

// computeReadDeadline is the instant the reader arms as the blocking-read deadline:
// the nearest of the loss-detection deadline, the idle deadline, and the caller's
// context deadline (mirrors the moved readWithPTO deadline computation).
func (c *Conn) computeReadDeadline(ctx context.Context) time.Time {
	dl, _ := c.lossDetectionDeadline()
	if idleDL, ok := c.idleDeadline(); ok && idleDL.Before(dl) {
		dl = idleDL // idle timeout may be nearer than a probe (§10.1)
	}
	if d, ok := ctx.Deadline(); ok && d.Before(dl) {
		dl = d
	}
	return dl
}

// deadlineSetter is the one optional PacketConn capability this package depends
// on: the receive path uses it to drain without blocking, the ACK timer to fire
// within max_ack_delay, and PTO to bound a probe wait. A transport without it (a
// plain pipe in a unit test) degrades to blocking one-datagram reads, which every
// caller handles.
//
// It is asserted per call rather than resolved once into a Conn field, and that
// is deliberate. 206 struct literals across 69 test files build &Conn{pc: ...}
// directly, 57 of them with a deadline-capable transport; a field populated only
// by NewConn and NewServerConn would read as nil for all of them and silently
// switch off the behaviour those tests exercise. Asserting to a single-method
// interface is an itab lookup the runtime caches.
type deadlineSetter interface {
	SetReadDeadline(time.Time) error
}

// readDeadliner returns the transport's deadline setter, if it has one.
func (c *Conn) readDeadliner() (deadlineSetter, bool) {
	dl, ok := c.pc.(deadlineSetter)
	return dl, ok
}

// setReadDeadline arms the PacketConn read deadline when the transport supports it
// (a no-op otherwise, preserving the plain-Read behavior of deadline-less pipes).
func (c *Conn) setReadDeadline(t time.Time) {
	if dl, ok := c.readDeadliner(); ok {
		_ = dl.SetReadDeadline(t)
	}
}

// canScheduleAckTimer reports whether the transport can arm a read deadline, the
// mechanism a deferred ACK relies on to fire within max_ack_delay when no outbound
// packet carries it first. A transport without it (a plain pipe in a unit test)
// cannot schedule the fallback, so an ACK is never deferred there — it is sent
// immediately, preserving the pre-deferral behavior and never risking a stall.
func (c *Conn) canScheduleAckTimer() bool {
	_, ok := c.readDeadliner()
	return ok
}

// updateAppAckDeferral decides, at the end of a receive burst, whether the owed
// Application-space ACK is sent now or deferred (RFC 9000 §13.2.1). Only the 1-RTT
// space defers, and only once the handshake is complete — Initial/Handshake ACKs
// and the pre-1-RTT path stay immediate. On an immediate trigger (an out-of-order
// arrival or the 2nd ack-eliciting packet since the last ACK, tracked in
// acks[spaceApp]) it clears the deadline so flush emits the ACK now. Otherwise it
// arms the deadline at now + max_ack_delay, anchored to the first deferred packet
// so a run of in-order packets cannot push the ACK past the bound. Assumes c.mu.
func (c *Conn) updateAppAckDeferral(now time.Time) {
	if !c.handshakeComplete || !c.acks[spaceApp].pending {
		return
	}
	if c.acks[spaceApp].immediate || !c.canScheduleAckTimer() {
		c.ackDeadline = time.Time{} // send now: §13.2.1 immediate, or no fallback timer
		return
	}
	if c.ackDeadline.IsZero() {
		// We advertise no max_ack_delay, so our value is the RFC 9000 §18.2 default —
		// and §18.2 says the advertised value "SHOULD include the receiver's expected
		// delays in alarms firing". Arming at exactly the advertised bound leaves zero
		// budget for read-deadline and scheduler slop, so our real worst case would
		// routinely exceed what the peer folds into its PTO. Arm a slop short of it.
		c.ackDeadline = now.Add(defaultMaxAckDelay - ackAlarmSlop)
	}
}

// ackDue reports whether the owed ACK for space sp must be written now. Initial and
// Handshake spaces, and the Application space before the handshake completes, ACK
// immediately (RFC 9000 §13.2.1). A 1-RTT ACK is due on an immediate trigger, when
// no deferral is armed (an unset deadline — e.g. a transport that cannot schedule
// the timer), or once its max_ack_delay deadline has elapsed. Assumes c.mu.
func (c *Conn) ackDue(sp int) bool {
	if !c.acks[sp].pending {
		return false
	}
	if sp != spaceApp || !c.handshakeComplete {
		return true
	}
	if c.acks[sp].immediate || c.ackDeadline.IsZero() {
		return true
	}
	return !c.ackDeadline.After(c.clock())
}

// rearmReadDeadline shortens the parked reader's read deadline after a Do-side send
// put a packet in flight (docs/HTTP3_DESIGN.md §4 INV-4): the loss-detection
// deadline is now sub-second where the reader armed the idle scale, so the reader
// must wake to run PTO/loss detection. SetReadDeadline is legal against a blocked
// Read. A no-op when no reader has parked (armedReadDeadline zero). Assumes c.mu is
// held (called from the send epilogues, which hold it after sealPacket).
func (c *Conn) rearmReadDeadline() {
	if c.armedReadDeadline.IsZero() {
		return
	}
	newLossDL, _ := c.lossDetectionDeadline()
	if newLossDL.Before(c.armedLossDeadline) {
		c.armedLossDeadline = newLossDL // the in-flight packet shortened the loss/PTO timer
	}
	// The wake deadline is the nearer of the loss/PTO deadline and any deferred-ACK
	// deadline (RFC 9000 §13.2.1), so the owed ACK still fires within max_ack_delay.
	dl := c.armedLossDeadline
	forAck := false
	if !c.ackDeadline.IsZero() && c.ackDeadline.Before(dl) {
		dl = c.ackDeadline
		forAck = true
	}
	if dl.Before(c.armedReadDeadline) {
		c.setReadDeadline(dl)
		c.armedReadDeadline = dl
		c.armedForAck = forAck // this send moved the binding deadline; record which
	}
}

// ensureReadWatchdog starts, once per connection, the goroutine that pokes the read
// deadline into the past when ctx (the connection-lifetime connCtx) is cancelled,
// so a blocked pc.Read unblocks on Close (docs/HTTP3_DESIGN.md §3.1). It touches
// only pc.SetReadDeadline (safe concurrently with Read per the net.Conn contract),
// so it never races the engine, and exits when ctx is done. A no-op for a ctx with
// no Done channel (hand-built test conns) or a transport without deadlines. Assumes
// c.mu is held.
func (c *Conn) ensureReadWatchdog(ctx context.Context) {
	if c.readWatchdogStarted || ctx.Done() == nil {
		return
	}
	dl, ok := c.readDeadliner()
	if !ok {
		c.readWatchdogStarted = true // no deadline support: nothing to watch
		return
	}
	c.readWatchdogStarted = true
	go func() {
		<-ctx.Done()
		_ = dl.SetReadDeadline(pastDeadline)
	}()
}

// flushControl emits the app-space credit grants (MAX_DATA / MAX_STREAM_DATA) and a
// PATH_RESPONSE queued in pendingCtrl, on the CONSUMER's goroutine, before it
// releases c.mu (docs/HTTP3_DESIGN.md §4 INV-3): the goroutine that consumed
// received bytes grants its own credit immediately rather than waiting for the
// parked reader's next flush, so a flow-control-blocked peer is unblocked without a
// round trip. It emits ONLY pendingCtrl / PATH_RESPONSE, NEVER the pending ACK — the
// ACK cadence stays reader-owned (INV-6). A PATH_RESPONSE keeps its 1200-byte
// padding (BREAK3, RFC 9000 §8.2.2). Assumes c.mu is held.
func (c *Conn) flushControl() error {
	if len(c.pendingCtrl) == 0 {
		return nil
	}
	if c.closed || c.sealerFor(spaceApp) == nil {
		return nil // draining, or 1-RTT keys not yet installed
	}
	var frames []byte
	frames = append(frames, c.pendingCtrl...)
	c.pendingCtrl = c.pendingCtrl[:0]
	// A PATH_RESPONSE that rode along MUST go out in a >=1200-byte datagram even when
	// a Do-side Recv flushes it (BREAK3), so replicate flush's padding decision.
	padPath := c.pathRespPending
	c.pathRespPending = false
	// Credit grants are self-healing (a later grant supersedes a lost one), so they
	// are not retransmitted. The frames are ack-eliciting.
	pkt, err := c.sealPacket(spaceApp, frames, true, nil, padPath)
	if err != nil {
		return err
	}
	if _, err := c.pc.Write(pkt); err != nil {
		return err
	}
	c.rearmReadDeadline() // the grant put a packet in flight (§4 INV-4)
	return nil
}

// maxDrainBurst bounds how many extra datagrams a single Poll drains after its
// blocking read, so the acknowledgement flush is not starved by a peer sending
// as fast as we read. A full receive-window burst fits well under this; the cap
// is only a runaway guard.
const maxDrainBurst = 512

// drainBuffered reads and processes datagrams already waiting in the socket
// without blocking, using a read deadline in the past so an empty socket returns
// immediately. It is a no-op on transports that cannot set a read deadline (they
// have no non-blocking read), preserving their one-datagram-per-Poll behavior.
// Assumes c.mu is held (called from the Poll receive path).
func (c *Conn) drainBuffered() error {
	dl, ok := c.readDeadliner()
	if !ok {
		return nil
	}
	for i := 0; i < maxDrainBurst; i++ {
		_ = dl.SetReadDeadline(c.clock().Add(-time.Nanosecond))
		n, segSize, err := c.readPacket()
		if err != nil {
			if isTimeout(err) {
				return nil // socket drained
			}
			return err
		}
		// One read may be a GRO burst; recvGRO splits it into datagrams and feeds
		// each through the unchanged recvDatagram (segSize 0 = one datagram).
		if err := c.recvGRO(n, segSize); err != nil {
			return c.fail(err)
		}
		if c.peerClose != nil {
			//nolint:nilerr // Not the error path: recvGRO returned nil here.
			// c.peerClose records a CONNECTION_CLOSE, so draining stops
			// (§10.2.2) and the caller surfaces it.
			return nil
		}
	}
	return nil
}

// prefilterPacket handles packets that are dealt with before decryption. It
// returns skip=true when the packet needs no further processing: a Retry (validated
// and re-keyed in place), a Version Negotiation (which may abandon the connection
// via a non-nil err), a server Initial carrying a non-empty Token (RFC 9000
// §17.2.2), or a long-header packet whose Source Connection ID differs from the
// authenticated server one once it is known (§7.2).
func (c *Conn) prefilterPacket(hdr Header, pkt []byte) (skip bool, err error) {
	switch {
	case hdr.Type == PacketVersionNegotiation:
		if c.shouldAbandonOnVN(pkt, hdr) {
			return true, ErrVersionNegotiation
		}
		return true, nil
	case hdr.Type != PacketShort && hdr.Version != QUICVersion1:
		// "If a client receives a packet that uses a different version than it
		// initially selected, it MUST discard that packet" (RFC 9000 §5.2). Silently:
		// anyone can forge a long header, and Initial keys derive from the observable
		// connection ID, so a v1-sealed packet stamped with another version would
		// otherwise authenticate and be processed. Version Negotiation (version 0) is
		// handled above; short headers carry no version field. This gate precedes
		// Retry, whose integrity tag is version-specific (§17.2.5).
		return true, nil
	case hdr.Type == PacketZeroRTT:
		// A client never has 0-RTT read keys — only a server does — so RFC 9001 §5.7
		// says a client "MUST NOT attempt to decrypt 0-RTT packets it receives and
		// instead MUST discard them". Without this case packetSpace's default arm
		// routes 0-RTT into the Initial space and hands the packet to the Initial
		// Opener: Initial keys derive from the observable connection ID with a public
		// salt, so an on-path forger could seal a 0-RTT-typed packet that
		// authenticates and have its frames processed and its SCID adopted. The
		// exported processDatagram helper has always skipped it (keySet.openerFor
		// returns nil for this type); the live path had not.
		return true, nil
	case hdr.Type == PacketRetry:
		c.handleRetry(hdr, pkt) // validate + re-key + resend the Initial (§17.2.5); never fatal
		return true, nil
	case hdr.Type == PacketInitial && len(hdr.Token) > 0:
		// A server's Initial MUST carry an empty Token; discard one with a non-zero
		// Token Length rather than erroring, so an injected Initial cannot end the
		// connection (RFC 9000 §17.2.2).
		return true, nil
	case c.gotServerCID && hdr.Type != PacketShort && !bytesEqual(hdr.SCID, c.serverSCID):
		// A long-header packet with a different Source Connection ID is from another
		// source (§7.2). Short-header packets carry no SCID.
		return true, nil
	}
	return false, nil
}

// recvDatagram decrypts and dispatches every packet coalesced in one datagram.
// Assumes c.mu is held (called from Poll/drainBuffered/Establish).
func (c *Conn) recvDatagram(datagram []byte) error {
	rest := datagram
	first := true
	var firstDCID []byte // the first packet's Destination Connection ID (§12.2)
	for len(rest) > 0 {
		isFirst := first
		first = false
		hdr, err := ParseHeader(rest, len(c.scid))
		if err != nil {
			// An unauthenticated header that cannot be parsed cannot be associated
			// with the connection, so the remainder of the datagram is discarded —
			// never a connection error (RFC 9000 §5.2, §12.2). Stopping the scan
			// (the next packet boundary is unknown) still lets any valid coalesced
			// packets decoded earlier drive the handshake below. This also absorbs
			// trailing datagram padding.
			break
		}
		pkt := rest[:hdr.PacketLen]
		rest = rest[hdr.PacketLen:]
		// A sender MUST NOT coalesce packets with different Destination Connection
		// IDs, and a receiver SHOULD ignore any that follow one that does (RFC 9000
		// §12.2). Skipping, never erroring: the header is unauthenticated.
		if isFirst {
			firstDCID = hdr.DCID
		} else if !bytesEqual(hdr.DCID, firstDCID) {
			continue
		}
		if skip, err := c.prefilterPacket(hdr, pkt); err != nil {
			return err
		} else if skip {
			continue
		}
		sp := packetSpace(hdr.Type)
		pn, payload, ok, err := c.decryptPacket(sp, pkt, hdr.PNOffset, isFirst, datagram)
		if err != nil {
			return err
		}
		if !ok {
			continue // no keys yet, or authentication failed; skip this packet
		}
		// "A receiver MUST discard a newly unprotected packet unless it is certain
		// that it has not processed another packet with the same packet number from
		// the same packet number space" (RFC 9000 §12.3). A bit-identical replayed
		// datagram authenticates — the AEAD nonce derives from the packet number — so
		// without this its frames would be dispatched a second time. Individual frame
		// handlers absorb most duplicates, but the requirement is on the packet.
		//
		// A consequence worth naming: a duplicate no longer marks an ACK owed. That is
		// safe only because QUIC never reuses a packet number — a peer whose ACK was
		// lost re-sends the frames under a NEW number, which is decidably new here. Any
		// future dedup keyed on something other than the packet number would break it.
		if c.acks[sp].seen(pn) {
			continue
		}
		// Adopt the server's connection ID from the first AUTHENTICATED long-header
		// packet (RFC 9000 §7.2). Deferring adoption until after the AEAD succeeds
		// keeps a forged or garbage Initial — which anyone can build, since Initial
		// keys derive from the on-wire connection ID — from poisoning our
		// Destination Connection ID and stalling the handshake.
		if !c.gotServerCID && len(hdr.SCID) > 0 {
			c.dcid = append(c.dcid[:0], hdr.SCID...)
			c.serverSCID = append(c.serverSCID[:0], hdr.SCID...)
			c.gotServerCID = true
		}
		// The header's reserved bits MUST be zero once protection is removed from
		// an authenticated packet (RFC 9000 §17.2 long header, §17.3.1 short
		// header); a non-zero value is a PROTOCOL_VIOLATION. pkt[0] was unmasked by
		// the open path above, and the AEAD success authenticated it.
		reserved := byte(0x0c) // long header (Initial/Handshake)
		if sp == spaceApp {
			reserved = 0x18 // short header (1-RTT)
		}
		if pkt[0]&reserved != 0 {
			return ErrProtocolViolation
		}
		// "An endpoint MUST treat receipt of a packet containing no frames as a
		// connection error of type PROTOCOL_VIOLATION" (RFC 9000 §12.4). The packet
		// authenticated, so unlike the skips above this is the peer's own violation.
		if len(payload) == 0 {
			return ErrProtocolViolation
		}
		fh := &c.fh
		fh.reset(c, sp)
		if err := ParseFrames(payload, fh); err != nil {
			return err
		}
		c.acks[sp].receive(pn, fh.ackEliciting)
		c.lastActivity = c.clock() // a processed packet resets the idle timer (§10.1)
		if !c.haveRecv[sp] || pn > c.largestRecv[sp] {
			c.largestRecv[sp] = pn
			c.haveRecv[sp] = true
		}
		// An ACK removed acknowledged packets and updated the RTT during parsing;
		// now run loss detection with the fresh RTT (RFC 9002 §6.1, §A.7 order).
		if fh.sawAck {
			c.detectLost(sp)
		}
	}
	fedCrypto := false
	for sp := 0; sp < numSpaces; sp++ {
		if data := c.cryptoRecv[sp].read(); len(data) > 0 {
			if err := c.hs.HandleCrypto(spaceLevel(sp), data); err != nil {
				return err
			}
			fedCrypto = true
		}
	}
	// Drive TLS only while the handshake runs or when new CRYPTO arrived
	// (e.g. a post-handshake session ticket); otherwise there is nothing to do.
	if fedCrypto || !c.handshakeComplete {
		return c.hs.Pump(c)
	}
	return nil
}

// connFrameHandler dispatches the frames of one received packet into the Conn.
//
// Every On* handler on this type assumes c.mu is held: they run only from
// recvDatagram, which itself runs inside a locked Poll/Establish section.
// A handler that must mutate send-side stream state (e.g. OnStopSending) calls
// the assume-held …Locked internal, never the re-locking public wrapper.
type connFrameHandler struct {
	nopFrameHandler
	c             *Conn
	space         int
	ackEliciting  bool
	sawAck        bool   // an ACK frame was seen (run loss detection after parsing)
	ackLow        uint64 // smallest packet number of the ACK range being decoded
	priorInFlight uint64 // bytes in flight before this ACK frame, for the §7.8 cwnd-limited test
}

// reset re-arms the handler for one received packet. Every field is per-packet
// state, so this must clear all of them: the handler is reused across packets
// (it lives on the Conn to keep ParseFrames from escaping a fresh one each
// time), and a field left set would carry, say, a previous packet's sawAck into
// a packet that carried no ACK — running loss detection against stale ranges.
func (h *connFrameHandler) reset(c *Conn, space int) {
	h.c = c
	h.space = space
	h.ackEliciting = false
	h.sawAck = false
	h.ackLow = 0
	h.priorInFlight = 0
}

// OnAck processes the first (largest) range of an ACK frame: it acknowledges
// [largest-firstRange, largest] and samples the RTT from the largest packet
// (RFC 9000 §19.3, RFC 9002 §5). ACK frames are not themselves ack-eliciting.
func (h *connFrameHandler) OnAck(largest, ackDelay, firstRange uint64) error {
	h.sawAck = true
	// Capture the bytes in flight before this frame removes any acknowledged
	// packet; the whole frame's ranges share it for the §7.8 cwnd-limited test.
	h.priorInFlight = h.c.bytesInFlight
	// Decode the ACK Delay (RFC 9000 §19.3). The negotiated ack_delay_exponent
	// applies only to the Application space; the Initial and Handshake spaces use a
	// fixed exponent of 3 (the transport parameters are not yet available there).
	exp := defaultAckDelayExponent
	if h.space == spaceApp {
		exp = h.c.peer.AckDelayExponent
	}
	delay := decodeAckDelay(ackDelay, exp)
	// Once the handshake is confirmed, clamp the Application-space ack delay to the
	// peer's max_ack_delay (RFC 9002 §5.3) so an overstated delay cannot deflate
	// the RTT estimate.
	if h.space == spaceApp && h.c.handshakeConfirmed && delay > h.c.peer.MaxAckDelay {
		delay = h.c.peer.MaxAckDelay
	}
	// The smallest packet number in the first range is Largest - First ACK Range;
	// if that is negative the ACK is malformed (RFC 9000 §19.3.1).
	if firstRange > largest {
		return ErrFrameEncoding
	}
	// An ACK acknowledging a packet number we never sent is a PROTOCOL_VIOLATION
	// (RFC 9000 §13.1). Packet numbers are assigned sequentially with no gaps, so
	// sendPN — the next number to send — is one past every packet in flight; a
	// Largest Acknowledged at or above it cannot name a packet we sent.
	if largest >= h.c.sendPN[h.space] {
		return ErrProtocolViolation
	}
	low := largest - firstRange
	h.c.onAckRange(h.space, low, largest, delay, h.priorInFlight)
	h.ackLow = low
	return nil
}

// maxAckDelayMicros bounds a decoded ACK Delay (in microseconds) so that shifting
// a peer-controlled varint by the exponent, and converting the result to a
// Duration in nanoseconds, cannot overflow int64 into a negative or wrapped
// value. The cap (~2^53 µs ≈ 285 years) is far above any real delay, so a
// conformant peer is unaffected; an oversized value is harmless — it is clamped
// to max_ack_delay for the Application space and rejected by rtt.update's min_rtt
// guard for the handshake spaces.
const maxAckDelayMicros uint64 = 1 << 53

// decodeAckDelay converts an ACK Delay field value and its exponent to a
// duration, saturating at maxAckDelayMicros rather than overflowing.
func decodeAckDelay(ackDelay, exp uint64) time.Duration {
	if exp >= 64 || ackDelay > maxAckDelayMicros>>exp {
		return time.Duration(maxAckDelayMicros) * time.Microsecond
	}
	return time.Duration(ackDelay<<exp) * time.Microsecond
}

// OnAckRange processes an additional ACK range below the previous one: a gap of
// gap+1 unacknowledged packets, then length+1 acknowledged (RFC 9000 §19.3).
func (h *connFrameHandler) OnAckRange(gap, length uint64) error {
	// This range's highest packet is ackLow - Gap - 2 and its lowest is that minus
	// Length; if either computed packet number is negative the ACK is malformed
	// (RFC 9000 §19.3.1). gap and length are varints (≤ 2^62-1), so gap+2 cannot
	// overflow uint64.
	if gap+2 > h.ackLow {
		return ErrFrameEncoding
	}
	high := h.ackLow - gap - 2
	if length > high {
		return ErrFrameEncoding
	}
	low := high - length
	h.c.onAckRange(h.space, low, high, 0, h.priorInFlight) // only the first range carries the largest
	h.ackLow = low
	return nil
}

// maxCryptoBuffer bounds how far past the consumed prefix a gapped CRYPTO frame
// may reach. A real handshake is a few KB delivered largely in order, so 64 KiB
// of headroom never trips on legitimate traffic while capping the reassembly
// buffer a peer without a flow-control gate could otherwise grow without bound.
const maxCryptoBuffer uint64 = 64 << 10

func (h *connFrameHandler) OnCrypto(offset uint64, data []byte) error {
	// The sum of a CRYPTO frame's offset and length cannot exceed 2^62-1
	// (RFC 9000 §19.6); a frame beyond that limit is a FRAME_ENCODING_ERROR. Unlike
	// a STREAM frame, CRYPTO has no flow-control gate, so this is the only bound.
	// offset is a varint (≤ MaxVarint) and len(data) fits a datagram, so the sum
	// cannot overflow uint64.
	if offset+uint64(len(data)) > bytesx.MaxVarint {
		return ErrFrameEncoding
	}
	// CRYPTO has no flow-control window (RFC 9000 §7.5), so bound how far ahead of
	// the consumed prefix a gapped frame may sit. Without this a peer — or an
	// on-path forger of Initial packets — could buffer unbounded gapped handshake
	// bytes that never become contiguous and so are never handed to (and rejected
	// by) the TLS layer. maxCryptoBuffer past the read cursor is ample for a real
	// certificate flight, which is delivered largely in order.
	cr := &h.c.cryptoRecv[h.space]
	if offset+uint64(len(data)) > cr.base+maxCryptoBuffer {
		return ErrCryptoBufferExceeded // §7.5 permits closing with CRYPTO_BUFFER_EXCEEDED
	}
	// The handshake CRYPTO stream spans many frames and packets (a server's
	// certificate flight is several KB); reassemble it by offset so out-of-order
	// or gapped delivery still yields the TLS messages in order.
	//
	// Propagate receive's error. There is no FIN here, so ErrFinalSize never
	// arises — but receive also returns ErrProtocolViolation once the retained
	// out-of-order range count crosses maxRecvGapChunks, the anti-quadratic
	// reassembly cap. Discarding it left the CRYPTO stream (which has no
	// flow-control window) open to the same O(n^2) bufferGap denial of service the
	// STREAM path is protected against, since the byte-window bound alone still
	// admits ~32K one-byte gapped chunks. The sibling OnStream propagates the same
	// error.
	if err := cr.receive(offset, data, false); err != nil {
		return err
	}
	h.ackEliciting = true
	return nil
}

func (h *connFrameHandler) OnPing() error { h.ackEliciting = true; return nil }

// OnDataBlocked, OnStreamDataBlocked, and OnStreamsBlocked handle the peer's
// flow-control-blocked signals (RFC 9000 §19.12–§19.14). They are ack-eliciting, so
// a packet carrying only one of them must still be acknowledged (§13.2.1); the
// inherited nop handlers left such a packet unacknowledged, so the peer
// retransmitted it.
//
// The two data-blocked signals are also acted on. Treating them as purely
// informational is what let a lost credit grant deadlock a transfer outright, since
// the grants are never retransmitted and the next one only follows more consumption
// — see the note above regrantConnLimit in recvflow.go.
func (h *connFrameHandler) OnDataBlocked(limit uint64) error {
	h.ackEliciting = true
	h.c.regrantConnLimit(limit)
	return nil
}

// OnStreamsBlocked marks the packet ack-eliciting and rejects an out-of-range limit
// (RFC 9000 §19.14): the Maximum Streams field cannot exceed 2^60, as a larger value
// implies a stream ID past the 2^62-1 varint space — a FRAME_ENCODING_ERROR, the
// same bound MAX_STREAMS enforces (§19.11).
func (h *connFrameHandler) OnStreamsBlocked(_ bool, maximum uint64) error {
	h.ackEliciting = true
	if maximum > maxStreamsLimit {
		return ErrFrameEncoding
	}
	return nil
}

// OnStreamDataBlocked additionally validates the stream (RFC 9000 §19.13):
// STREAM_DATA_BLOCKED is sent by a stream's sender, so receiving one for a stream
// only WE send on — a unidirectional stream this endpoint initiated, where the
// peer has no send side — or for a locally initiated stream not yet created is a
// STREAM_STATE_ERROR.
func (h *connFrameHandler) OnStreamDataBlocked(id, limit uint64) error {
	h.ackEliciting = true
	if h.c.sendOnlyStream(id) || h.c.localStreamNotCreated(id) {
		return ErrStreamState
	}
	// A peer-uni stream past the advertised limit is a STREAM_LIMIT_ERROR (§4.6).
	if h.c.exceedsUniStreamLimit(id) {
		return ErrTooManyUniStreams
	}
	h.c.regrantStreamLimit(id, limit)
	return nil
}

// permitInSpace enforces RFC 9000 §12.4 (Table 3) / §12.5: a frame carried in a
// packet-number space that does not permit its type is a PROTOCOL_VIOLATION. Only
// PADDING, PING, ACK, CRYPTO, and a transport CONNECTION_CLOSE (0x1c) may appear in
// Initial and Handshake packets; the 1-RTT (application) space permits any frame.
// Without this gate a forged Initial — protected only with keys derived from the
// on-wire connection ID, so any observer can build one — could carry HANDSHAKE_DONE
// and drive the client to a spurious "handshake complete" with no 1-RTT keys, or
// inject STREAM/flow-control state before the handshake is authenticated.
func (h *connFrameHandler) permitInSpace(typ uint64) error {
	if h.space == spaceApp {
		return nil
	}
	switch typ {
	case FramePadding, FramePing, FrameACK, FrameACKECN, FrameCrypto, FrameConnectionClose:
		return nil
	default:
		if typ > FrameHandshakeDone {
			// An unknown frame type is a FRAME_ENCODING_ERROR regardless of space
			// (RFC 9000 §12.4), matching how parseFrameBody rejects it in 1-RTT.
			return ErrFrameEncoding
		}
		return ErrProtocolViolation // a known frame not permitted in this space (§12.5)
	}
}

// Stream-ID quadrants are relative to the endpoint reading them (RFC 9000 §2.1):
// the low bit names the initiator and the second bit names directionality, so
// which quadrant is "ours" flips between a client and a server Conn. Writing the
// client's answer as a literal — id&0x3 == 0x2 for send-only, 0x3 for
// receive-only — is correct exactly half the time, and the half it gets wrong is
// a server rejecting conformant peer behaviour with a connection error.
//
// The two predicates below own that question so the five frame handlers do not
// each answer it. OnStream already derived it role-aware; these are the same
// derivation, named.

// sendOnlyStream reports whether id names a unidirectional stream THIS endpoint
// initiated. We are its only sender, so a frame that only a sender may send —
// RESET_STREAM (§19.4), STREAM_DATA_BLOCKED (§19.13) — must never arrive for it.
func (c *Conn) sendOnlyStream(id uint64) bool {
	return id&0x2 != 0 && (id&0x1 == 1) == c.isServer
}

// recvOnlyStream reports whether id names a unidirectional stream the PEER
// initiated. We have no send side there, so a frame addressed to a sender —
// STOP_SENDING (§19.5), MAX_STREAM_DATA (§19.10) — must never arrive for it.
func (c *Conn) recvOnlyStream(id uint64) bool {
	return id&0x2 != 0 && (id&0x1 == 1) != c.isServer
}

// exceedsUniStreamLimit reports whether id names a peer-initiated unidirectional
// stream beyond the initial_max_streams_uni this endpoint advertised. Receiving
// any frame that references such an ID is a STREAM_LIMIT_ERROR (RFC 9000 §4.6):
// id>>2 is the stream's zero-based index among its type. acceptPeerUniStream
// applies the same bound when a STREAM frame opens the stream; this covers the
// other frames that can reference one.
func (c *Conn) exceedsUniStreamLimit(id uint64) bool {
	return c.recvOnlyStream(id) && id>>2 >= c.localMaxStreamsUni
}

// localStreamNotCreated reports whether id names a locally initiated (client)
// stream that has not yet been created — one at or above the next ID we would
// allocate for its type (RFC 9000 §3.2). A frame that references such a stream is a
// STREAM_STATE_ERROR (§19.5, §19.8, §19.10); an ID below the high-water mark was created
// (perhaps since closed) and must instead be ignored. Peer-initiated streams
// (server-uni/bidi) are never "not yet created" from our side and return false.
func (c *Conn) localStreamNotCreated(id uint64) bool {
	if (id&0x1 == 1) != c.isServer {
		return false // peer-initiated streams are never "not yet created" from our side
	}
	if id&0x2 == 2 { // our unidirectional stream
		return id >= c.uniStreamBase()+c.openedUni*4
	}
	return id >= c.nextBidiStreamID // our bidirectional stream
}

func (h *connFrameHandler) OnStream(id, offset uint64, fin bool, data []byte) error {
	h.ackEliciting = true
	ours := (id&0x1 == 1) == h.c.isServer // a stream this endpoint initiated
	uni := id&0x2 == 2
	// A STREAM frame on our own send-only (unidirectional) stream is a
	// STREAM_STATE_ERROR (RFC 9000 §19.8).
	if uni && ours {
		return ErrStreamState
	}
	s := h.c.streams[id]
	if s == nil {
		// An id we have no record of: accept a peer-initiated stream, or classify a
		// locally initiated one (RFC 9000 §2.1).
		switch {
		case !ours && uni: // peer-initiated unidirectional (control, QPACK) — accept it
			var err error
			if s, err = h.c.acceptPeerUniStream(id); err != nil {
				return err // STREAM_LIMIT_ERROR → CONNECTION_CLOSE
			}
		case !ours && !uni: // peer-initiated bidirectional
			if !h.c.isServer {
				return ErrServerBidiStream // a client never permits a server-initiated bidi stream
			}
			var err error
			if s, err = h.c.acceptPeerBidiStream(id); err != nil {
				return err // a request stream past our advertised limit → STREAM_LIMIT_ERROR
			}
		default: // ours && bidirectional: a locally initiated stream we have no open record of
			// A STREAM for a locally initiated stream not yet created is a
			// STREAM_STATE_ERROR (§19.8); one already created but since closed is ignored.
			if h.c.localStreamNotCreated(id) {
				return ErrStreamState
			}
			return nil
		}
	}
	if err := h.c.chargeRecv(s, offset+uint64(len(data))); err != nil {
		return err
	}
	if err := s.recv.receive(offset, data, fin); err != nil { // §4.5 final-size checks
		return err
	}
	h.c.signalReady(s) // response data / fin arrived — wake a blocked reader (§3.3)
	h.c.maybeRetire(s) // fully received + our FIN sent → drop from the routing map
	return nil
}

// chargeRecv applies RFC 9000 §4.1 receive flow control for a stream whose data
// now extends to end: the peer must not have sent past the per-stream or the
// connection receive limit we advertised, and any advance of the stream's highest
// received offset is charged to the connection receiver.
//
// It is shared by OnStream and OnResetStream, which arrive at end differently —
// one from offset+len(data), the other from a RESET_STREAM's final size — but
// account for it identically. Keeping one copy is what makes the connection
// counter and the two limits impossible to update in one path and not the other.
//
// connRecvMax == 0 is the disabled sentinel used by hand-built test connections,
// and it disables the per-stream bound as well, exactly as both call sites did
// when they carried their own copy.
//
// An end at or below recvHighest is not an error: on the STREAM path that is
// ordinary retransmission of bytes already accounted for, and on the reset path
// a final size equal to what was already received. Either way there is nothing
// new to charge.
func (c *Conn) chargeRecv(s *Stream, end uint64) error {
	if c.connRecvMax == 0 {
		return nil
	}
	if end > s.recvMax {
		return ErrFlowControl // per-stream limit exceeded
	}
	if end <= s.recvHighest {
		return nil
	}
	delta := end - s.recvHighest
	if c.connRecvTotal+delta > c.connRecvMax {
		return ErrFlowControl // connection limit exceeded
	}
	c.connRecvTotal += delta
	s.recvHighest = end
	return nil
}

// OnResetStream records that the peer abruptly ended its send side of a stream
// (RFC 9000 §3.5): the receive side is now finished (abnormally). Whatever was
// received contiguously before the reset remains readable.
func (h *connFrameHandler) OnResetStream(id, errCode, finalSize uint64) error {
	h.ackEliciting = true
	// A RESET_STREAM on a stream only WE send on — a unidirectional stream this
	// endpoint initiated — is a STREAM_STATE_ERROR (RFC 9000 §19.4).
	if h.c.sendOnlyStream(id) {
		return ErrStreamState
	}
	// A RESET_STREAM referencing a peer-uni stream past the advertised limit is a
	// STREAM_LIMIT_ERROR, even though we never opened it (RFC 9000 §4.6).
	if h.c.exceedsUniStreamLimit(id) {
		return ErrTooManyUniStreams
	}
	s := h.c.streams[id]
	if s == nil {
		return nil
	}
	if s.recv.complete() {
		return nil // the whole stream was already received; a later reset has no effect (§3.5)
	}
	// A final size already fixed by a received FIN cannot change (RFC 9000 §4.5): a
	// RESET_STREAM declaring a different final size is a FINAL_SIZE_ERROR. This is
	// independent of flow control, so it is checked before the FC-gated bounds.
	if s.recv.fin && finalSize != s.recv.finalSize {
		return ErrFinalSize
	}
	// The final size accounts for every byte the peer sent on the stream (RFC 9000
	// §4.5), so it may not fall below data already received. This one stays here
	// rather than moving into chargeRecv: it is a final-size rule, not a
	// flow-control one, and OnStream has no equivalent — an offset below the
	// highest received is ordinary retransmission there. It keeps the FC-enabled
	// gating it has always had, so a hand-built test connection is unaffected.
	if h.c.connRecvMax != 0 && finalSize < s.recvHighest {
		return ErrFinalSize
	}
	// The rest is the same §4.1 accounting OnStream does: bound the new end
	// against both limits and charge the increment to the connection receiver.
	if err := h.c.chargeRecv(s, finalSize); err != nil {
		return err
	}
	s.recvReset = true
	s.recvResetCode = errCode // surfaced to the application (RFC 9114 §8.1 request error)
	h.c.signalReady(s)        // reset arrived — wake a blocked reader (§3.3)
	h.c.maybeRetire(s)        // receive side terminal (reset) + our FIN sent → drop from the map
	return nil
}

// OnStopSending handles the peer asking us to stop sending on a stream (RFC 9000
// §3.5): the endpoint SHOULD respond by resetting its send side with the same
// application error code.
func (h *connFrameHandler) OnStopSending(id, errCode uint64) error {
	h.ackEliciting = true
	// A STOP_SENDING on a stream we only RECEIVE on — a unidirectional stream the
	// peer initiated — is a STREAM_STATE_ERROR (RFC 9000 §19.5): we have no send
	// side there, so there is nothing to ask us to stop.
	if h.c.recvOnlyStream(id) {
		return ErrStreamState
	}
	if s := h.c.streams[id]; s != nil {
		// resetLocked, not the public Reset: this handler runs under c.mu inside the
		// receive path, so re-locking through the public wrapper would self-deadlock.
		_ = s.resetLocked(errCode)
		// sendReset is now flipped: a parked sender's next Send returns
		// ErrStreamReset and must fall through to reading the response (§3.3, fix F4).
		h.c.signalReady(s)
		return nil
	}
	// A STOP_SENDING for a locally initiated stream not yet created is a
	// STREAM_STATE_ERROR (§19.5); one already created but since closed is ignored.
	if h.c.localStreamNotCreated(id) {
		return ErrStreamState
	}
	return nil
}

func (h *connFrameHandler) OnMaxData(maximum uint64) error {
	h.ackEliciting = true
	if maximum > h.c.connMax { // absolute ceiling; ignore non-increasing (§4.1)
		h.c.connMax = maximum
		// Connection-level send credit rose: wake every stream parked on blockConn
		// (§3.3). Also flag the burst so the end-of-burst cwnd sweep runs for any
		// blockCong waiter that the raised credit lets past the window.
		for _, s := range h.c.streams {
			h.c.signalReady(s)
		}
		h.c.sendWindowGrew = true
	}
	return nil
}

func (h *connFrameHandler) OnMaxStreamData(streamID, maximum uint64) error {
	h.ackEliciting = true
	// A MAX_STREAM_DATA for a stream we only RECEIVE on — a unidirectional stream
	// the peer initiated — is a STREAM_STATE_ERROR (RFC 9000 §19.10): we have no
	// send side there to credit.
	if h.c.recvOnlyStream(streamID) {
		return ErrStreamState
	}
	if s := h.c.streams[streamID]; s != nil {
		if maximum > s.sendMax {
			s.sendMax = maximum
			h.c.signalReady(s) // this stream's send credit rose — wake a blocked sender (§3.3)
		}
		return nil
	}
	// One for a locally initiated stream not yet created is likewise a
	// STREAM_STATE_ERROR (§19.10); one created and since closed is ignored.
	if h.c.localStreamNotCreated(streamID) {
		return ErrStreamState
	}
	return nil
}

// maxStreamsLimit is the largest legal MAX_STREAMS value: a larger cumulative
// stream count would imply a stream ID past the 2^62 varint space (RFC 9000
// §4.6, §19.11).
const maxStreamsLimit = uint64(1) << 60

// OnMaxStreams raises the cumulative number of streams the client may open,
// honoring the peer's MAX_STREAMS grant (RFC 9000 §4.6). A value above 2^60 is a
// FRAME_ENCODING_ERROR (§19.11); a value that does not increase the current
// limit is ignored. Without this the client would stay pinned to the peer's
// initial_max_streams limit for the life of the connection.
func (h *connFrameHandler) OnMaxStreams(uni bool, maximum uint64) error {
	h.ackEliciting = true
	if maximum > maxStreamsLimit {
		return ErrFrameEncoding
	}
	raised := false
	if uni {
		if maximum > h.c.peer.InitialMaxStreamsUni {
			h.c.peer.InitialMaxStreamsUni = maximum
			raised = true
		}
	} else if maximum > h.c.peer.InitialMaxStreamsBidi {
		h.c.peer.InitialMaxStreamsBidi = maximum
		raised = true
	}
	if raised {
		// Wake a caller blocked on the cumulative stream limit (§3.3; consumed in
		// PR 2d, where OpenStream becomes waitable on c.streamCredit).
		h.c.signalStreamCredit()
	}
	return nil
}

// maxPendingCtrl bounds the app-space control-frame buffer so a peer-driven
// PATH_RESPONSE echo (RFC 9000 §8.2.2) cannot grow it without limit: without the
// cap, a burst of PATH_CHALLENGE frames across a drain of many datagrams could
// inflate the buffer to hundreds of KB, and flush would then attempt to write a
// single oversized datagram and fail. A conformant peer sends one challenge, and
// validating the path needs only one matching response, so dropping the excess
// under a flood is safe. (Bounding the whole flushed datagram to the path MTU —
// the cap leaves headroom for a co-resident ACK, but the ACK itself is not yet
// size-bounded — is a separate concern tracked for a follow-up.)
const maxPendingCtrl = 900

// OnPathChallenge answers a PATH_CHALLENGE by echoing its 8 bytes in a
// PATH_RESPONSE (RFC 9000 §8.2.2): an endpoint MUST respond so the peer can
// validate the path — e.g. after a NAT rebinding on a long-lived connection. The
// response is queued on the self-healing app-space control path and sent by the
// next flush (within the same Poll cycle, so it is not unduly delayed, §8.2.2);
// it is not retransmitted, since a lost response simply prompts a new challenge.
func (h *connFrameHandler) OnPathChallenge(data *[8]byte) error {
	h.ackEliciting = true
	if len(h.c.pendingCtrl) < maxPendingCtrl {
		h.c.pendingCtrl = appendPathResponse(h.c.pendingCtrl, *data)
		h.c.pathRespPending = true // its datagram MUST be expanded to 1200 (§8.2.2)
	}
	return nil
}

// OnNewToken consumes a NEW_TOKEN (RFC 9000 §19.7). This client keeps no token
// store across connections, so the token is not retained, but the frame is
// ack-eliciting and must be acknowledged (§13.2.1); the nop handler would have left
// a packet carrying only it unacknowledged, prompting the peer to retransmit.
func (h *connFrameHandler) OnNewToken([]byte) error {
	h.ackEliciting = true
	return nil
}

// OnPathResponse consumes a PATH_RESPONSE (RFC 9000 §19.18). This client never
// sends a PATH_CHALLENGE, so it has no path validation to complete, but the frame
// is ack-eliciting and must be acknowledged (§13.2.1).
func (h *connFrameHandler) OnPathResponse(*[8]byte) error {
	h.ackEliciting = true
	return nil
}

func (h *connFrameHandler) OnHandshakeDone() error {
	h.c.handshakeComplete = true
	// HANDSHAKE_DONE confirms the handshake for a client (RFC 9001 §4.1.2), which
	// is the precondition for accepting a key update (§6.1) — distinct from TLS
	// completion, which fires earlier.
	h.c.handshakeConfirmed = true
	// The confirmed handshake discards Handshake keys (RFC 9001 §4.9.2), releasing
	// any of their still-unacknowledged bytes from the congestion controller.
	h.c.discardSpace(spaceHandshake)
	h.ackEliciting = true
	return nil
}

// OnConnectionClose enters the draining state on a received CONNECTION_CLOSE
// (RFC 9000 §10.2.2): the connection is marked closed so the send path emits
// nothing further, and the peer's error is recorded for Poll to surface.
func (h *connFrameHandler) OnConnectionClose(app bool, code, _ uint64, reason []byte) error {
	h.c.closed = true
	if h.c.peerClose == nil {
		h.c.peerClose = &PeerClosedError{App: app, Code: code, Reason: string(reason)}
	}
	return nil
}
