package quic

import (
	"context"
	"errors"
	"time"
)

// Stream is a QUIC stream. For an HTTP/3 client the relevant streams are
// client-initiated bidirectional streams (one request/response exchange each).
type Stream struct {
	id   uint64
	conn *Conn
	recv recvStream

	sendOffset     uint64 // next byte offset to send = bytes already sent on this stream
	sendMax        uint64 // absolute per-stream send ceiling (§4.1); init = peer bidi_remote
	sdBlockedLimit uint64 // last sendMax a STREAM_DATA_BLOCKED was emitted for
	sdBlockedSet   bool   // whether a STREAM_DATA_BLOCKED has been emitted yet
	finSent        bool   // FIN latch (§4.5): the final size is fixed once set
	sendReset      bool   // RESET_STREAM sent — the send side is aborted (§3.2)
	recvMax        uint64 // per-stream receive limit we advertise; raised via MAX_STREAM_DATA
	recvHighest    uint64 // highest byte offset received on this stream (flow control, §4.1)
	recvReset      bool   // peer sent RESET_STREAM — the receive side is aborted (§3.5)
	recvResetCode  uint64 // the application error code carried by that RESET_STREAM (§19.4)

	// Stream wake vocabulary (docs/HTTP3_DESIGN.md §3.3), guarded by conn.mu.
	ready     chan struct{} // cap 1; reader→consumer progress signal for recv-readiness and send-unblock (never closed)
	sendBlock blockKind     // which flow-control limit last stalled a short Send; WaitSendable arms a pacing timer only for blockPace
}

// ID returns the stream's QUIC stream identifier.
func (s *Stream) ID() uint64 { return s.id }

// OpenStream opens the next client-initiated bidirectional stream (RFC 9000
// §2.1: IDs 0, 4, 8, … — the low two bits are zero). It returns
// ErrTooManyStreams if opening another stream would exceed the peer's
// advertised initial_max_streams_bidi limit (§4.6). It does not send anything
// until the caller writes to the stream.
func (c *Conn) OpenStream() (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.openStreamLocked()
}

// openStreamLocked is OpenStream's body. Assumes c.mu is held.
func (c *Conn) openStreamLocked() (*Stream, error) {
	if c.openedBidi >= c.peer.InitialMaxStreamsBidi {
		return nil, ErrTooManyStreams
	}
	id := c.nextBidiStreamID
	c.nextBidiStreamID += 4
	c.openedBidi++
	s := &Stream{id: id, conn: c, sendMax: c.peer.InitialMaxStreamDataBidiRemote, recvMax: DefaultStreamRecvWindow, ready: make(chan struct{}, 1)}
	if c.streams == nil {
		c.streams = map[uint64]*Stream{}
	}
	c.streams[id] = s
	// Baton-pass (docs/HTTP3_DESIGN.md §5, PR 2d): a single MAX_STREAMS grant of N
	// slots signals c.streamCredit only once (cap-1), so if credit still remains
	// after this open, wake the next OpenStreamContext parked on it. Chaining the
	// wake here lets one grant wake N waiters in turn; the chain stops on its own
	// once openStreamLocked returns ErrTooManyStreams (which signals nothing).
	if c.openedBidi < c.peer.InitialMaxStreamsBidi {
		c.signalStreamCredit()
	}
	return s, nil
}

// OpenStreamContext opens the next client-initiated bidirectional stream, blocking
// on stream credit rather than failing fast (docs/HTTP3_DESIGN.md §5, PR 2d). When
// the cumulative initial_max_streams_bidi limit is reached (RFC 9000 §4.6) it parks
// on c.streamCredit until the peer raises the limit with MAX_STREAMS
// (signalStreamCredit), ctx is cancelled, or the connection terminates, then
// retries. This is the documented backpressure a concurrent Do may see: it can
// block on stream credit until the peer grants more or ctx fires. A caller that
// prefers fail-fast uses OpenStream (which returns ErrTooManyStreams immediately).
func (c *Conn) OpenStreamContext(ctx context.Context) (*Stream, error) {
	for {
		c.mu.Lock()
		s, err := c.openStreamLocked()
		c.mu.Unlock()
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, ErrTooManyStreams) {
			return nil, err // a non-credit error is not resolved by waiting
		}
		if werr := c.waitStreamCredit(ctx); werr != nil {
			return nil, werr
		}
	}
}

// waitStreamCredit blocks until the peer raises the cumulative stream limit
// (signalStreamCredit from OnMaxStreams), ctx is cancelled, or the connection
// terminates (docs/HTTP3_DESIGN.md §3.3). Level-triggered: OpenStreamContext
// re-reads the limit under c.mu after waking, so the cap-1 c.streamCredit buffer
// plus that recheck close the check-then-block lost-wakeup window, exactly like
// WaitReadable. c.mu is never held across this wait.
func (c *Conn) waitStreamCredit(ctx context.Context) error {
	select {
	case <-c.streamCredit:
		return nil // recheck the limit under c.mu after waking
	case <-ctx.Done():
		return ctx.Err() // the request's own ctx — fail-fast backpressure escape
	case <-c.done:
		return c.closeErr // connection terminated; the latch published closeErr
	}
}

// uniStreamBase is the first unidirectional stream ID this endpoint opens
// (RFC 9000 §2.1): 2 for a client, 3 for a server. Later ones step by 4. The
// peer rejects a stream opened under the other role's IDs, so this must follow
// the connection's role.
func (c *Conn) uniStreamBase() uint64 {
	if c.isServer {
		return 3
	}
	return 2
}

// OpenUniStream opens the next unidirectional stream this endpoint initiates
// (RFC 9000 §2.1: IDs 2, 6, 10, … for a client; 3, 7, 11, … for a server) — the
// HTTP/3 control and QPACK streams, which are send-only from the opener. It
// returns ErrTooManyStreams if opening another would exceed the peer's advertised
// initial_max_streams_uni limit (§4.6).
func (c *Conn) OpenUniStream() (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.openUniStreamLocked()
}

// openUniStreamLocked is OpenUniStream's body. Assumes c.mu is held.
func (c *Conn) openUniStreamLocked() (*Stream, error) {
	if c.openedUni >= c.peer.InitialMaxStreamsUni {
		return nil, ErrTooManyStreams
	}
	id := c.uniStreamBase() + c.openedUni*4
	c.openedUni++
	s := &Stream{id: id, conn: c, sendMax: c.peer.InitialMaxStreamDataUni, recvMax: DefaultStreamRecvWindow, ready: make(chan struct{}, 1)}
	if c.streams == nil {
		c.streams = map[uint64]*Stream{}
	}
	c.streams[id] = s
	return s, nil
}

// Recv returns the stream bytes that have become contiguous since the previous
// Recv call (the newly available prefix of the received data), for reading a
// response. It returns an empty slice when nothing new is available. The result
// aliases the stream's buffer and should be consumed before the next Recv.
// Consuming bytes frees receive-flow-control window, which may queue
// MAX_STREAM_DATA / MAX_DATA to grant the peer more credit (RFC 9000 §4.1).
func (s *Stream) Recv() []byte {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.recvLocked()
}

// recvLocked is Recv's body. Assumes s.conn.mu is held. Consuming bytes queues the
// credit grants in pendingCtrl (onStreamConsumed); flushControl then emits them on
// this consuming goroutine before Recv releases c.mu, so a flow-control-blocked
// peer is unblocked without waiting for the parked reader's next flush
// (docs/HTTP3_DESIGN.md §4 INV-3).
func (s *Stream) recvLocked() []byte {
	data := s.recv.read()
	if len(data) > 0 {
		s.conn.onStreamConsumed(s, uint64(len(data)))
		_ = s.conn.flushControl() // grant our own receive credit now (INV-3)
	}
	return data
}

// Finished reports whether the receive side is complete — either the FIN has
// arrived with every byte contiguous (RFC 9000 §2.2), or the peer aborted the
// stream with RESET_STREAM (§3.5).
func (s *Stream) Finished() bool { return s.recvReset || s.recv.complete() }

// ResetReceived reports whether the peer aborted its send side with RESET_STREAM
// (RFC 9000 §3.5) — an abrupt end that, unlike a clean FIN, may fall in the
// middle of a frame. Callers distinguish it from a clean, complete receive.
func (s *Stream) ResetReceived() bool { return s.recvReset }

// ResetCode returns the application error code the peer carried in its
// RESET_STREAM (RFC 9000 §19.4); it is meaningful only when ResetReceived is
// true. An HTTP/3 caller maps it to a request-level error (RFC 9114 §8.1).
func (s *Stream) ResetCode() uint64 { return s.recvResetCode }

// maybeRetire drops a stream from the routing map once both directions have
// reached a terminal state — the send side finished (FIN sent) or reset, and the
// receive side finished (fully received) or reset — so a long-lived connection
// carrying many aborted or completed requests does not accumulate dead streams.
// A stream reset before its FIN is retired too: Reset first scrubs the stream's
// STREAM data from the retransmit sources (scrubResetStreamData), so a retired
// stream is never one whose data the retransmitter would resend (§13.3). Its
// received bytes stay counted in connection flow control. The map only routes
// inbound frames; the caller's *Stream and its buffered data are unaffected, and
// a late retransmit for the id is ignored as a stream we never opened.
func (c *Conn) maybeRetire(s *Stream) {
	sendDone := s.finSent || s.sendReset
	recvDone := s.recvReset || s.recv.complete()
	if sendDone && recvDone {
		delete(c.streams, s.id)
	}
}

// scrubResetStreamData drops every STREAM frame of a reset stream from the
// retransmit sources — the pending retransmit queue and the frames of packets
// still in flight — so the stream's data is never resent (RFC 9000 §13.3), even
// once the stream is dropped from the routing map and the flush-time suppression
// check can no longer find it. The RESET_STREAM frame itself is a different kind
// and is left in place, so it still retransmits until acknowledged.
func (c *Conn) scrubResetStreamData(id uint64) {
	q := c.retransQueue[spaceApp][:0]
	for _, rf := range c.retransQueue[spaceApp] {
		if rf.kind == retransStream && rf.streamID == id {
			continue
		}
		q = append(q, rf)
	}
	c.retransQueue[spaceApp] = q
	for pn, p := range c.sent[spaceApp].packets {
		var kept []retransFrame
		changed := false
		for _, rf := range p.frames {
			if rf.kind == retransStream && rf.streamID == id {
				changed = true
				continue
			}
			kept = append(kept, rf)
		}
		if changed {
			p.frames = kept
			c.sent[spaceApp].packets[pn] = p
		}
	}
}

// Reset abruptly terminates the send side of the stream with an application error
// code, sending a RESET_STREAM frame (RFC 9000 §3.2, §19.4). No further data may
// be sent (Send returns ErrStreamReset). It is idempotent and a no-op once the
// stream's FIN has been sent — the peer already has the whole request.
func (s *Stream) Reset(errCode uint64) error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.resetLocked(errCode)
}

// resetLocked is Reset's body. Assumes s.conn.mu is held. It is the
// reentrancy-safe target for OnStopSending, which resets a stream while already
// holding c.mu inside the receive path.
func (s *Stream) resetLocked(errCode uint64) error {
	if s.finSent || s.sendReset {
		return nil
	}
	if s.conn.oneRTTSealer == nil {
		return ErrNotEstablished
	}
	s.sendReset = true
	// The send side is aborted: this stream's buffered STREAM data must never be
	// resent (§13.3), so drop it from the retransmit sources before it can be
	// retired below.
	s.conn.scrubResetStreamData(s.id)
	// finalSize is the number of bytes already sent (§19.4). Retransmitted until
	// acknowledged (§13.3) via the retrans descriptor.
	rf := retransFrame{kind: retransReset, streamID: s.id, errCode: errCode, offset: s.sendOffset}
	if err := s.conn.writeAppFrames(AppendResetStream(nil, s.id, errCode, s.sendOffset), []retransFrame{rf}); err != nil {
		return err
	}
	s.conn.maybeRetire(s) // if the receive side is already terminal, both are now done
	return nil
}

// StopSending requests that the peer abort the send side of the stream (its data
// flow toward us) with an application error code, sending a STOP_SENDING frame
// (RFC 9000 §3.5, §19.5). Used to abandon a response the caller no longer wants,
// so the peer stops filling our receive buffer. Idempotent.
func (s *Stream) StopSending(errCode uint64) error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.stopSendingLocked(errCode)
}

// stopSendingLocked is StopSending's body. Assumes s.conn.mu is held. Added for
// symmetry with the other …Locked internals so a future receive-path caller can
// stop a stream's receive side without re-locking.
func (s *Stream) stopSendingLocked(errCode uint64) error {
	if s.recvReset {
		return nil
	}
	if s.conn.oneRTTSealer == nil {
		return ErrNotEstablished
	}
	s.recvReset = true
	s.conn.maybeRetire(s) // receive side terminal; if our FIN is also sent, drop from the map
	// Retransmitted until acknowledged (§13.3): it has no self-healing successor.
	rf := retransFrame{kind: retransStopSending, streamID: s.id, errCode: errCode}
	return s.conn.writeAppFrames(AppendStopSending(nil, s.id, errCode), []retransFrame{rf})
}

// streamChunk is a run of received stream bytes buffered until the bytes before
// it arrive.
type streamChunk struct {
	offset uint64
	data   []byte
}

// recvStream reassembles the bytes of one stream's receive direction from
// STREAM frames that may arrive out of order or overlapping (RFC 9000 §2.2). It
// keeps the not-yet-read contiguous run in data and buffers later chunks until
// the gap fills.
//
// data is a sliding window, not the whole stream: read releases the bytes it
// returns, so the buffer holds only what the peer has sent but the application
// has not yet consumed — bounded by the receive-flow-control window
// (DefaultStreamRecvWindow), not by the stream's total length. Retaining the
// whole stream instead was a real leak: receive flow control slides its limit
// forward as the application consumes (recvflow.go), so it bounds bytes
// in flight and never bounds bytes retained.
//
// base makes that release possible. Every offset in a STREAM frame is absolute,
// so the contiguous prefix's extent is base+len(data), never len(data) alone;
// there is no other place the consumed byte count is recorded.
type recvStream struct {
	data      []byte        // contiguous unread bytes, starting at absolute offset base
	pending   []streamChunk // buffered chunks beyond base+len(data), sorted by offset
	base      uint64        // absolute stream offset of data[0] = bytes already returned by read
	highest   uint64        // highest byte offset received (offset+len), for §4.5 checks
	fin       bool
	finalSize uint64
}

// receive incorporates a STREAM frame's data at the given stream offset. fin
// marks that this frame carries the last byte (the final size is offset+len). It
// returns ErrFinalSize if the frame violates the stream's final size (RFC 9000
// §4.5): data past a declared final size, a conflicting FIN, or a FIN below data
// already received.
func (r *recvStream) receive(offset uint64, data []byte, fin bool) error {
	end := offset + uint64(len(data))
	if r.fin && (end > r.finalSize || (fin && end != r.finalSize)) {
		return ErrFinalSize // beyond, or inconsistent with, a fixed final size
	}
	if fin {
		if end < r.highest {
			return ErrFinalSize // a FIN below data already received
		}
		r.fin, r.finalSize = true, end
	}
	if end > r.highest {
		r.highest = end
	}
	r.insert(offset, data)
	// Cap the number of retained out-of-order ranges. A peer that sends many
	// tiny, non-adjacent STREAM frames (e.g. one byte at every other offset)
	// forces one buffered chunk per gap. The receive-flow-control window bounds
	// the BYTES buffered but not the COUNT of ranges, and bufferGap is O(ranges)
	// per frame — so an unbounded range count is a quadratic-CPU denial of
	// service. RFC 9000 §11.1 lets an endpoint treat resource-exhausting
	// behaviour as a connection error; past the cap we stop reassembling and
	// close with PROTOCOL_VIOLATION instead. The bound is far above what real
	// packet reordering produces (adjacent ranges fuse in bufferGap and never
	// count).
	if len(r.pending) > maxRecvGapChunks {
		return ErrProtocolViolation
	}
	return nil
}

// maxRecvGapChunks bounds the out-of-order STREAM ranges a single stream will
// hold before its reassembly is treated as a denial of service (see receive).
// 512 is orders of magnitude above the handful of ranges genuine packet
// reordering leaves outstanding, while keeping the per-frame O(ranges) merge and
// the total retained state bounded by a small constant.
const maxRecvGapChunks = 512

func (r *recvStream) insert(offset uint64, data []byte) {
	have := r.base + uint64(len(r.data)) // absolute end of the contiguous prefix
	if offset+uint64(len(data)) <= have {
		return // wholly duplicate
	}
	if offset <= have {
		r.data = append(r.data, data[have-offset:]...)
		r.absorb()
		return
	}
	r.bufferGap(offset, data)
}

// bufferGap stores [offset, offset+len(data)) among the out-of-order chunks,
// merging it with any chunk it overlaps or abuts so pending stays sorted by
// offset and non-overlapping — one chunk per gap, no byte buffered twice. A peer
// that resends the same gapped frame, or overlapping gapped frames, therefore
// cannot inflate pending past the receive-flow-control window: without this a
// duplicate append (recvHighest does not advance, so flow control never rejects
// it) grew the buffer without bound. Overlapping input yields to bytes already
// held, which a conformant peer guarantees are identical (RFC 9000 §2.2). The
// merge is a single ordered pass, replacing the previous append-then-resort
// (O(n log n) per frame, quadratic against a peer sending many tiny gaps).
func (r *recvStream) bufferGap(offset uint64, data []byte) {
	lo, hi := offset, offset+uint64(len(data))
	out := make([]streamChunk, 0, len(r.pending)+1)
	placed := false
	for _, c := range r.pending {
		cLo, cHi := c.offset, c.offset+uint64(len(c.data))
		if cHi < lo || cLo > hi { // disjoint from the (possibly grown) new interval
			if cLo > hi && !placed { // first chunk beyond the new one: emit it in order
				out = append(out, streamChunk{lo, append([]byte(nil), data...)})
				placed = true
			}
			out = append(out, c)
			continue
		}
		// Overlap or adjacency: fuse c into the new interval, keeping already-held
		// bytes on any overlap.
		nLo, nHi := min(lo, cLo), max(hi, cHi)
		buf := make([]byte, nHi-nLo)
		copy(buf[lo-nLo:], data)    // new bytes first
		copy(buf[cLo-nLo:], c.data) // existing bytes win on overlap
		lo, hi, data = nLo, nHi, buf
	}
	if !placed {
		out = append(out, streamChunk{lo, append([]byte(nil), data...)})
	}
	r.pending = out
}

// absorb folds any buffered chunks that are now contiguous into data.
func (r *recvStream) absorb() {
	for len(r.pending) > 0 {
		c := r.pending[0]
		have := r.base + uint64(len(r.data)) // absolute end of the contiguous prefix
		if c.offset+uint64(len(c.data)) <= have {
			r.pending = r.pending[1:] // duplicate
			continue
		}
		if c.offset > have {
			break // still a gap
		}
		r.data = append(r.data, c.data[have-c.offset:]...)
		r.pending = r.pending[1:]
	}
}

// bytes returns the contiguous received run that read has not yet returned
// (absolute offsets [base, base+len(data))). Once read has released a prefix,
// that prefix is gone: this is the unread remainder, not the stream from 0.
func (r *recvStream) bytes() []byte { return r.data }

// read returns the contiguous bytes not yet returned by a prior read and
// releases them from the buffer, so a consumed stream retains nothing.
//
// The returned slice aliases the buffer — Stream.Recv's contract is that the
// result stays valid until the next Recv — so the release must re-slice PAST the
// returned bytes, never truncate back over them. r.data keeps the same backing
// array positioned at the array index just after d, so a later append writes
// beyond d and cannot clobber it; truncating to r.data[:0] would instead point
// the next append straight at d[0]. Re-slicing does leave d's array alive while
// the tail is reused, but the array is at most a window's worth of appends, and
// append reallocates a fresh (small) one once that tail is spent, which is what
// keeps retention O(window) rather than O(bytes ever received).
func (r *recvStream) read() []byte {
	d := r.data
	r.base += uint64(len(d))
	r.data = r.data[len(d):] // empty, positioned after d — never r.data[:0]
	return d
}

// complete reports whether the whole stream has been received: the FIN has
// arrived and every byte up to the final size is contiguous. The contiguous
// prefix ends at base+len(data): bytes already read still count toward the final
// size even though they are no longer buffered.
func (r *recvStream) complete() bool {
	return r.fin && r.base+uint64(len(r.data)) == r.finalSize && len(r.pending) == 0
}

// WaitReadable blocks until the stream may have more response data to read, its
// context is cancelled, or the connection terminates (docs/HTTP3_DESIGN.md §3.3).
// It is level-triggered: a woken caller re-reads its predicate (Recv / RecvState)
// under conn.mu before deciding to block again, so the cap-1 s.ready buffer plus
// that recheck close the check-then-block lost-wakeup window. Added in PR 2b but
// not yet driven by Do (the request path still inline-polls).
func (s *Stream) WaitReadable(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil // recheck the predicate under conn.mu after waking
	case <-ctx.Done():
		return ctx.Err() // per-request cancel — only this caller
	case <-s.conn.done:
		return s.conn.closeErr // connection terminated; the latch published closeErr before closing done
	}
}

// WaitSendable blocks until the stream may admit more send data, its context is
// cancelled, or the connection terminates (docs/HTTP3_DESIGN.md §3.3). It owns
// the pacing timer: blockCong/blockStream/blockConn all resolve to an
// inbound-event wake via s.ready, but blockPace refills on wall-clock time with
// no inbound event, so only that case arms a time.After(pacingRefillDelay). The
// sendBlock read and the timer are taken under conn.mu, released before the
// select, so the lock is never held across the wait. Added in PR 2b but not yet
// driven by Do.
func (s *Stream) WaitSendable(ctx context.Context) error {
	s.conn.mu.Lock()
	var timer <-chan time.Time
	if s.sendBlock == blockPace {
		timer = time.After(s.conn.pacingRefillDelay())
	}
	s.conn.mu.Unlock()
	select {
	case <-s.ready:
		return nil
	case <-timer:
		return nil // the pacing bucket has refilled; retry Send
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.done:
		return s.conn.closeErr
	}
}

// RecvState returns a locked snapshot of the receive side (docs/HTTP3_DESIGN.md
// §3.3, fix F5): finished mirrors Finished (a clean complete FIN or a peer
// RESET_STREAM), reset mirrors ResetReceived, and code is the reset's application
// error code. The reader mutates recv.fin/finalSize/data and recvReset under
// conn.mu in OnStream/OnResetStream, so a consumer must read them under the same
// lock rather than via the individual lock-free accessors. Added in PR 2b for the
// future Do read loop; not yet called by Do.
func (s *Stream) RecvState() (finished, reset bool, code uint64) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.recvReset || s.recv.complete(), s.recvReset, s.recvResetCode
}
