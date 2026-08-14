package conn

import (
	"context"
	"errors"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
)

// Cross-stream send batching.
//
// SendHeadersAndDataV already puts one request's HEADERS and its whole body in
// one transport write. What it cannot do is put TWO requests there: every send
// takes the write lock, emits, flushes and releases, so N concurrent requests
// on one connection cost N writes — and on TLS, N records. At the concurrency a
// load generator runs at, that write is the single largest item in the profile:
// 44% of CPU in the measurements recorded in docs/H2_RAW_FRAMES_DESIGN.md §7a.
//
// SendBatch is the seam that removes it. It emits several streams' frames under
// one hold of the write lock and flushes once, so a batch of N requests is one
// write while its bytes fit in the write buffer (ConnOptions.WriteBufferSize).
//
// # Why this takes requests and not frames
//
// The design this comes from (issue #438) sketched a raw-frame port —
// `WriteFrames(ctx, []FrameChunks)`, where the caller owns the wire bytes the
// way ozontech/framer's request objects do. The R0 measurements in that same
// issue refuted its premise, and the connection's own bookkeeping refuses the
// rest of it:
//
//   - Caller-encoded HPACK blocks were the point of caller-owned bytes, and
//     they lose: a warm dynamic-table encode of a 7-field gRPC request set is 7
//     bytes and 444 ns, the cacheable stateless equivalent is 84 bytes. That is
//     +77 bytes on every request — 12x header inflation, ~15 MB/s at 200k rps —
//     to save 1.4% of CPU. A block encoded by a different encoder also desyncs
//     this connection's model of the peer's dynamic table, corrupting every
//     later request on it.
//   - A caller-chosen stream id does not advance c.nextID, so a later NewStream
//     reissues it — a §5.1.1 violation the peer answers with PROTOCOL_ERROR.
//   - A caller-chosen frame type escapes the connection's state: a raw
//     RST_STREAM leaks a MAX_CONCURRENT_STREAMS slot, a raw PING collides in
//     pingWaiters and steals a real Ping's ACK, a raw WINDOW_UPDATE desyncs the
//     recv window and the BDP tuner, a raw GOAWAY breaks §6.8's monotonic
//     last-stream-id clamp.
//
// What survives is the part that was measured to pay: coalescing. So the batch
// is a list of ordinary sends, the connection builds the frames, and the only
// thing that changes is how many times the result reaches the socket. Emitting
// deliberately non-conformant frames is a separate feature (R5 in the design
// doc) and is not this.

// BatchEntry is one stream's contribution to a SendBatch call: an optional
// header block followed by an optional body, emitted contiguously.
//
// A caller reuses one []BatchEntry across batches — nothing here is retained
// past the call, and SendBatch assigns Err on every entry, so a reused slice
// needs no clearing.
type BatchEntry struct {
	// Stream names the lifetime the frames belong to, obtained from NewStream.
	// A handle whose stream has since been recycled fails this entry with
	// ErrStaleStream and emits nothing.
	//
	// A StreamRef and not a *Stream for the reason StreamRef exists at all: the
	// structs are pooled, the receiver of a *Stream method IS the pooled struct,
	// and so nothing on a raw pointer can tell a live request from the next one
	// to claim the struct. A generator keeping its own stream table is exactly
	// the shape that produces such a pointer.
	Stream StreamRef

	// Fields, when non-nil, emits HEADERS (plus CONTINUATION when the encoded
	// block exceeds one frame) before the body. The stream's on-wire id is
	// assigned here, under the same write lock that emits it, so ids reach the
	// wire in the order they were allocated (RFC 9113 §5.1.1).
	//
	// Leave it nil to send body only on a stream whose HEADERS already went out.
	Fields []header.Field

	// Body is the DATA payload as one contiguous buffer.
	//
	// BodyV is the same payload already split across buffers — a gRPC length
	// prefix and the message it describes, say — sent as the frames the joined
	// bytes would produce, without joining them. When BodyV is non-nil it is the
	// body and Body is ignored.
	//
	// Neither is retained past the call.
	Body  []byte
	BodyV [][]byte

	// EndStream sets END_STREAM on the last frame this entry emits, half-closing
	// the stream. It is not set when Err is non-nil.
	EndStream bool

	// Err reports what happened to this entry and is assigned by every
	// SendBatch call, whatever the batch's own return value. nil means every
	// frame for this entry reached the write buffer and, if EndStream was set,
	// the stream is half-closed.
	//
	// ErrNoCredit is the one outcome that is not a failure: see its
	// documentation for what was emitted.
	Err error
}

// SendBatch emits several streams' HEADERS and DATA under one hold of the
// connection's write lock and flushes them once, so a batch of N requests costs
// one transport write instead of N. It is the cross-stream sibling of
// StreamRef.SendHeadersAndDataV.
//
// Entries are emitted in slice order, and stream ids are assigned in that same
// order, so ids reach the wire increasing (RFC 9113 §5.1.1).
//
// # It never waits
//
// Flow-control credit for an entry's body is taken without blocking. Blocking
// for credit under the write lock would stall every other stream on the
// connection, and a frame left sitting in the write buffer while its writer
// waits is a deadlock — the peer never sees the frame, so it never sends the
// WINDOW_UPDATE being waited for. An entry whose body the send windows cannot
// cover in full is therefore reported ErrNoCredit rather than waited on. ctx is
// consulted once, before any credit is spent — a connection-level debit has no
// refund path — and is never a park point.
//
// It does not defer, either. Group commit (ConnOptions.GroupCommit) batches by
// making a writer WAIT for the next one, which is why extending it to DATA cost
// p50 +81% (#360); SendBatch batches frames the caller has already handed over
// and waits for nothing. Its flush is immediate — and it still releases any
// writer deferring behind it.
//
// # Errors
//
// The returned error is batch-fatal: the connection is closed or was closed
// mid-batch, the context was already cancelled, or the transport write failed.
// Entries the batch had already emitted keep a nil Err and are half-closed, but
// on a transport failure nothing can say how much of the buffer reached the
// peer, so a caller redialling should resubmit the whole batch.
//
// Every entry's own outcome is in its Err field, assigned whether or not the
// batch itself failed. Four are the entry's problem alone and leave the rest of
// the batch to go out:
//
//   - ErrStaleStream — the handle names a finished request. Nothing emitted.
//   - ErrPushedStreamReadOnly — the stream is server-pushed, so receive-only.
//   - ErrStreamClosed — reset by the peer, already half-closed locally, or
//     body-only on a stream whose HEADERS have not been sent. Nothing emitted.
//   - ErrNoCredit — see below.
//
// # Sizing
//
// One write holds for as long as the batch's bytes fit in
// ConnOptions.WriteBufferSize (16 KiB by default); past that the buffered
// writer flushes as it fills and the batch costs ceil(bytes/buffer) writes,
// which is still fewer than one per entry. Raise WriteBufferSize to batch more.
func (c *Conn) SendBatch(ctx context.Context, batch []BatchEntry) error {
	if len(batch) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.closed.Load() {
		return ErrConnClosed
	}

	// enter/leave bracket the lock acquisition so a writer already holding wmu
	// can see this one queued and fold its frame into the flush below.
	c.wbatch.enter()
	c.wmu.Lock()
	c.wbatch.leave()
	if c.closed.Load() {
		c.wmu.Unlock()
		return ErrConnClosed
	}

	var v dataVec
	var fatal error
	for i := range batch {
		e := &batch[i]
		e.Err = c.sendBatchEntry(e, &v)
		if isBatchFatal(e.Err) {
			fatal = e.Err
			// The remaining entries never reached the wire, and saying so per
			// entry is what lets a caller resubmit exactly those.
			for j := i + 1; j < len(batch); j++ {
				batch[j].Err = fatal
			}
			break
		}
	}

	// flushNow, not commitFrame: commitFrame may defer into a group-commit
	// convoy, which is the waiting mechanism this API exists to avoid. flushNow
	// forces the wire AND does the convoy bookkeeping, so a writer parked behind
	// this batch is released by the flush that carried its bytes.
	ferr := c.flushNow()
	c.wmu.Unlock()
	if fatal == nil && ferr != nil {
		fatal = ferr
	}

	// Retire outside wmu: markStreamDone takes smu and may recycle the struct,
	// and the send paths this mirrors all do it after their write section.
	for i := range batch {
		if batch[i].Err == nil && batch[i].EndStream {
			batch[i].Stream.s.endLocalAndRetire()
		}
	}
	return fatal
}

// isBatchFatal reports whether an entry's failure killed the connection, in
// which case the entries after it cannot be emitted either. Everything else is
// this entry's own problem and the batch continues.
func isBatchFatal(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrStaleStream), errors.Is(err, ErrStreamClosed),
		errors.Is(err, ErrPushedStreamReadOnly), errors.Is(err, ErrNoCredit):
		return false
	case errors.Is(err, frame.ErrFrameTooLarge):
		// A body whose length overflows: a caller bug, but one caught before the
		// entry emitted anything, so it is this entry's problem and not the
		// batch's.
		return false
	default:
		return true
	}
}

// sendBatchEntry emits one entry's frames. MUST hold c.wmu.
//
// v is the caller's cursor, re-seated per entry so the whole batch shares one
// stack value — a fresh dataVec per entry would allocate, and the batch path
// has a zero-allocation gate.
func (c *Conn) sendBatchEntry(e *BatchEntry, v *dataVec) error {
	s := e.Stream.s
	if s == nil {
		return ErrStaleStream
	}
	wantGen := e.Stream.gen

	if e.BodyV != nil {
		nv, err := newDataVec(e.BodyV)
		if err != nil {
			return err
		}
		*v = nv
	} else {
		v.seatSingle(e.Body)
	}
	if err := gateBatchEntry(s, wantGen, e.Fields); err != nil {
		return err
	}

	padLen, padOverhead, effective := c.dataFraming()
	if effective <= 0 {
		return ErrConnClosed
	}

	// HEADERS first, and unconditionally: they carry no flow-controlled bytes,
	// and assigning an id without putting it on the wire would let a later
	// stream's HEADERS go out with a HIGHER id first — §5.1.1 requires a new id
	// to exceed every id already opened, so the skipped one could never be used
	// afterwards. Taking credit first and bailing would do exactly that, because
	// the per-stream send window does not exist until assignStreamIDLocked seeds
	// it.
	if e.Fields != nil {
		if err := c.emitBatchHeaders(s, e.Fields, e.EndStream && v.rem == 0); err != nil {
			return err
		}
	}

	// Re-read the id and the lifetime together, after the encode: the struct can
	// be recycled between the gate above and here, and a bare s.id read would
	// then name whatever request claimed it next. The retire pass in SendBatch is
	// what makes this load-bearing rather than defensive — reporting success on a
	// recycled struct would half-close a stranger's stream.
	s.mu.Lock()
	id, stale := s.id, s.gen.Load() != wantGen
	s.mu.Unlock()
	if stale {
		return ErrStaleStream
	}
	if id == 0 {
		// Body-only on a stream whose HEADERS never went out: it has no on-wire
		// identity to send DATA on.
		return ErrStreamClosed
	}

	if v.rem == 0 {
		if !e.EndStream || e.Fields != nil {
			return nil // nothing to send, or END_STREAM already rode the HEADERS
		}
		// A terminal zero-length DATA carries no flow-controlled bytes and is
		// sent unpadded, so it needs no credit.
		if err := c.fr.WriteData(id, true, nil); err != nil {
			return err
		}
		c.bumpFramesSent()
		return nil
	}
	return c.emitBatchBody(s, e, v, wantGen, id, effective, padOverhead, padLen)
}

// gateBatchEntry runs the door checks SendHeadersAndData makes, in one lock
// section, and captures the HEAD flag the receive side's Content-Length
// validator reads.
func gateBatchEntry(s *Stream, wantGen uint64, fields []header.Field) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.gen.Load() != wantGen:
		return ErrStaleStream
	case s.pushed:
		return ErrPushedStreamReadOnly
	case s.closed || s.localEnded:
		// s.closed is checked here and not left to the credit acquisition, which
		// also reports it: a peer RST_STREAM sets s.closed WITHOUT bumping s.gen
		// (markStreamDone only recycles a stream when !s.closed), so the
		// generation check cannot see it — and an entry that reached the credit
		// call would already have put its HEADERS on a stream §5.1 permits
		// nothing but PRIORITY on.
		return ErrStreamClosed
	}
	for i := range fields {
		if string(fields[i].Name) == ":method" {
			s.reqIsHead = string(fields[i].Value) == "HEAD"
			return nil
		}
	}
	return nil
}

// dataFraming samples the padding for one send and returns the largest DATA
// payload that fits a frame alongside it.
func (c *Conn) dataFraming() (padLen uint8, padOverhead, effective int) {
	padLen = c.opts.Padding.ForData()
	if padLen > 0 {
		padOverhead = 1 + int(padLen)
	}
	effective = c.maxOutFrameSize()
	if padOverhead > 0 && padOverhead < effective {
		effective -= padOverhead
	}
	return padLen, padOverhead, effective
}

// emitBatchHeaders assigns the stream's on-wire id and writes its header block.
// MUST hold c.wmu.
func (c *Conn) emitBatchHeaders(s *Stream, fields []header.Field, endStream bool) error {
	c.assignStreamIDLocked(s, fields)
	buf := encBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	block := c.enc.EncodeBlock(*buf, fields)
	err := c.writeHeaderBlock(s.id, block, endStream, nil)
	*buf = block[:0]
	encBufPool.Put(buf)
	if err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

// emitBatchBody takes the credit for the whole body and writes it. MUST hold
// c.wmu.
func (c *Conn) emitBatchBody(s *Stream, e *BatchEntry, v *dataVec, wantGen uint64,
	id uint32, effective, padOverhead int, padLen uint8,
) error {
	frames := (v.rem + effective - 1) / effective
	ok, err := c.tryAcquireSendCreditsAll(s, wantGen, v.rem, padOverhead*frames)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNoCredit
	}

	// Committed: the credit is spent and the connection-level half of it has no
	// refund path, so from here every path must write the bytes or fail the
	// connection. Nothing below may introduce a new way to bail out — the frame
	// size is bounded by construction above rather than checked after the debit.
	for v.rem > 0 {
		n := v.rem
		if n > effective {
			n = effective
		}
		last := e.EndStream && n == v.rem
		segs, werr := c.emitDataV(v, id, n, last, padLen, c.dvSegs)
		c.dvSegs = segs
		if werr != nil {
			return werr
		}
	}
	return nil
}
