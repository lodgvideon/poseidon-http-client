package conn

import (
	"context"
	"errors"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
)

// Vectored DATA: send a body that lives in several buffers as the same frames a
// single joined buffer would produce, without joining it.
//
// The caller this exists for is gRPC, which puts a 5-byte length prefix in front
// of every message. Copying a 1 MiB message to prepend five bytes is a 1 MiB
// memmove and a 1 MiB buffer per call, and HTTP/2 cannot dodge it the way HTTP/3
// did in #347 by writing the header and the body as two writes: two SendData
// calls are two DATA frames, which is a different thing on the wire.
//
// So the split has to survive down to the frame writer, and the chunking has to
// cut ACROSS buffer boundaries — a 16 KiB frame boundary does not respect where
// one buffer ends and the next begins. That cursor is the whole substance here.
//
// Nothing about this reduces the number of writes. writev through a TLS
// connection was measured to be void in this repo's write-batching work, and the
// buffers still reach the transport one at a time. The saving is the copy.

// ErrVecUnderrun reports that the buffer cursor could not produce the number of
// bytes the flow controller had already granted credit for.
//
// It is unreachable by construction — the cursor's remaining count and the credit
// request are derived from the same sum — and it exists so that a future edit
// that breaks that correspondence fails loudly instead of emitting a DATA frame
// whose header Length disagrees with the bytes that follow it. That disagreement
// is not recoverable at the peer: it reads the declared count and then parses the
// middle of the payload as the next frame header.
var ErrVecUnderrun = errors.New("conn: vectored write produced fewer bytes than credited")

// dataVec walks a [][]byte as one logical byte stream, handing out each frame's
// pieces as sub-slices of the caller's buffers.
type dataVec struct {
	bufs [][]byte
	i    int // index of the buffer the cursor sits in
	off  int // offset within bufs[i]
	rem  int // bytes not yet handed out

	// single holds the scalar caller's buffer, and bufs is nil in that case.
	//
	// A separate field rather than a one-element array inside this struct: the
	// array form makes bufs point INTO the vec, and escape analysis then cannot
	// prove the segments handed to the frame writer do not alias it — so the vec
	// went to the heap and every DATA frame paid an allocation. Measured, and
	// caught by TestWriteData_DoesNotAllocate. Sub-slicing this field instead
	// yields a slice whose data pointer is the caller's buffer, which is what the
	// analyzer needs to see.
	single []byte
}

// newDataVec seats a cursor at the start of bufs.
func newDataVec(bufs [][]byte) (dataVec, error) {
	total := 0
	for _, b := range bufs {
		total += len(b)
		if total < 0 {
			return dataVec{}, frame.ErrFrameTooLarge
		}
	}
	return dataVec{bufs: bufs, rem: total}, nil
}

// seatSingle points a cursor at one contiguous buffer. bufs stays nil, which is
// what next branches on.
func (v *dataVec) seatSingle(p []byte) {
	v.single = p
	v.bufs = nil
	v.i, v.off, v.rem = 0, 0, len(p)
}

// next appends the pieces covering the next n bytes to dst and returns it with
// the byte count it actually covers. A short return means the cursor ran out
// early, which the caller must treat as fatal rather than write.
//
// Every step is bounded by i < len(bufs) rather than trusting rem, because this
// runs inside the section that holds the write lock, that lock is released
// explicitly rather than by defer, and an index panic there would strand it for
// the life of the process.
func (v *dataVec) next(n int, dst [][]byte) ([][]byte, int) {
	if v.bufs == nil { // scalar: one contiguous buffer, walked by offset
		take := n
		if take > v.rem {
			take = v.rem
		}
		if take > 0 {
			dst = append(dst, v.single[v.off:v.off+take])
			v.off += take
			v.rem -= take
		}
		return dst, take
	}
	got := 0
	for got < n && v.i < len(v.bufs) {
		b := v.bufs[v.i]
		if v.off >= len(b) { // empty buffer, or one just exhausted
			v.i++
			v.off = 0
			continue
		}
		take := len(b) - v.off
		if take > n-got {
			take = n - got
		}
		dst = append(dst, b[v.off:v.off+take])
		v.off += take
		got += take
	}
	v.rem -= got
	return dst, got
}

// SendDataV sends bufs as DATA for this lifetime, as one logical payload: the
// frames on the wire are exactly those SendData would produce for the same bytes
// joined together, and flow control charges the same total. It is the copy that
// is avoided, nothing else.
func (r StreamRef) SendDataV(ctx context.Context, bufs [][]byte, endStream bool) error {
	if r.s == nil {
		return ErrStaleStream
	}
	return r.s.sendDataV(ctx, r.gen, bufs, endStream)
}

// SendHeadersAndDataV is SendHeadersAndData with a vectored body — the shape
// every unary gRPC call takes, so it is the one that matters most.
func (r StreamRef) SendHeadersAndDataV(ctx context.Context, fields []header.Field, bufs [][]byte, endStream bool) error {
	if r.s == nil {
		return ErrStaleStream
	}
	return r.s.sendHeadersAndDataV(ctx, r.gen, fields, bufs, endStream)
}

// sendDataV mirrors sendData's door checks and end-of-stream bookkeeping.
func (s *Stream) sendDataV(ctx context.Context, wantGen uint64, bufs [][]byte, endStream bool) error {
	s.mu.Lock()
	if s.gen.Load() != wantGen {
		s.mu.Unlock()
		return ErrStaleStream
	}
	if s.pushed {
		s.mu.Unlock()
		return ErrPushedStreamReadOnly
	}
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()

	c, ok := s.w.(*Conn)
	if !ok {
		// A test double or another writer implementation: join and take the
		// ordinary path, so a vectored caller works against any writer.
		if err := s.w.writeData(ctx, s, wantGen, joinBufs(bufs), endStream); err != nil {
			return err
		}
	} else if err := c.writeDataV(ctx, s, wantGen, bufs, endStream); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
		if c != nil {
			c.markStreamDone(s.id)
		}
	}
	return nil
}

// sendHeadersAndDataV mirrors sendHeadersAndData.
func (s *Stream) sendHeadersAndDataV(ctx context.Context, wantGen uint64, fields []header.Field, bufs [][]byte, endStream bool) error {
	c, ok := s.w.(*Conn)
	if !ok {
		if err := s.sendHeadersWithPriority(ctx, wantGen, fields, false, nil); err != nil {
			return err
		}
		return s.sendDataV(ctx, wantGen, bufs, endStream)
	}

	s.mu.Lock()
	if s.gen.Load() != wantGen {
		s.mu.Unlock()
		return ErrStaleStream
	}
	if s.pushed {
		s.mu.Unlock()
		return ErrPushedStreamReadOnly
	}
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	for i := range fields {
		if string(fields[i].Name) == ":method" {
			s.reqIsHead = string(fields[i].Value) == "HEAD"
			break
		}
	}
	s.mu.Unlock()

	if err := c.writeHeadersAndDataV(ctx, s, wantGen, fields, bufs, endStream); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
		c.markStreamDone(s.id)
	}
	return nil
}

// joinBufs concatenates bufs. Only the non-*Conn fallback uses it — the whole
// point of this file is not to call it.
func joinBufs(bufs [][]byte) []byte {
	total := 0
	for _, b := range bufs {
		total += len(b)
	}
	if total == 0 {
		return nil
	}
	out := make([]byte, 0, total)
	for _, b := range bufs {
		out = append(out, b...)
	}
	return out
}

// emitDataV writes one DATA frame covering the next n bytes of v, under wmu.
// dst is the reusable segment scratch; the caller stores the returned slice back
// so its capacity survives, and this function clears the element pointers before
// returning so an idle connection does not pin the caller's message buffer.
func (c *Conn) emitDataV(v *dataVec, id uint32, n int, last bool, padLen uint8, dst [][]byte) ([][]byte, error) {
	segs, got := v.next(n, dst[:0])
	if got != n {
		return segs, ErrVecUnderrun
	}
	var err error
	if padLen > 0 {
		err = c.fr.WriteDataVPadded(id, last, segs, padLen)
	} else {
		err = c.fr.WriteDataV(id, last, segs)
	}
	// Reslicing to zero length keeps the element pointers alive, and this scratch
	// lives on the Conn: an idle pooled connection would hold the last message's
	// backing array for as long as it sits in the pool.
	for i := range segs {
		segs[i] = nil
	}
	if err != nil {
		return segs, err
	}
	c.bumpFramesSent()
	return segs, nil
}

// writeDataVec is the DATA send loop: chunk at the effective frame size, take
// credit for each chunk, emit it, flush. Both entry points are wrappers around
// it, so the invariants live in one place.
//
// That matters more here than deduplication usually does. This loop's history is
// invariant fixes — the stale-generation gate, the flush before parking, the
// padding debit — and while there were two copies each fix had to be replicated
// by hand into the other. A missed copy silently reinstates a race that was
// already shipped once.
//
// v is taken by pointer and never retained, so the caller's cursor stays on its
// stack; that is what lets writeData pass a single-buffer cursor without
// allocating a slice header array per frame.
func (c *Conn) writeDataVec(ctx context.Context, s *Stream, wantGen uint64, v *dataVec, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	if s.id == 0 {
		// SendHeaders has not run; the stream has no on-wire identity.
		return ErrStreamClosed
	}
	padLen := c.opts.Padding.ForData()
	padOverhead := 0
	if padLen > 0 {
		padOverhead = 1 + int(padLen)
	}
	maxFrame := c.maxOutFrameSize()
	effective := maxFrame
	if padOverhead > 0 && padOverhead < effective {
		effective -= padOverhead
	}
	if effective <= 0 {
		return ErrConnClosed
	}

	// An empty payload is the same special case writeData makes: a terminal
	// zero-length DATA carries no flow-controlled bytes and is sent unpadded, so
	// it needs no credit.
	if v.rem == 0 {
		if !endStream {
			return nil
		}
		c.wmu.Lock()
		defer c.wmu.Unlock()
		s.mu.Lock()
		staleEnd := s.gen.Load() != wantGen
		s.mu.Unlock()
		if staleEnd {
			return ErrStaleStream
		}
		if werr := c.fr.WriteData(s.id, true, nil); werr != nil {
			return werr
		}
		c.bumpFramesSent()
		return c.flushWrite()
	}

	for v.rem > 0 {
		// Flush anything left from the previous frame before parking: a frame
		// sitting in the buffer while we wait for the peer's WINDOW_UPDATE is a
		// deadlock, because the peer never sees what it would be granting for.
		c.wmu.Lock()
		if c.closed.Load() {
			c.wmu.Unlock()
			return ErrConnClosed
		}
		if ferr := c.flushWrite(); ferr != nil {
			c.wmu.Unlock()
			return ferr
		}
		c.wmu.Unlock()

		want := v.rem
		if want > effective {
			want = effective
		}
		n, cerr := c.acquireSendCredits(ctx, s, wantGen, want, padOverhead)
		if cerr != nil {
			return cerr
		}

		c.wmu.Lock()
		if c.closed.Load() {
			c.wmu.Unlock()
			return ErrConnClosed
		}
		s.mu.Lock()
		id, stale := s.id, s.gen.Load() != wantGen
		s.mu.Unlock()
		if stale {
			c.wmu.Unlock()
			return ErrStaleStream
		}
		// Decided BEFORE the cursor advances, exactly as the scalar loop decides it
		// from the pre-advance len(p): this is the last frame when the credit just
		// granted covers everything still uncovered.
		last := endStream && n == v.rem
		segs, werr := c.emitDataV(v, id, n, last, padLen, c.dvSegs)
		c.dvSegs = segs
		if werr != nil {
			c.wmu.Unlock()
			return werr
		}
		if ferr := c.flushWrite(); ferr != nil {
			c.wmu.Unlock()
			return ferr
		}
		c.wmu.Unlock()
	}
	return nil
}

// writeData sends p as DATA. It is writeDataVec over a single buffer.
func (c *Conn) writeData(ctx context.Context, s *Stream, wantGen uint64, p []byte, endStream bool) error {
	var v dataVec
	v.seatSingle(p)
	return c.writeDataVec(ctx, s, wantGen, &v, endStream)
}

// writeDataV sends the concatenation of bufs as DATA without joining them.
func (c *Conn) writeDataV(ctx context.Context, s *Stream, wantGen uint64, bufs [][]byte, endStream bool) error {
	v, err := newDataVec(bufs)
	if err != nil {
		return err
	}
	return c.writeDataVec(ctx, s, wantGen, &v, endStream)
}

// writeHeadersAndDataV is writeHeadersAndData with a vectored body. It keeps the
// group-commit handling exactly as the scalar form has it: wbatch.enter/leave
// around wmu and commitFrame at the end, NOT flushWrite. This path never parks
// after committing credit, so deferring its flush is safe — and replacing
// commitFrame with a bare flush would leave a writer parked in the batcher's
// condition variable with nothing left to wake it.
// writeHeadersAndDataV sends HEADERS and the concatenation of bufs as one write.
func (c *Conn) writeHeadersAndDataV(ctx context.Context, s *Stream, wantGen uint64, fields []header.Field, bufs [][]byte, endStream bool) error {
	v, err := newDataVec(bufs)
	if err != nil {
		return err
	}
	return c.writeHeadersAndDataVec(ctx, s, wantGen, fields, &v, endStream)
}

// writeHeadersAndDataVec is the fused one-shot: HEADERS and the whole body in a
// single write when the credit for all of it is available up front. Both fused
// entry points are wrappers around it, for the reason writeDataVec exists — the
// credit-commit boundary here is the one place in this file where a mistake is
// unrecoverable rather than stream-fatal, and it should exist once.
func (c *Conn) writeHeadersAndDataVec(ctx context.Context, s *Stream, wantGen uint64, fields []header.Field, v *dataVec, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	if v.rem == 0 {
		return c.writeHeadersWithPriority(ctx, s, fields, endStream, nil)
	}

	padLen := c.opts.Padding.ForData()
	padOverhead := 0
	if padLen > 0 {
		padOverhead = 1 + int(padLen)
	}

	c.wbatch.enter()
	c.wmu.Lock()
	c.wbatch.leave()
	c.assignStreamIDLocked(s, fields)

	effective := c.maxOutFrameSize()
	if padOverhead > 0 && padOverhead < effective {
		effective -= padOverhead
	}
	if effective <= 0 {
		c.wmu.Unlock()
		return c.slowHeadersThenDataVec(ctx, s, wantGen, fields, v, endStream)
	}
	frames := (v.rem + effective - 1) / effective
	ok, err := c.tryAcquireSendCreditsAll(s, wantGen, v.rem, padOverhead*frames)
	if err != nil {
		c.wmu.Unlock()
		return err
	}
	if !ok {
		c.wmu.Unlock()
		return c.slowHeadersThenDataVec(ctx, s, wantGen, fields, v, endStream)
	}

	// Committed: the credit is spent, and the connection-level half of it has no
	// refund path, so from here every path must write the bytes or fail the
	// stream. Nothing below may introduce a new way to bail out — which is why
	// the frame size is bounded by construction above rather than checked by the
	// frame writer after the debit.
	defer c.wmu.Unlock()

	buf := encBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	block := c.enc.EncodeBlock(*buf, fields)
	herr := c.writeHeaderBlock(s.id, block, false, nil)
	*buf = block[:0]
	encBufPool.Put(buf)
	if herr != nil {
		c.wbatch.wakeDeferredLocked()
		return herr
	}
	c.bumpFramesSent()

	s.mu.Lock()
	id, stale := s.id, s.gen.Load() != wantGen
	s.mu.Unlock()
	if stale {
		return ErrStaleStream
	}
	for v.rem > 0 {
		n := v.rem
		if n > effective {
			n = effective
		}
		last := endStream && n == v.rem
		segs, werr := c.emitDataV(v, id, n, last, padLen, c.dvSegs)
		c.dvSegs = segs
		if werr != nil {
			return werr
		}
	}
	return c.commitFrame()
}

// slowHeadersThenDataVec is the fallback when the one-shot cannot run: the
// ordinary two-write path. It is reached only BEFORE any credit is committed and
// before any byte is emitted, so the cursor is still seated at the start and can
// simply be walked by the normal send loop.
//
// One fallback rather than the two this file used to carry, for the same reason
// the loops merged: it sits on the path a credit-commit failure takes, and that
// path should not have two implementations.
func (c *Conn) slowHeadersThenDataVec(ctx context.Context, s *Stream, wantGen uint64, fields []header.Field, v *dataVec, endStream bool) error {
	if err := c.writeHeadersWithPriority(ctx, s, fields, false, nil); err != nil {
		return err
	}
	return c.writeDataVec(ctx, s, wantGen, v, endStream)
}
