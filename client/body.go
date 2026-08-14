package client

import (
	"context"
	"io"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// responseBodyReader streams response DATA frames as an io.ReadCloser.
// Constructed by do() when Request.BodyMode is BodyStream; ownership
// transfers to Response.BodyReader.
type responseBodyReader struct {
	ctx     context.Context
	cancel  context.CancelFunc // cancels ctx in Close, waking a parked Read
	stream  respStream
	release releaser  // returns conn to pool; called exactly once in Close
	resp    *Response // written with trailers when EventTrailers arrives

	// mu guards every field below it. This is an io.ReadCloser handed to the
	// caller, and the convention that type carries — net/http's resp.Body — is
	// that Close from another goroutine is how you abort a slow read. Without
	// this, Close recycled the pooled DATA slab while an in-flight Read still
	// aliased it through buf, so a later request could be handed the same slab
	// and overwrite bytes this caller was about to copy out.
	//
	// mu is never held across stream.Recv or stream.Close: those block, and
	// holding it there would make the abort wait for the read it is aborting.
	mu      sync.Mutex
	buf     []byte  // unconsumed tail of last DATA event (aliases curData)
	curData *[]byte // pooled buffer backing buf/last event; recycled on next event/Close
	done    bool    // no more events coming: END_STREAM, trailers, or reset
	closed  bool    // Close ran; buf and curData are no longer ours to read

	closeOnce sync.Once
	lg        *leakGuard // reports a leak if GC'd without Close (debug builds only)
}

// recycleDataLocked is recycleData with mu held. It clears buf as well as
// curData: buf aliases the slab, so leaving it set would let a later Read copy
// out of a buffer another request already owns.
func (r *responseBodyReader) recycleDataLocked() {
	if r.curData != nil {
		putDataSlab(r.curData)
		r.curData = nil
	}
	r.buf = nil
}

// Read implements io.Reader. Blocks on stream.Recv until DATA arrives,
// fills p, and saves any surplus in r.buf for the next call. Returns
// io.EOF when END_STREAM or EventTrailers is observed.
func (r *responseBodyReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	// closed, not done, gates the buf fast path below. They are different
	// states: done means no more EVENTS are coming, and a surplus tail may
	// still be sitting in buf waiting to be drained — the ordinary
	// END_STREAM-with-remainder case. closed means Close ran, so buf aliases a
	// slab that has gone back to the pool and must not be read at all.
	if r.closed {
		r.mu.Unlock()
		return 0, io.EOF
	}
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		r.mu.Unlock()
		return n, nil
	}
	if r.done {
		r.mu.Unlock()
		return 0, io.EOF
	}
	r.mu.Unlock()
	for {
		ev, err := r.stream.Recv(r.ctx)
		if err != nil {
			return 0, err
		}
		switch ev.Type {
		case conn.EventData:
			r.mu.Lock()
			// The previous frame's surplus was fully consumed before we
			// reached this Recv, so its pooled buffer is safe to recycle.
			r.recycleDataLocked()
			if r.closed {
				// Close landed while we were parked in Recv. The stream is gone
				// and release() has run; hand this frame's slab straight back
				// rather than storing it on a reader nobody will drain.
				putDataSlab(ev.DataSlab)
				r.mu.Unlock()
				return 0, io.EOF
			}
			r.curData = ev.DataSlab
			n := copy(p, ev.Data)
			if n < len(ev.Data) {
				r.buf = ev.Data[n:] // aliases r.curData until consumed
			}
			eof := false
			if ev.EndStream {
				r.done = true
				eof = n == len(ev.Data)
			}
			r.mu.Unlock()
			if eof {
				return n, io.EOF
			}
			return n, nil
		case conn.EventTrailers:
			r.mu.Lock()
			r.recycleDataLocked()
			if r.resp != nil {
				r.resp.Trailers = append(r.resp.Trailers[:0], ev.Headers...)
			}
			r.done = true
			r.mu.Unlock()
			return 0, io.EOF
		case conn.EventReset:
			r.mu.Lock()
			r.recycleDataLocked()
			r.done = true
			r.mu.Unlock()
			return 0, &StreamResetError{Code: ev.RSTCode}
		case conn.EventInterimHeaders:
			// Informational 1xx (RFC 7540 §8.1) — no body, stream continues.
			// Do() consumed the final HEADERS before handing this reader over,
			// so a 1xx can only appear here on a peer that interleaves them
			// after the final status; skip it either way.
			ev.Release()
			continue
		case conn.EventHeaders:
			ev.Release()
			continue // spurious mid-stream HEADERS; skip
		case conn.EventPushPromise:
			// A PUSH_PROMISE arrives on the parent stream, which is the one this
			// reader is draining. Client.Do's buffered path dispatches it to the
			// push handler; a body reader has no handler to dispatch to, so the
			// promise is dropped — but its header block still has to go back, the
			// same as the two skip arms above.
			ev.Release()
			continue
		}
	}
}

// Close releases the stream and returns the conn to the pool. Sends
// RST_STREAM(CANCEL) when the body has not been fully drained.
// Idempotent.
func (r *responseBodyReader) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.lg.disarm()
		// Both outside mu on purpose: they release a Read parked in Recv, and
		// holding the lock across them would make the abort wait for the very
		// read it is aborting.
		//
		// The cancel is what actually wakes that Read. stream.Close tears the
		// stream down but does not signal a Recv already blocked on the event
		// channel, so without this an abort through the io.ReadCloser left the
		// reader hanging until the caller's own deadline fired — Close returned
		// promptly and the goroutine behind it did not.
		if r.cancel != nil {
			r.cancel()
		}
		err = r.stream.Close()
		r.mu.Lock()
		r.closed = true // later Reads return EOF instead of touching a freed slab
		// Safe to recycle here: mu serialises this against every point where a
		// Read touches curData or buf, and a Read parked in Recv holds no alias
		// (the buf fast path returns before it reaches Recv). A Read that wakes
		// afterwards sees closed and hands its own slab straight back.
		r.recycleDataLocked()
		r.mu.Unlock()
		r.release.release()
	})
	return err
}
