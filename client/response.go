package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Response is the synchronous result of Client.Do.
//
// Headers, Body, and Trailers backing bytes are valid until Reset() is
// called; do not retain slices past that point. Callers should allocate
// one Response per goroutine and reuse it across Do calls:
//
//	var resp client.Response
//	for {
//	    resp.Reset()
//	    if err := c.Do(ctx, req, &resp); err != nil { ... }
//	    use(resp.Status, resp.Headers)
//	}
type Response struct {
	// Status is the integer value parsed from the :status pseudo-header.
	Status int
	// Headers is the regular response header fields (no pseudo-headers).
	Headers []conn.HeaderField
	// Body is nil unless Request.BodyMode is BodyBuffer.
	Body []byte
	// Trailers is nil unless Request.WantTrailers is true and the peer
	// sent a trailers frame.
	Trailers []conn.HeaderField
	// BytesReceived is the total DATA payload received, even when
	// Request.BodyMode was BodyDiscard.
	BytesReceived int64

	// BodyReader is non-nil when the request had BodyMode=BodyStream.
	// Caller reads body bytes then calls Close(). Trailers (if any) are
	// written into Response.Trailers just before Close returns io.EOF.
	// Reset() calls Close() automatically when BodyReader is non-nil.
	//
	// Close may be called from another goroutine while a Read is in flight —
	// the io.ReadCloser convention for aborting a slow read, as with
	// net/http's Response.Body. It releases the blocked Read promptly, and
	// every Read after it returns io.EOF.
	BodyReader io.ReadCloser

	// blocks holds the pooled header blocks backing Headers and Trailers —
	// both the field slices and the bytes their Names and Values point into.
	// Released on Reset().
	blocks []*conn.HeaderBlock
}

// Reset clears all exported fields for reuse, retaining backing arrays.
// Any references to Headers[i].Name / .Value / Body / Trailers bytes
// must not be used after Reset returns.
//
// On the first call after a zero-value Response, the first Reset preallocates
// Headers and blocks backing arrays (cap=8 / cap=2) so subsequent appends in
// parseStatus and drainResponse do not allocate.
func (r *Response) Reset() {
	if r.BodyReader != nil {
		_ = r.BodyReader.Close()
		r.BodyReader = nil
	}
	for _, b := range r.blocks {
		b.Release()
	}
	r.blocks = r.blocks[:0]
	r.Status = 0
	if r.Headers == nil {
		r.Headers = make([]conn.HeaderField, 0, 8)
	} else {
		r.Headers = r.Headers[:0]
	}
	if r.Trailers == nil {
		r.Trailers = make([]conn.HeaderField, 0, 2)
	} else {
		r.Trailers = r.Trailers[:0]
	}
	if cap(r.blocks) == 0 {
		r.blocks = make([]*conn.HeaderBlock, 0, 2)
	}
	r.Body = r.Body[:0]
	r.BytesReceived = 0
}

// EventType discriminates StreamEvent variants returned from
// StreamResponse.Recv.
type EventType uint8

// EventType values.
const (
	// EventNone is the zero value of EventType. Recv never delivers it; it
	// names the uninitialized/zero StreamEvent so callers can compare against
	// a constant instead of a bare 0 (and a switch can default safely).
	EventNone EventType = iota // 0
	// EventData carries a chunk of DATA payload in StreamEvent.Data (valid only
	// until the next Recv/Close; see StreamEvent — copy to retain).
	EventData // 1
	// EventTrailers carries response trailers in StreamEvent.Trailers.
	EventTrailers // 2
	// EventReset signals that the peer sent RST_STREAM; the code is
	// in StreamEvent.ResetCode and EndStream is always true.
	EventReset // 3
)

// String returns the lowercase event-type name.
func (t EventType) String() string {
	switch t {
	case EventNone:
		return "none"
	case EventData:
		return "data"
	case EventTrailers:
		return "trailers"
	case EventReset:
		return "reset"
	default:
		return "unknown"
	}
}

// StreamEvent is one chunk of a streaming response.
//
// Data aliases a pooled connection-layer buffer that is recycled on the next
// Recv or Close; Trailers alias the response's header block, valid until
// Close. Copy these slices if you need to retain the bytes past then — do NOT
// hold them across a Recv/Close.
type StreamEvent struct {
	// Type discriminates which other fields are populated.
	Type EventType
	// Data is the DATA payload for EventData. It aliases a pooled buffer that is
	// recycled on the next Recv/Close; copy it to retain the bytes past then.
	Data []byte
	// Trailers is populated for EventTrailers; aliases header-block memory that
	// is valid until Close.
	Trailers []conn.HeaderField
	// ResetCode is populated for EventReset.
	ResetCode conn.ErrCode
	// EndStream is true on the final event of a stream.
	EndStream bool
}

// StreamResponse is returned by Client.DoStream after the initial
// HEADERS frame arrives. The caller pumps Recv for subsequent events.
// Close MUST be called if the caller does not drain to EndStream;
// it is idempotent and sends RST_STREAM(CANCEL) when needed.
//
// Callers may allocate StreamResponse once and reuse across DoStream calls;
// sr.Close() handles header-block cleanup automatically.
type StreamResponse struct {
	// Status is the integer value parsed from :status.
	Status int
	// Headers is the regular response header fields received with
	// the initial HEADERS frame. Valid until Close() is called.
	Headers []conn.HeaderField

	stream    respStream
	release   releaser
	closeOnce sync.Once
	drained   bool
	trailers  []conn.HeaderField // cached when Recv delivers EventTrailers

	// blocks holds the pooled header blocks backing Headers — the field slice
	// and the bytes behind it. Released on Close().
	blocks []*conn.HeaderBlock

	// doCtx is the context DoStream was called with. recvCtx is that context
	// merged with this StreamResponse's own abort, and abortCancel fires the
	// abort. Close needs them because conn.Stream.Recv parks on the event
	// channel and the stream's reset/end signals — not on anything Close
	// touches — so tearing the stream down does not wake a reader already
	// blocked in it. Without this an abort returned promptly and the goroutine
	// behind it hung until the caller's own deadline.
	//
	// Keeping recvCtx pre-merged is what stops Recv allocating a context per
	// event on the streaming path: a caller that passes the same context it
	// gave DoStream, which is the ordinary shape, hits the identity check in
	// Recv and pays nothing.
	doCtx       context.Context
	recvCtx     context.Context
	abortCancel context.CancelFunc

	// mu guards curData and closed. Close is a legitimate call from another
	// goroutine — it is how a caller aborts a stream it no longer wants — and
	// without this it handed the pooled DATA slab back while a concurrent Recv
	// still held it, so a later request could be given the same slab and
	// overwrite bytes this caller was about to read. Reported by -race at the
	// putDataSlab in recycleData.
	//
	// mu is never held across stream.Recv or stream.Close: those block, and
	// holding it there would make the abort wait for the read it is aborting.
	mu     sync.Mutex
	closed bool

	// curData is the pooled buffer backing the Data of the most recently
	// delivered EventData. Recycled on the next Recv (Data is valid only
	// until then per the StreamEvent contract) and on Close.
	curData *[]byte

	// lg reports a leak if this StreamResponse is garbage-collected without
	// Close (debug builds only; nil and no-op in normal builds).
	lg *leakGuard
}

// recycleData returns the last delivered EventData's pooled buffer to the pool.
// Callers must hold sr.mu.
func (sr *StreamResponse) recycleData() {
	if sr.curData != nil {
		putDataSlab(sr.curData)
		sr.curData = nil
	}
}

// recvContext returns the context to park in stream.Recv on: the caller's,
// merged with this StreamResponse's abort so Close can wake a parked reader.
// The returned stop func must be called before Recv returns.
//
// The common case is free. A caller that hands Recv the same context it gave
// DoStream gets the merged one built once at DoStream time; only a caller that
// varies the context per call pays for a fresh merge.
func (sr *StreamResponse) recvContext(ctx context.Context) (context.Context, func()) {
	if sr.recvCtx == nil {
		return ctx, func() {} // pre-#370 construction path, e.g. a zero value
	}
	if ctx == sr.doCtx {
		return sr.recvCtx, func() {}
	}
	cctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(sr.recvCtx, cancel)
	return cctx, func() { stop(); cancel() }
}

// reset zeroes the private fields before DoStream reuses the struct.
// The exported Headers slice backing array is retained for reuse.
func (sr *StreamResponse) reset() {
	sr.Status = 0
	sr.Headers = sr.Headers[:0]
	sr.stream = nil
	sr.release = nil
	sr.closeOnce = sync.Once{}
	sr.drained = false
	sr.trailers = nil
	sr.curData = nil // Close() already recycled it; clear defensively, do not Put.
	sr.closed = false
	// Fire it rather than just dropping it. Close normally has, and cancel is
	// idempotent — but DoStream resets the struct on entry, so a caller that
	// reuses a StreamResponse without closing the previous one would otherwise
	// leave a cancelCtx attached to its parent for as long as that parent lives.
	if sr.abortCancel != nil {
		sr.abortCancel()
	}
	sr.doCtx = nil
	sr.recvCtx = nil
	sr.abortCancel = nil
	// blocks are cleaned up in Close(); reset() is only called for a
	// struct that has been properly closed already.
}

// Recv blocks until the next event is available, the stream
// terminates, or ctx is cancelled. After the event whose EndStream is
// true, subsequent calls return ErrStreamEnded.
func (sr *StreamResponse) Recv(ctx context.Context) (StreamEvent, error) {
	// The previously delivered EventData.Data is invalid once Recv is called
	// again; recycle its pooled buffer now (also returns the final frame's
	// buffer when a fully-drained caller calls Recv past EndStream).
	sr.mu.Lock()
	sr.recycleData()
	sr.mu.Unlock()
	if sr.drained {
		return StreamEvent{}, ErrStreamEnded
	}
	rctx, stopAbort := sr.recvContext(ctx)
	defer stopAbort()
	for {
		ev, err := sr.stream.Recv(rctx)
		if err != nil {
			return StreamEvent{}, err
		}
		switch ev.Type {
		case conn.EventInterimHeaders:
			// Informational 1xx (RFC 7540 §8.1). DoStream already consumed the
			// final HEADERS to fill Status, so a 1xx reaching Recv is a peer
			// oddity; skip it and keep pumping. StreamResponse exposes no
			// interim surface, matching Client.Do across all three protocols.
			ev.Release()
			continue
		case conn.EventHeaders:
			// Spurious post-initial HEADERS without trailer detection —
			// protocol oddity from peer. Skip and keep pumping.
			ev.Release()
			continue
		case conn.EventData:
			out := StreamEvent{
				Type:      EventData,
				Data:      ev.Data,
				EndStream: ev.EndStream,
			}
			sr.mu.Lock()
			if sr.closed {
				// Close landed while we were parked in stream.Recv. The stream
				// is gone and release() has run; hand this frame's slab straight
				// back rather than storing it on a StreamResponse nobody will
				// drain, and do not return Data that aliases a pooled buffer.
				putDataSlab(ev.DataSlab)
				sr.mu.Unlock()
				return StreamEvent{}, ErrStreamEnded
			}
			sr.curData = ev.DataSlab
			sr.mu.Unlock()
			if ev.EndStream {
				sr.drained = true
			}
			return out, nil
		case conn.EventTrailers:
			out := StreamEvent{
				Type:      EventTrailers,
				Trailers:  ev.Headers,
				EndStream: ev.EndStream,
			}
			sr.trailers = out.Trailers // cache for WaitTrailers
			if sr.trailers == nil {
				sr.trailers = []conn.HeaderField{} // sentinel: EventTrailers received but empty
			}
			if ev.EndStream {
				sr.drained = true
			}
			return out, nil
		case conn.EventPushPromise:
			// Delivered on the parent stream, which is this one. StreamResponse
			// exposes no push surface (Client.Do's buffered path is the only one
			// that dispatches to a push handler), so the promise is skipped —
			// but its header block goes back to the pool, as in the arms above.
			ev.Release()
			continue
		case conn.EventReset:
			sr.drained = true
			return StreamEvent{
				Type:      EventReset,
				ResetCode: ev.RSTCode,
				EndStream: true,
			}, nil
		}
	}
}

// WaitTrailers pumps Recv, discarding any remaining EventData events,
// until EventTrailers arrives or the stream ends. Returns the trailer
// fields and nil on success. Returns nil, nil when the server sent no
// trailers or the stream was reset — use Recv directly to distinguish
// these cases. Returns nil, ctx.Err() when the context is cancelled.
//
// When the server sends an empty trailer block (EventTrailers with no
// header fields), WaitTrailers returns a non-nil empty slice; callers
// can distinguish "trailers received" (non-nil) from "no trailers"
// (nil).
//
// If Recv already delivered EventTrailers, the cached result is
// returned immediately without further network I/O.
func (sr *StreamResponse) WaitTrailers(ctx context.Context) ([]conn.HeaderField, error) {
	if sr.trailers != nil {
		return sr.trailers, nil
	}
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, ErrStreamEnded) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		//exhaustive:ignore // EventNone is the zero value, which Recv never
		// returns alongside a nil error: every arm of its switch either returns
		// a typed event or continues the loop.
		switch ev.Type {
		case EventData:
			continue
		case EventTrailers:
			return ev.Trailers, nil // also cached in sr.trailers by Recv
		case EventReset:
			return nil, nil
		}
	}
}

// Close releases the stream and returns any pooled header blocks.
// If neither side reached END_STREAM, RST_STREAM(CANCEL) is sent. Idempotent.
func (sr *StreamResponse) Close() error {
	var closeErr error
	sr.closeOnce.Do(func() {
		sr.lg.disarm()
		for _, b := range sr.blocks {
			b.Release()
		}
		sr.blocks = sr.blocks[:0]
		// Both outside mu on purpose: they release a Recv parked in the stream,
		// and holding the lock across them would make the abort wait for the
		// very read it is aborting.
		//
		// The cancel is what actually wakes that Recv. stream.Close tears the
		// stream down but does not signal a reader already blocked on the event
		// channel, so without this an abort through DoStream left the goroutine
		// hanging until the caller's own deadline — Close returned promptly and
		// the reader behind it did not.
		if sr.abortCancel != nil {
			sr.abortCancel()
		}
		closeErr = sr.stream.Close()
		sr.mu.Lock()
		sr.closed = true // a Recv that wakes now hands its own slab straight back
		sr.recycleData()
		sr.mu.Unlock()
		if sr.release != nil {
			sr.release.Release()
		}
	})
	return closeErr
}

// ErrStreamEnded is returned from StreamResponse.Recv after the final
// event with EndStream=true has been delivered.
var ErrStreamEnded = errors.New("client: stream ended")

// parseStatus extracts the integer value of the :status pseudo-header
// and appends all non-pseudo headers into *dst. Returns ErrEmptyResponse
// if :status is absent or unparseable.
func parseStatus(in []conn.HeaderField, dst *[]conn.HeaderField) (int, error) {
	for i := range in {
		if !bytes.Equal(in[i].Name, hdrStatus) {
			continue
		}
		n, perr := parseThreeDigitInt(in[i].Value)
		if perr != nil {
			return 0, fmt.Errorf("%w: %q", ErrInvalidStatus, in[i].Value)
		}
		for j := range in {
			if j != i {
				*dst = append(*dst, in[j])
			}
		}
		return n, nil
	}
	return 0, ErrEmptyResponse
}

// parseThreeDigitInt parses a 3-digit decimal number from b without allocating.
// HTTP/2 status codes are always exactly 3 ASCII digits (RFC 7540 §8.1.2.1).
func parseThreeDigitInt(b []byte) (int, error) {
	if len(b) != 3 {
		return 0, fmt.Errorf("invalid status: expected 3 digits, got %d", len(b))
	}
	d0 := int(b[0] - '0')
	d1 := int(b[1] - '0')
	d2 := int(b[2] - '0')
	if d0 < 0 || d0 > 9 || d1 < 0 || d1 > 9 || d2 < 0 || d2 > 9 {
		return 0, fmt.Errorf("invalid status: non-digit character")
	}
	return d0*100 + d1*10 + d2, nil
}
