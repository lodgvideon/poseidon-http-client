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
	// Body is nil unless Request.WantBody is true.
	Body []byte
	// Trailers is nil unless Request.WantTrailers is true and the peer
	// sent a trailers frame.
	Trailers []conn.HeaderField
	// BytesReceived is the total DATA payload received, even when
	// Request.WantBody was false.
	BytesReceived int64

	// BodyReader is non-nil when the request had StreamBody=true.
	// Caller reads body bytes then calls Close(). Trailers (if any) are
	// written into Response.Trailers just before Close returns io.EOF.
	// Reset() calls Close() automatically when BodyReader is non-nil.
	BodyReader io.ReadCloser

	// slabs holds pooled slab pointers that back Headers and Trailers
	// field bytes. Storing *[]byte (not []byte) avoids heap escape when
	// returning to conn.GetHeaderSlabPool() via Put. Returned on Reset().
	slabs []*[]byte
}

// Reset clears all exported fields for reuse, retaining backing arrays.
// Any references to Headers[i].Name / .Value / Body / Trailers bytes
// must not be used after Reset returns.
//
// On the first call after a zero-value Response, the first Reset preallocates
// Headers and slabs backing arrays (cap=8 / cap=2) so subsequent appends in
// parseStatus and drainResponse do not allocate.
func (r *Response) Reset() {
	if r.BodyReader != nil {
		_ = r.BodyReader.Close()
		r.BodyReader = nil
	}
	for _, sp := range r.slabs {
		*sp = (*sp)[:0]
		conn.GetHeaderSlabPool().Put(sp)
	}
	r.slabs = r.slabs[:0]
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
	if cap(r.slabs) == 0 {
		r.slabs = make([]*[]byte, 0, 2)
	}
	r.Body = r.Body[:0]
	r.BytesReceived = 0
}

// EventType discriminates StreamEvent variants returned from
// StreamResponse.Recv.
type EventType uint8

// EventType values.
const (
	// EventData carries a chunk of DATA payload in StreamEvent.Data (valid only
	// until the next Recv/Close; see StreamEvent — copy to retain).
	EventData EventType = iota + 1
	// EventTrailers carries response trailers in StreamEvent.Trailers.
	EventTrailers
	// EventReset signals that the peer sent RST_STREAM; the code is
	// in StreamEvent.ResetCode and EndStream is always true.
	EventReset
)

// String returns the lowercase event-type name.
func (t EventType) String() string {
	switch t {
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
// Recv or Close; Trailers alias the response's header-slab buffer, valid until
// Close. Copy these slices if you need to retain the bytes past then — do NOT
// hold them across a Recv/Close.
type StreamEvent struct {
	// Type discriminates which other fields are populated.
	Type EventType
	// Data is the DATA payload for EventData. It aliases a pooled buffer that is
	// recycled on the next Recv/Close; copy it to retain the bytes past then.
	Data []byte
	// Trailers is populated for EventTrailers; aliases header-slab memory that
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
// sr.Close() handles slab cleanup automatically.
type StreamResponse struct {
	// Status is the integer value parsed from :status.
	Status int
	// Headers is the regular response header fields received with
	// the initial HEADERS frame. Valid until Close() is called.
	Headers []conn.HeaderField

	stream    *conn.Stream
	release   func()
	closeOnce sync.Once
	drained   bool
	trailers  []conn.HeaderField // cached when Recv delivers EventTrailers

	// slabs holds pooled slab pointers that back Headers field bytes.
	// Storing *[]byte avoids heap escape on return to HeaderSlabPool.
	slabs []*[]byte

	// curData is the pooled buffer backing the Data of the most recently
	// delivered EventData. Recycled on the next Recv (Data is valid only
	// until then per the StreamEvent contract) and on Close.
	curData *[]byte
}

// recycleData returns the last delivered EventData's pooled buffer to the pool.
func (sr *StreamResponse) recycleData() {
	if sr.curData != nil {
		conn.GetDataBufPool().Put(sr.curData)
		sr.curData = nil
	}
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
	// slabs are cleaned up in Close(); reset() is only called for a
	// struct that has been properly closed already.
}

// Recv blocks until the next event is available, the stream
// terminates, or ctx is cancelled. After the event whose EndStream is
// true, subsequent calls return ErrStreamEnded.
func (sr *StreamResponse) Recv(ctx context.Context) (StreamEvent, error) {
	// The previously delivered EventData.Data is invalid once Recv is called
	// again; recycle its pooled buffer now (also returns the final frame's
	// buffer when a fully-drained caller calls Recv past EndStream).
	sr.recycleData()
	if sr.drained {
		return StreamEvent{}, ErrStreamEnded
	}
	for {
		ev, err := sr.stream.Recv(ctx)
		if err != nil {
			return StreamEvent{}, err
		}
		switch ev.Type {
		case conn.EventHeaders:
			// Spurious post-initial HEADERS without trailer detection —
			// protocol oddity from peer. Skip and keep pumping.
			continue
		case conn.EventData:
			out := StreamEvent{
				Type:      EventData,
				Data:      ev.Data,
				EndStream: ev.EndStream,
			}
			sr.curData = ev.DataSlab
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

// Close releases the stream and returns any pooled header slabs.
// If neither side reached END_STREAM, RST_STREAM(CANCEL) is sent. Idempotent.
func (sr *StreamResponse) Close() error {
	var closeErr error
	sr.closeOnce.Do(func() {
		for _, sp := range sr.slabs {
			*sp = (*sp)[:0]
			conn.GetHeaderSlabPool().Put(sp)
		}
		sr.slabs = sr.slabs[:0]
		sr.recycleData()
		closeErr = sr.stream.Close()
		if sr.release != nil {
			sr.release()
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
