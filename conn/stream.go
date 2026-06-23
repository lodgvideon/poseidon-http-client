package conn

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// StreamEventType discriminates the StreamEvent variants.
type StreamEventType uint8

// StreamEventType values. Use Type to dispatch which fields of
// StreamEvent are populated.
const (
	EventHeaders     StreamEventType = iota + 1 // Headers populated
	EventData                                   // Data populated
	EventTrailers                               // Headers populated, trailers
	EventReset                                  // RSTCode populated
	EventPushPromise                            // Headers populated (promised), PushStreamID set
)

// String returns the lowercase name of t.
func (t StreamEventType) String() string {
	switch t {
	case EventHeaders:
		return "headers"
	case EventData:
		return "data"
	case EventTrailers:
		return "trailers"
	case EventReset:
		return "reset"
	case EventPushPromise:
		return "push_promise"
	default:
		return "unknown"
	}
}

// StreamEvent is one observation about an in-flight stream. The Type
// field tells the caller which other fields are populated.
//
// When Slab is non-nil, all Headers[i].Name and .Value byte slices are
// sub-slices of *Slab. Ownership transfers to the client layer, which
// returns the pointer to conn.GetHeaderSlabPool() in Response.Reset / sr.Close.
type StreamEvent struct {
	Type      StreamEventType
	Headers   []hpack.HeaderField // EventHeaders / EventTrailers
	Data      []byte              // EventData
	EndStream bool                // any event closing the response side
	RSTCode   frame.ErrCode       // EventReset

	// PushStreamID is the promised (even) stream ID for EventPushPromise.
	PushStreamID uint32 // EventPushPromise

	// Slab is the pooled backing buffer pointer for all Headers[i].Name
	// and .Value slices. nil for non-headers events and when the pool is
	// cold (first request). The client layer must return this pointer to
	// conn.GetHeaderSlabPool(), not the slice value, to avoid heap escape.
	Slab *[]byte

	// DataSlab is the pooled buffer backing Data (EventData). nil for
	// non-data events and when the pool is cold. The client returns it to
	// conn.GetDataBufPool() once Data is consumed — incrementally on the
	// streaming paths (Data is valid until the next Recv), immediately on
	// the buffered path. Return the pointer, not the slice, to avoid escape.
	// Buffers of events still queued at stream/connection teardown are
	// dropped to GC rather than pooled (see recycleStream).
	DataSlab *[]byte
}

// streamWriter is the narrow surface a *Stream needs from its owner Conn.
// Tests fake this out; production code wires it to *Conn.
type streamWriter interface {
	writeHeadersWithPriority(ctx context.Context, s *Stream, fields []hpack.HeaderField, endStream bool, prio *frame.Priority) error
	writeData(ctx context.Context, s *Stream, p []byte, endStream bool) error
	writeRSTStream(s *Stream, code frame.ErrCode) error
}

// Stream is one in-flight HTTP/2 stream.
type Stream struct {
	id     uint32
	w      streamWriter
	events chan StreamEvent

	mu           sync.Mutex
	localEnded   bool // we sent END_STREAM
	remoteEnded  bool // peer sent END_STREAM (or RST)
	closed       bool // RST or graceful close
	inflightDone bool // inflight slot already returned to the pool

	// headersReceived is set after the first non-trailer HEADERS block
	// for this stream is delivered. The reader goroutine consults it to
	// classify subsequent HEADERS frames as trailers (RFC 7540 §8.1).
	// Single-goroutine access — only the reader goroutine reads and
	// writes this field — so no synchronization is required.
	headersReceived bool

	// recvWindow is the number of payload bytes the peer can still
	// send to *this* stream before we must refill it via WINDOW_UPDATE
	// (RFC 7540 §6.9.1). Initialized from our advertised
	// SETTINGS_INITIAL_WINDOW_SIZE; debited by every received DATA
	// frame's full payload length, including padding.
	recvWindow int32
	// recvRefundPending is the number of bytes we have already debited
	// but not yet returned to the peer via a WINDOW_UPDATE. Reset when
	// the connection emits a WINDOW_UPDATE for this stream.
	recvRefundPending uint32

	// sendWindow is the number of payload bytes we may still send on
	// *this* stream without WINDOW_UPDATE credit from the peer (RFC
	// 7540 §6.9.1, peer's per-stream view). Initialized from the
	// peer's SETTINGS_INITIAL_WINDOW_SIZE at first HEADERS write;
	// debited by writeData and replenished by OnWindowUpdate. Guarded
	// by Stream.mu.
	sendWindow int32

	// resetSignal is closed when the stream is forcibly reset (event
	// channel overflow, GOAWAY drain, or connection shutdown). Recv()
	// selects on it so a blocked consumer unblocks immediately even
	// when the events channel is full — no silent hang. Replaced with
	// a fresh channel in recycleStream.
	resetSignal chan struct{}

	// resetCode stores the ErrCode delivered with the forced reset.
	// 0 means no reset has been signalled. Written via CAS in
	// signalReset to guarantee exactly one close(resetSignal).
	resetCode atomic.Uint32

	// released guards Close() idempotency independently of the operational
	// `closed` flag. recycleStream resets `closed` (for pool reuse) but must
	// NOT reset `released`, so a repeat Close() while the struct is still
	// pooled is a no-op instead of dereferencing the nil-ed w. allocStream
	// re-arms it for the next lifetime (so a stale reference to a re-allocated
	// struct is not protected — callers must not retain across Close).
	released atomic.Bool
}

func newStream(id uint32, eventBuf int, w streamWriter, recvWindow int32) *Stream {
	return &Stream{
		id:          id,
		w:           w,
		events:      make(chan StreamEvent, eventBuf),
		recvWindow:  recvWindow,
		resetSignal: make(chan struct{}),
	}
}

// signalReset marks the stream as forcibly reset and closes resetSignal
// so any Recv() blocked on a full events channel unblocks immediately.
// The CAS ensures only the first caller closes resetSignal (idempotent).
func (s *Stream) signalReset(code frame.ErrCode) {
	if s.resetCode.CompareAndSwap(0, uint32(code)) {
		close(s.resetSignal)
	}
}

// recycleStream drains any buffered events, zeroes all fields, and
// returns s to pool. Only call when the stream is fully done (both
// sides ended or RST sent/received) and no goroutine holds a reference.
func recycleStream(pool *sync.Pool, s *Stream) {
	// Drop any events still buffered on the old channel. Their pooled DATA
	// buffers (DataSlab) are abandoned to GC here, matching shutdownStreams
	// and the header-slab teardown path: sync.Pool tolerates buffers that are
	// never Put back. These events were never delivered to a consumer, so
	// dropping them keeps exactly one return site per buffer — the consumer on
	// the next Recv/Close for delivered frames, or OnData itself for frames
	// dropped at push() under backpressure — and rules out a double-Put.
	for len(s.events) > 0 {
		<-s.events
	}
	// Recreate the events channel with the same capacity. Any stale
	// reference held by a goroutine from the previous stream lifetime
	// (e.g. a deferred push/RST send) now writes to the orphaned old
	// channel, preventing cross-stream event contamination.
	s.events = make(chan StreamEvent, cap(s.events))
	s.id = 0
	s.w = nil
	s.localEnded = false
	s.remoteEnded = false
	s.closed = false
	s.inflightDone = false
	s.headersReceived = false
	s.recvRefundPending = 0
	s.sendWindow = 0
	s.resetSignal = make(chan struct{})
	s.resetCode.Store(0)
	pool.Put(s)
}

// ID returns the HTTP/2 stream identifier.
func (s *Stream) ID() uint32 { return s.id }

// markRemoteEnd is called by the connection-level frame.Handler when
// END_STREAM is observed for this stream.
func (s *Stream) markRemoteEnd() {
	s.mu.Lock()
	s.remoteEnded = true
	s.mu.Unlock()
}

// push delivers an event from the reader goroutine. Non-blocking under
// the channel's capacity; documented as part of the public contract.
// On overflow: marks stream closed, dispatches the RST send to a
// background goroutine (so the reader is never blocked on wmu), and
// signals via resetSignal so a blocked Recv unblocks immediately.
func (s *Stream) push(e StreamEvent) bool {
	select {
	case s.events <- e:
		return true
	default:
	}
	s.mu.Lock()
	already := s.closed
	s.closed = true
	s.mu.Unlock()
	if already {
		return false
	}
	go func() {
		// Use best-effort write with a 5-second deadline so the goroutine
		// cannot hang indefinitely on a stuck transport (F-P0-04).
		if c, ok := s.w.(*Conn); ok {
			c.writeRSTStreamBestEffort(s, frame.ErrCodeRefusedStream)
		} else {
			_ = s.w.writeRSTStream(s, frame.ErrCodeRefusedStream)
		}
	}()
	// Try to deliver EventReset via channel; if full, signal via resetSignal.
	select {
	case s.events <- StreamEvent{
		Type:      EventReset,
		RSTCode:   frame.ErrCodeRefusedStream,
		EndStream: true,
	}:
	default:
		s.signalReset(frame.ErrCodeRefusedStream)
	}
	return false
}

// SendHeaders sends a HEADERS frame with the given fields. Always emits
// END_HEADERS=true (B.1 does not split into CONTINUATION). When endStream
// is true the request side is half-closed.
// SendHeaders sends a HEADERS frame with the given fields. When called
// for the first time on a Stream, the connection assigns the stream ID
// under the writer mutex, ensuring the on-wire ID order matches RFC
// 7540 §5.1.1's monotonic-id rule. Always emits END_HEADERS=true (B.1
// does not split into CONTINUATION). When endStream is true the request
// side is half-closed.
// SendHeaders sends a HEADERS frame with the given fields. When called
// for the first time on a Stream, the connection assigns the stream ID
// under the writer mutex, ensuring the on-wire ID order matches RFC
// 7540 §5.1.1's monotonic-id rule. Always emits END_HEADERS=true (B.1
// does not split into CONTINUATION). When endStream is true the request
// side is half-closed.
func (s *Stream) SendHeaders(ctx context.Context, fields []hpack.HeaderField, endStream bool) error {
	return s.SendHeadersWithPriority(ctx, fields, endStream, nil)
}

// SendHeadersWithPriority sends a HEADERS frame with optional
// PRIORITY fields embedded (RFC 7540 §6.3). When prio is non-nil
// the HEADERS frame carries the PRIORITY flag plus a 5-byte
// priority payload. StreamID 0 in prio means root stream (no
// parent). When prio is nil the frame is emitted identically to
// SendHeaders.
func (s *Stream) SendHeadersWithPriority(ctx context.Context, fields []hpack.HeaderField, endStream bool, prio *frame.Priority) error {
	s.mu.Lock()
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	if err := s.w.writeHeadersWithPriority(ctx, s, fields, endStream, prio); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
		if c, ok := s.w.(*Conn); ok {
			c.markStreamDone(s.id)
		}
	}
	return nil
}

// SendData sends a single DATA frame. The caller is responsible for
// chunking p to fit the peer's MaxFrameSize. When endStream is true the
// request side is half-closed.
// SendData sends a single DATA frame. The caller must call SendHeaders
// first; the request side is half-closed when endStream is true.
// SendData sends a DATA frame, automatically chunking the payload to
// the peer's MAX_FRAME_SIZE and respecting both per-stream and
// connection-level outbound flow control (RFC 7540 §6.9). Blocks until
// enough send-window credit is available, the context is cancelled, or
// the connection closes. The caller must call SendHeaders first.
func (s *Stream) SendData(ctx context.Context, p []byte, endStream bool) error {
	s.mu.Lock()
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	if err := s.w.writeData(ctx, s, p, endStream); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
		if c, ok := s.w.(*Conn); ok {
			c.markStreamDone(s.id)
		}
	}
	return nil
}

// Recv blocks until the next event for this stream is ready, the stream
// terminates, or ctx is cancelled.
func (s *Stream) Recv(ctx context.Context) (StreamEvent, error) {
	select {
	case e, ok := <-s.events:
		if !ok {
			return StreamEvent{}, ErrStreamClosed
		}
		return e, nil
	case <-s.resetSignal:
		code := frame.ErrCode(s.resetCode.Load())
		return StreamEvent{Type: EventReset, RSTCode: code, EndStream: true}, nil
	case <-ctx.Done():
		return StreamEvent{}, ctx.Err()
	}
}

// Close cancels the stream. If neither side has reached END_STREAM, sends
// RST_STREAM(CANCEL). Idempotent for the common case: a repeat Close() is a
// no-op while the recycled struct still sits in the pool. Callers must not
// retain a *Stream past Close — allocStream re-arms the guard for the next
// lifetime, so a Close on a stale reference to a re-allocated struct is not
// protected (no in-tree caller does this).
func (s *Stream) Close() error {
	// released is the idempotency guard. It survives recycleStream (which
	// resets closed/w/... for pool reuse), so a repeat Close while the struct
	// is still pooled returns here instead of dereferencing the nil-ed w.
	if !s.released.CompareAndSwap(false, true) {
		return nil
	}
	s.mu.Lock()
	already := s.closed // e.g. push() set this on event-channel overflow
	bothEnded := s.localEnded && s.remoteEnded
	s.closed = true
	w := s.w
	s.mu.Unlock()
	if already {
		// Already closed (RST already sent by push overflow); don't double-RST.
		return nil
	}
	if bothEnded {
		// Both sides ended normally; recycle without sending RST.
		if c, ok := w.(*Conn); ok {
			recycleStream(&c.streamPool, s)
		}
		return nil
	}
	return w.writeRSTStream(s, frame.ErrCodeCancel)
}
