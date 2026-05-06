package conn

import (
	"context"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// StreamEventType discriminates the StreamEvent variants.
type StreamEventType uint8

// StreamEventType values. Use Type to dispatch which fields of
// StreamEvent are populated.
const (
	EventHeaders  StreamEventType = iota + 1 // Headers populated
	EventData                                  // Data populated
	EventTrailers                              // Headers populated, trailers
	EventReset                                 // RSTCode populated
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
	default:
		return "unknown"
	}
}

// StreamEvent is one observation about an in-flight stream. The Type
// field tells the caller which other fields are populated. Slices alias
// internal pool buffers and are valid only until the next call to
// (*Stream).Recv or (*Stream).Close on the same stream.
type StreamEvent struct {
	Type      StreamEventType
	Headers   []hpack.HeaderField // EventHeaders / EventTrailers
	Data      []byte              // EventData
	EndStream bool                // any event closing the response side
	RSTCode   frame.ErrCode       // EventReset
}

// streamWriter is the narrow surface a *Stream needs from its owner Conn.
// Tests fake this out; production code wires it to *Conn.
type streamWriter interface {
	writeHeaders(ctx context.Context, s *Stream, fields []hpack.HeaderField, endStream bool) error
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
}

func newStream(id uint32, eventBuf int, w streamWriter, recvWindow int32) *Stream {
	return &Stream{
		id:         id,
		w:          w,
		events:     make(chan StreamEvent, eventBuf),
		recvWindow: recvWindow,
	}
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
func (s *Stream) push(e StreamEvent) {
	select {
	case s.events <- e:
	default:
		// Channel full -- drop and reset to protect the reader. Callers
		// who care must drain Recv promptly.
		_ = s.w.writeRSTStream(s, frame.ErrCodeRefusedStream)
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}
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
	s.mu.Lock()
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	if err := s.w.writeHeaders(ctx, s, fields, endStream); err != nil {
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
	case <-ctx.Done():
		return StreamEvent{}, ctx.Err()
	}
}

// Close cancels the stream. If neither side has reached END_STREAM, sends
// RST_STREAM(CANCEL). Idempotent.
func (s *Stream) Close() error {
	s.mu.Lock()
	already := s.closed
	bothEnded := s.localEnded && s.remoteEnded
	s.closed = true
	s.mu.Unlock()
	if already || bothEnded {
		return nil
	}
	return s.w.writeRSTStream(s, frame.ErrCodeCancel)
}
