package conn

import (
	"context"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// StreamEventType discriminates the StreamEvent variants.
type StreamEventType uint8

const (
	EventHeaders  StreamEventType = iota + 1
	EventData
	EventTrailers
	EventReset
)

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
	writeHeaders(streamID uint32, fields []hpack.HeaderField, endStream bool) error
	writeData(streamID uint32, p []byte, endStream bool) error
	writeRSTStream(streamID uint32, code frame.ErrCode) error
}

// Stream is one in-flight HTTP/2 stream.
type Stream struct {
	id     uint32
	w      streamWriter
	events chan StreamEvent

	mu          sync.Mutex
	localEnded  bool // we sent END_STREAM
	remoteEnded bool // peer sent END_STREAM (or RST)
	closed      bool // RST or graceful close
}

func newStream(id uint32, eventBuf int, w streamWriter) *Stream {
	return &Stream{
		id:     id,
		w:      w,
		events: make(chan StreamEvent, eventBuf),
	}
}

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
		_ = s.w.writeRSTStream(s.id, frame.ErrCodeRefusedStream)
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}
}

func (s *Stream) SendHeaders(ctx context.Context, fields []hpack.HeaderField, endStream bool) error {
	s.mu.Lock()
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	if err := s.w.writeHeaders(s.id, fields, endStream); err != nil {
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

func (s *Stream) SendData(ctx context.Context, p []byte, endStream bool) error {
	s.mu.Lock()
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	if err := s.w.writeData(s.id, p, endStream); err != nil {
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

func (s *Stream) Close() error {
	s.mu.Lock()
	already := s.closed
	bothEnded := s.localEnded && s.remoteEnded
	s.closed = true
	s.mu.Unlock()
	if already || bothEnded {
		return nil
	}
	return s.w.writeRSTStream(s.id, frame.ErrCodeCancel)
}
