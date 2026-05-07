package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Response is the synchronous result of Client.Do. Body and Trailers
// are nil when the corresponding Request.WantBody / WantTrailers was
// false. Headers, Body, and Trailers are deep copies and are safe to
// retain after Do returns.
type Response struct {
	// Status is the integer value parsed from the :status pseudo-header.
	Status int
	// Headers is the regular response header fields (no pseudo-headers).
	Headers []hpack.HeaderField
	// Body is nil unless Request.WantBody is true.
	Body []byte
	// Trailers is nil unless Request.WantTrailers is true and the peer
	// sent a trailers frame.
	Trailers []hpack.HeaderField
	// BytesReceived is the total DATA payload received, even when
	// Request.WantBody was false.
	BytesReceived int64
}

// EventType discriminates StreamEvent variants returned from
// StreamResponse.Recv.
type EventType uint8

// EventType values.
const (
	// EventData carries a chunk of DATA payload in StreamEvent.Data.
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

// StreamEvent is one chunk of a streaming response. Data slice aliases
// internal scratch buffers and is valid only until the next Recv on
// the same StreamResponse — copy if retained.
type StreamEvent struct {
	// Type discriminates which other fields are populated.
	Type EventType
	// Data is the DATA payload for EventData. Aliases scratch.
	Data []byte
	// Trailers is populated for EventTrailers.
	Trailers []hpack.HeaderField
	// ResetCode is populated for EventReset.
	ResetCode frame.ErrCode
	// EndStream is true on the final event of a stream.
	EndStream bool
}

// StreamResponse is returned by Client.DoStream after the initial
// HEADERS frame arrives. The caller pumps Recv for subsequent events.
// Close MUST be called if the caller does not drain to EndStream;
// it is idempotent and sends RST_STREAM(CANCEL) when needed.
type StreamResponse struct {
	// Status is the integer value parsed from :status.
	Status int
	// Headers is the regular response header fields received with
	// the initial HEADERS frame.
	Headers []hpack.HeaderField

	stream    *conn.Stream
	release   func()
	closeOnce sync.Once
	drained   bool
}

// Recv blocks until the next event is available, the stream
// terminates, or ctx is cancelled. After the event whose EndStream is
// true, subsequent calls return ErrStreamEnded.
func (sr *StreamResponse) Recv(ctx context.Context) (StreamEvent, error) {
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
			// Spurious second HEADERS — protocol-level oddity. Skip.
			continue
		case conn.EventData:
			out := StreamEvent{
				Type:      EventData,
				Data:      ev.Data,
				EndStream: ev.EndStream,
			}
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

// Close releases the stream. If neither side reached END_STREAM, the
// underlying conn.Stream sends RST_STREAM(CANCEL). Idempotent.
func (sr *StreamResponse) Close() error {
	var closeErr error
	sr.closeOnce.Do(func() {
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
// from a HEADERS payload. Returns ErrEmptyResponse if absent or
// unparseable. The returned regular slice is the input with :status
// removed.
func parseStatus(in []hpack.HeaderField) (status int, regular []hpack.HeaderField, err error) {
	for i := range in {
		if string(in[i].Name) == ":status" {
			n, perr := strconv.Atoi(string(in[i].Value))
			if perr != nil {
				return 0, nil, fmt.Errorf("%w: :status %q not numeric",
					ErrEmptyResponse, in[i].Value)
			}
			regular = make([]hpack.HeaderField, 0, len(in)-1)
			regular = append(regular, in[:i]...)
			regular = append(regular, in[i+1:]...)
			return n, regular, nil
		}
	}
	return 0, nil, ErrEmptyResponse
}
