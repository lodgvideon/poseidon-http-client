package conn

import (
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
