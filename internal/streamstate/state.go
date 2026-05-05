// Package streamstate implements the HTTP/2 stream state machine
// (RFC 7540 §5.1). Pure logic, no concurrency, no I/O.
package streamstate

import "errors"

// State enumerates RFC 7540 §5.1 stream states.
type State uint8

const (
	Idle State = iota
	ReservedLocal
	ReservedRemote
	Open
	HalfClosedLocal
	HalfClosedRemote
	Closed
)

// Event triggers a transition.
type Event uint8

const (
	EventSendHeaders Event = iota
	EventRecvHeaders
	EventSendEndStream
	EventRecvEndStream
	EventSendRSTStream
	EventRecvRSTStream
	EventRecvPushPromise
)

// ErrInvalidTransition signals a protocol violation per RFC §5.1.
// Maps to HTTP/2 PROTOCOL_ERROR or STREAM_CLOSED depending on context
// (caller decides; we just flag invalidity here).
var ErrInvalidTransition = errors.New("poseidon/streamstate: invalid transition")

// Transition returns the new state after applying e to s, or
// ErrInvalidTransition if the transition is forbidden.
func Transition(s State, e Event) (State, error) {
	switch s {
	case Idle:
		switch e {
		case EventSendHeaders, EventRecvHeaders:
			return Open, nil
		case EventRecvPushPromise:
			return ReservedRemote, nil
		}
	case Open:
		switch e {
		case EventSendEndStream:
			return HalfClosedLocal, nil
		case EventRecvEndStream:
			return HalfClosedRemote, nil
		case EventSendRSTStream, EventRecvRSTStream:
			return Closed, nil
		case EventSendHeaders, EventRecvHeaders:
			// Trailers — staying in Open conceptually; caller validates that
			// the headers carry END_STREAM if appropriate.
			return Open, nil
		}
	case HalfClosedLocal:
		switch e {
		case EventRecvEndStream:
			return Closed, nil
		case EventSendRSTStream, EventRecvRSTStream:
			return Closed, nil
		case EventRecvHeaders:
			// Trailers from peer.
			return HalfClosedLocal, nil
		}
	case HalfClosedRemote:
		switch e {
		case EventSendEndStream:
			return Closed, nil
		case EventSendRSTStream, EventRecvRSTStream:
			return Closed, nil
		case EventSendHeaders:
			// Trailers from us.
			return HalfClosedRemote, nil
		}
	case ReservedLocal:
		switch e {
		case EventSendHeaders:
			return HalfClosedRemote, nil
		case EventSendRSTStream, EventRecvRSTStream:
			return Closed, nil
		}
	case ReservedRemote:
		switch e {
		case EventRecvHeaders:
			return HalfClosedLocal, nil
		case EventSendRSTStream, EventRecvRSTStream:
			return Closed, nil
		}
	case Closed:
		// No outbound transitions from Closed.
	}
	return s, ErrInvalidTransition
}
