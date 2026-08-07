package conn

import (
	"testing"
)

// TestPushIfID_RefusesRecycledParent pins the id gate on PUSH_PROMISE delivery.
//
// handlePushPromiseBlock resolves the parent by id and then does real work
// before delivering — reserving the promised stream, taking a slab from the
// pool, copying every header field. A *Stream is pooled, so in that window the
// application can finish the parent request, Close it, and a fresh NewStream can
// claim the struct. An ungated push then announces the promise to whichever
// request now owns it.
//
// The recycle is simulated by assigning a new id, so the property is pinned
// deterministically rather than by racing the window.
func TestPushIfID_RefusesRecycledParent(t *testing.T) {
	s := &Stream{id: 3, events: make(chan StreamEvent, 4), resetSignal: make(chan struct{})}
	ev := StreamEvent{Type: EventPushPromise, PushStreamID: 4}

	// Still the same stream: delivered.
	if !s.pushIfID(3, ev) {
		t.Fatal("pushIfID refused the live parent")
	}
	select {
	case got := <-s.events:
		if got.Type != EventPushPromise || got.PushStreamID != 4 {
			t.Fatalf("delivered %+v, want the promise", got)
		}
	default:
		t.Fatal("the live parent received nothing")
	}

	// Recycled and re-issued to another request before delivery.
	s.id = 7
	if s.pushIfID(3, ev) {
		t.Fatal("pushIfID delivered to a recycled struct; the gate did not hold")
	}
	select {
	case got := <-s.events:
		t.Fatalf("the next request was announced %+v — a promise it never asked for", got)
	default:
	}
	if s.closed {
		t.Fatal("the refused delivery closed the new lifetime's stream")
	}
}

// TestPushIfID_FullChannelStillResets pins that the gate does not swallow
// pushLocked's own overflow path: a live parent whose event channel is full must
// still be reset, exactly as an ungated push would.
func TestPushIfID_FullChannelStillResets(t *testing.T) {
	s := &Stream{id: 3, events: make(chan StreamEvent, 1), resetSignal: make(chan struct{}), w: &fakeStreamWriter{}}
	s.events <- StreamEvent{Type: EventData} // fill it

	if s.pushIfID(3, StreamEvent{Type: EventPushPromise, PushStreamID: 4}) {
		t.Fatal("pushIfID reported success on a full channel")
	}
	if !s.closed {
		t.Fatal("overflow on a live parent did not close the stream")
	}
	if !s.resetSignalled.Load() && len(s.events) < cap(s.events) {
		t.Fatal("neither an EventReset nor a reset signal reached the caller")
	}
}
