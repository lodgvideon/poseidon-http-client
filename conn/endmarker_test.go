package conn

import (
	"context"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestStream_TerminalEmptyDataNeverShedsTheStream pins the shape behind #344.
//
// A flushing server whose chunks exactly fill the event channel loses the whole
// response to the frame that carries no bytes at all: every byte of the body is
// already delivered, and then the zero-length DATA frame that only says "this
// is the end" finds the channel full, so pushLocked resets the stream and hands
// the caller an EventReset instead of a completed response.
//
// Nothing is gained by shedding a stream over a marker. The event is reported
// out of band instead, and the consumer sees its buffered body followed by a
// clean end.
func TestStream_TerminalEmptyDataNeverShedsTheStream(t *testing.T) {
	w := &fakeStreamWriter{}
	s := newStream(1, 2, w, 65535)
	s.push(StreamEvent{Type: EventHeaders})
	s.push(StreamEvent{Type: EventData, Data: []byte("body")})
	if got, want := len(s.events), cap(s.events); got != want {
		t.Fatalf("channel is %d/%d; the test needs it full", got, want)
	}

	// The terminal marker: END_STREAM, no payload, no room.
	s.deliverEnd(StreamEvent{Type: EventData, EndStream: true}, true)

	w.mu.Lock()
	rst := w.rstCalls
	w.mu.Unlock()
	if rst != 0 {
		t.Fatalf("wrote %d RST_STREAM for a response the peer completed", rst)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	want := []StreamEventType{EventHeaders, EventData}
	for i, wantType := range want {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if ev.Type != wantType {
			t.Fatalf("event %d = %v, want %v", i, ev.Type, wantType)
		}
		if ev.Type == EventReset {
			t.Fatalf("event %d is a reset; the response was complete", i)
		}
	}
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("terminal Recv: %v", err)
	}
	if ev.Type == EventReset {
		t.Fatalf("terminal event is a reset (code %v); the response was complete", ev.RSTCode)
	}
	if !ev.EndStream {
		t.Fatalf("terminal event = %v without EndStream", ev.Type)
	}
}

// TestStream_TerminalTrailersStillShedWhenFull is the scope guard. A trailer
// block also ends the stream, and it also arrives through deliverEnd — but it
// carries fields the caller must receive, so diverting it out of band would
// silently drop response metadata. Only a payload-free DATA frame qualifies.
func TestStream_TerminalTrailersStillShedWhenFull(t *testing.T) {
	w := &fakeStreamWriter{}
	s := newStream(1, 1, w, 65535)
	s.push(StreamEvent{Type: EventHeaders})

	slab := make([]byte, 0)
	if s.deliverEnd(StreamEvent{
		Type:      EventTrailers,
		Headers:   []hpack.HeaderField{{Name: []byte("grpc-status"), Value: []byte("0")}},
		Slab:      &slab,
		EndStream: true,
	}, true) {
		t.Fatal("trailers were enqueued into a full channel")
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if !closed {
		t.Fatal("a trailer block that could not be delivered must still shed the stream, not vanish")
	}
}
