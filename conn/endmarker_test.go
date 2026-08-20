package conn

import (
	"context"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.Equalf(t, cap(s.events), len(s.events),
		"channel is %d/%d; the test needs it full", len(s.events), cap(s.events))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The terminal marker: END_STREAM, no payload, no room.
	s.deliverEnd(StreamEvent{Type: EventData, EndStream: true}, true)

	w.mu.Lock()
	rst := w.bestEffortRSTs
	w.mu.Unlock()
	require.Zerof(t, rst, "wrote %d RST_STREAM for a response the peer completed", rst)
	for i, wantType := range []StreamEventType{EventHeaders, EventData} {
		ev, err := s.ref().Recv(ctx)
		require.NoErrorf(t, err, "Recv %d", i)
		require.Equalf(t, wantType, ev.Type,
			"event %d = %v, want %v — a reset here would mean the response was shed", i, ev.Type, wantType)
	}
	ev, err := s.ref().Recv(ctx)
	require.NoErrorf(t, err, "terminal Recv")
	require.NotEqualf(t, EventReset, ev.Type,
		"terminal event is a reset (code %v); the response was complete", ev.RSTCode)
	require.Truef(t, ev.EndStream, "terminal event = %v without EndStream", ev.Type)
}

// TestStream_TerminalTrailersStillShedWhenFull is the scope guard. A trailer
// block also ends the stream, and it also arrives through deliverEnd — but it
// carries fields the caller must receive, so diverting it out of band would
// silently drop response metadata. Only a payload-free DATA frame qualifies.
func TestStream_TerminalTrailersStillShedWhenFull(t *testing.T) {
	w := &fakeStreamWriter{}
	s := newStream(1, 1, w, 65535)
	s.push(StreamEvent{Type: EventHeaders})
	// Built the way emitHeaderBlock builds it, so the event under test carries
	// the same owned storage a real trailer block does.
	blk := copyFieldsToBlock([]header.Field{{Name: []byte("grpc-status"), Value: []byte("0")}})

	enqueued := s.deliverEnd(StreamEvent{
		Type:      EventTrailers,
		Headers:   blk.Fields(),
		Block:     blk,
		EndStream: true,
	}, true)

	require.False(t, enqueued, "trailers were enqueued into a full channel")
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	require.True(t, closed,
		"a trailer block that could not be delivered must still shed the stream, not vanish")
}

// TestStream_NonTerminalEmptyDataStillShedsTheStream is the third row of the
// decision table the two tests above cover between them: they pin the type
// conjunct and the terminal case, and nothing drove end=false (#801).
//
// deliverEnd diverts a payload-free DATA frame out of band only when it is
// TERMINAL. A peer may legally send a zero-length DATA without END_STREAM, and
// OnData passes that flag straight through from the wire. Without the `end`
// conjunct such a frame calls signalEnd() on a stream the peer has not ended —
// the consumer sees a clean, complete response in the middle of a body — instead
// of taking the ordinary overflow path.
func TestStream_NonTerminalEmptyDataStillShedsTheStream(t *testing.T) {
	w := &fakeStreamWriter{}
	s := newStream(1, 1, w, 65535)
	s.push(StreamEvent{Type: EventHeaders})
	require.Equalf(t, cap(s.events), len(s.events),
		"channel is %d/%d; the test needs it full", len(s.events), cap(s.events))

	enqueued := s.deliverEnd(StreamEvent{Type: EventData}, false)

	require.False(t, enqueued, "a payload-free DATA frame was enqueued into a full channel")
	s.mu.Lock()
	closed, remoteEnded := s.closed, s.remoteEnded
	s.mu.Unlock()
	assert.True(t, closed,
		"a NON-terminal empty DATA frame that could not be delivered must shed the stream "+
			"like any other undeliverable event, not be quietly absorbed")
	assert.False(t, remoteEnded,
		"remoteEnded was set for a frame the peer did not mark END_STREAM")
	assert.False(t, s.endSignalled.Load(),
		"signalEnd() ran for a frame the peer did not mark END_STREAM; the consumer would "+
			"read a complete response with the rest of the body still to come")
}
