package conn

import (
	"context"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestConformance_RFC9113_Sec8_1_RecvPrefersBufferedEventsOverReset pins that a
// signalled reset never jumps ahead of events already delivered into the
// stream's channel.
//
// signalReset is reachable only from the default arm of a send into s.events —
// every caller (Stream.pushLocked, connHandler.OnRSTStream, onGoAwayReceived,
// shutdownStreams) tries the channel first — so a closed resetSignal means, by
// construction, that undelivered events are queued behind it. A plain
// three-way select expresses no priority and Go picks uniformly among ready
// cases, which made each Recv an independent coin flip: an N-event response
// reached its consumer with probability 2^-N.
//
// That is the §8.1 clause the conn layer already pins for the ordered path:
// "Clients MUST NOT discard responses as a result of receiving such a
// RST_STREAM". A server that answers in full and then sends
// RST_STREAM(NO_ERROR) to stop an upload it does not need hits exactly this
// path when the response filled the buffer.
func TestConformance_RFC9113_Sec8_1_RecvPrefersBufferedEventsOverReset(t *testing.T) {
	const trials = 2000
	resetFirst := 0
	for i := 0; i < trials; i++ {
		s := newStream(1, 4, &fakeStreamWriter{}, 65535)
		s.push(StreamEvent{Type: EventHeaders})
		s.push(StreamEvent{Type: EventData})
		s.push(StreamEvent{Type: EventTrailers, EndStream: true})
		s.signalReset(frame.ErrCodeNoError)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ev, err := s.Recv(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == EventReset {
			resetFirst++
		}
	}
	if resetFirst != 0 {
		t.Fatalf("the reset preempted buffered events %d/%d times; a complete response is discarded that often", resetFirst, trials)
	}
}

// TestConformance_RFC9113_Sec8_1_RecvStillReportsResetWhenDrained is the
// over-tolerance guard: preferring buffered events must not swallow the reset,
// only postpone it until there is nothing left to deliver.
func TestConformance_RFC9113_Sec8_1_RecvStillReportsResetWhenDrained(t *testing.T) {
	s := newStream(1, 4, &fakeStreamWriter{}, 65535)
	s.push(StreamEvent{Type: EventHeaders})
	s.signalReset(frame.ErrCodeEnhanceYourCalm)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if ev, err := s.Recv(ctx); err != nil || ev.Type != EventHeaders {
		t.Fatalf("first Recv = %v, %v; want the buffered headers", ev.Type, err)
	}
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("second Recv: %v", err)
	}
	if ev.Type != EventReset {
		t.Fatalf("second Recv = %v, want the reset once the buffer is empty", ev.Type)
	}
	if ev.RSTCode != frame.ErrCodeEnhanceYourCalm {
		t.Fatalf("reset code = %v, want ENHANCE_YOUR_CALM", ev.RSTCode)
	}
	if !ev.EndStream {
		t.Fatal("reset event did not carry EndStream")
	}
}

// TestStream_SignalReset_NoErrorIsIdempotent pins the guard on
// close(resetSignal). It used to be a CAS on resetCode from 0 to the code,
// which is not idempotent when the code is itself 0: the swap succeeds, leaves
// resetCode at 0, and admits the next caller — closing the channel twice and
// panicking. NO_ERROR is 0, and NO_ERROR is the code RFC 9113 §8.1 has a
// server send after a complete response, so the one value that broke the
// contract was the common one.
func TestStream_SignalReset_NoErrorIsIdempotent(t *testing.T) {
	s := newStream(1, 1, &fakeStreamWriter{}, 65535)
	s.signalReset(frame.ErrCodeNoError)
	s.signalReset(frame.ErrCodeCancel) // must not close a second time
	s.signalReset(frame.ErrCodeNoError)

	if got := frame.ErrCode(s.resetCode.Load()); got != frame.ErrCodeNoError {
		t.Fatalf("resetCode = %v, want the first signal's NO_ERROR", got)
	}
	select {
	case <-s.resetSignal:
	default:
		t.Fatal("resetSignal was never closed")
	}
}
