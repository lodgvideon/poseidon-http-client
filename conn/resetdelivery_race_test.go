package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// newResetRaceStream builds the one state both tests below need: a stream whose
// response has fully arrived (END_STREAM, so remoteEnded) while the client's
// upload is still open (localEnded false, so markStreamDone has not evicted it
// and it is still in the registry for the reader to find).
//
// remoteEnded is set directly rather than by feeding a real HEADERS frame. That
// is the only synthetic step; markRemoteEnd does exactly this and nothing else
// that matters here, and the application half of each test drives the real
// public API.
func newResetRaceStream(t *testing.T) (*Conn, *Stream) {
	t.Helper()
	var buf bytes.Buffer
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.fr = frame.NewFramer(&buf, nil) // writer first
	s := newStream(5, 8, c, 65535)
	s.remoteEnded = true
	c.streams[5] = s
	c.inflight = 1
	return c, s
}

// finishAndClose is the application half: it completes the upload, which sets
// localEnded and drives markStreamDone to set connDone, and then Closes —
// which sees connDone and recycles the struct, rewriting s.events, s.resetSignal
// and s.id. None of that involves the connection reader, which is the point:
// the reader's teardown loop cannot assume it alone decides when a stream
// becomes recycle-eligible.
func finishAndClose(s *Stream) {
	_ = s.SendData(context.Background(), nil, true)
	_ = s.Close()
}

// TestOnRSTStream_ResetDeliveryRacesRecycle covers the reachable half. RFC 9113
// §8.1 lets a server that has sent a complete response follow it with
// RST_STREAM(NO_ERROR) to stop a request body it does not need — the shape
// behind #337, and what net/http2 and grpc-go both do for a handler that never
// reads the body. So a conforming peer produces this collision.
//
// Before the fix, OnRSTStream delivered the event on s.events and only then
// took s.mu, on the reasoning that a reset ahead of the stream becoming
// bothEnded could not race the recycle. It could: the application makes the
// stream recycle-eligible on its own. Failed under -race in ~0.15 s.
func TestOnRSTStream_ResetDeliveryRacesRecycle(t *testing.T) {
	for i := 0; i < 5000; i++ {
		c, s := newResetRaceStream(t)
		h := &connHandler{streams: c}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = h.OnRSTStream(frame.FrameHeader{StreamID: 5}, frame.ErrCodeNoError)
		}()
		go func() {
			defer wg.Done()
			<-start
			finishAndClose(s)
		}()
		close(start)
		wg.Wait()
	}
}

// TestOnGoAway_ResetDeliveryRacesRecycle is the same collision on the GOAWAY
// teardown path, which had the identical delivery-then-lock shape and, in
// addition, read s.id outside the lock to retire the stream — so a recycled
// struct could have retired whatever owned that id next.
//
// Its trigger needs a peer that advertises a last-stream-id below a stream it
// has already answered, which §6.8 does not permit; the frames come from the
// peer, so it is still reachable input rather than a can't-happen.
func TestOnGoAway_ResetDeliveryRacesRecycle(t *testing.T) {
	for i := 0; i < 5000; i++ {
		c, s := newResetRaceStream(t)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			c.onGoAwayReceived(1, frame.ErrCodeNoError)
		}()
		go func() {
			defer wg.Done()
			<-start
			finishAndClose(s)
		}()
		close(start)
		wg.Wait()
	}
}

// TestStream_EndWithReset_RefusesRecycledStruct pins the identity check, which
// -race cannot: once the delivery moved under the mutex there is no data race
// to report, but a reset carrying a dead lifetime's id would still be pushed
// into the fresh channel of whatever request now owns the struct — and its
// caller would see a reset it was never sent. Stream ids are assigned
// monotonically under wmu and recycling zeroes the field, so the id is a
// sufficient identity.
func TestStream_EndWithReset_RefusesRecycledStruct(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.fr = frame.NewFramer(&buf, nil)

	s := newStream(5, 8, c, 65535)
	recycleStream(s) // the struct is now pooled and its id is zero

	if s.endWithReset(5, frame.ErrCodeNoError) {
		t.Fatal("endWithReset accepted a stream id the struct no longer has")
	}
	if n := len(s.events); n != 0 {
		t.Fatalf("delivered %d event(s) into a recycled struct's channel", n)
	}
	if s.closed || s.localEnded || s.remoteEnded {
		t.Fatalf("recycled struct was re-ended: closed=%v local=%v remote=%v",
			s.closed, s.localEnded, s.remoteEnded)
	}

	// The struct's next lifetime is unaffected and still works.
	s.id = 7
	if !s.endWithReset(7, frame.ErrCodeNoError) {
		t.Fatal("endWithReset refused the current lifetime")
	}
	if n := len(s.events); n != 1 {
		t.Fatalf("events = %d, want the one reset", n)
	}
}

// TestStream_PushOverflow_EmitsCancelEverywhere pins the error code on all
// three places a shed stream announces itself, because they are separate
// branches and a test that happens to take one leaves the others free to
// diverge: the RST_STREAM put on the wire, the EventReset delivered when the
// channel still has room, and the resetSignal fallback when it does not.
//
// CANCEL, not REFUSED_STREAM. RFC 9113 §7 defines CANCEL as "the stream is no
// longer needed", which is what shedding a response is. §8.7 makes
// REFUSED_STREAM a promise about the SERVER — "the stream is being closed
// prior to any processing having occurred.  Any request that was sent on the
// reset stream can be safely retried." — and the client is in no position to
// make it: the server answered, and the retry layer acts on the promise.
func TestStream_PushOverflow_EmitsCancelEverywhere(t *testing.T) {
	t.Run("wire and resetSignal", func(t *testing.T) {
		w := &fakeStreamWriter{}
		s := newStream(1, 1, w, 65535)
		s.push(StreamEvent{Type: EventHeaders}) // fills the one slot
		s.push(StreamEvent{Type: EventData})    // overflows

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			w.mu.Lock()
			n := w.rstCalls
			w.mu.Unlock()
			if n > 0 {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		w.mu.Lock()
		calls, code := w.rstCalls, w.lastRSTCode
		w.mu.Unlock()
		if calls == 0 {
			t.Fatal("no RST_STREAM written after overflow")
		}
		if code != frame.ErrCodeCancel {
			t.Fatalf("wire RST code = %v, want CANCEL", code)
		}
		// The channel was still full, so the reset took the signal fallback.
		if got := frame.ErrCode(s.resetCode.Load()); got != frame.ErrCodeCancel {
			t.Fatalf("signalReset code = %v, want CANCEL", got)
		}
	})

	// The channel-delivered branch cannot be reached on demand: pushLocked
	// drains nothing between the push that overflows and the reset it then
	// tries to enqueue, so the channel is still full unless a consumer pops in
	// that window. Race for it instead of pretending it is deterministic, and
	// say so rather than passing quietly if the window never opens.
	t.Run("buffered event", func(t *testing.T) {
		delivered := 0
		for i := 0; i < 500; i++ {
			w := &fakeStreamWriter{}
			s := newStream(1, 1, w, 65535)
			s.push(StreamEvent{Type: EventHeaders})
			done := make(chan struct{})
			go func() {
				defer close(done)
				<-s.events
			}()
			s.push(StreamEvent{Type: EventData})
			<-done
			for len(s.events) > 0 {
				ev := <-s.events
				if ev.Type != EventReset {
					continue
				}
				delivered++
				if ev.RSTCode != frame.ErrCodeCancel {
					t.Fatalf("delivered reset code = %v, want CANCEL", ev.RSTCode)
				}
			}
		}
		if delivered == 0 {
			t.Skip("the consumer never popped inside the window; the channel branch was not exercised")
		}
		t.Logf("channel-delivered resets observed: %d/500", delivered)
	})
}
