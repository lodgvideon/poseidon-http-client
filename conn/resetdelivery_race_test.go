package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"

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
