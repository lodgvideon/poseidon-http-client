package conn

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestStream_RecvAfterClose_RefusesInsteadOfRegistering covers the holder the
// recvActive registration cannot see: a reader BETWEEN two Recv calls.
//
// Registering keeps a recycle off the struct while a goroutine is parked in
// the select. It says nothing about the gap between two calls, which is the
// ordinary shape of a read loop — client's response-body reader issues one
// Recv per Read and loops outside. A Close landing in that gap pools the
// struct; the next call used to register on it, so a reader from a finished
// request inflated the recvActive of whatever request claimed the struct next
// and deferred that request's recycle behind itself. It also parked on the
// orphaned channel until its own context expired, turning a finished stream
// into a full context's worth of waiting.
func TestStream_RecvAfterClose_RefusesInsteadOfRegistering(t *testing.T) {
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	s := c.allocStream(4, 65535)
	s.id = 5
	c.streams[5] = s
	s.push(StreamEvent{Type: EventHeaders})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if ev, err := s.Recv(ctx); err != nil || ev.Type != EventHeaders {
		t.Fatalf("first Recv = %v, %v", ev.Type, err)
	}

	// The reader is now between calls. Close lands in the gap and, with
	// connDone already set, pools the struct immediately.
	s.mu.Lock()
	s.connDone = true
	s.mu.Unlock()
	_ = s.Close()

	start := time.Now()
	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer rcancel()
	if _, err := s.Recv(rctx); err != ErrStreamClosed {
		t.Fatalf("Recv after Close = %v, want ErrStreamClosed", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Recv after Close took %v — it parked on the orphaned channel instead of refusing", d)
	}
	s.mu.Lock()
	ra := s.recvActive
	s.mu.Unlock()
	if ra != 0 {
		t.Fatalf("recvActive = %d on a pooled struct; the next request to claim it would never be recycled", ra)
	}
}

// TestStream_RecvAfterClose_ReallocWindowStillOpen documents what the guard
// above does NOT fix, so nobody reads it as more than it is.
//
// allocStream re-arms released for the new lifetime, so a stale reference that
// re-enters Recv after the struct has been handed to another request is
// admitted and receives that request's events. Closing this needs the caller
// to present the lifetime it believes it holds — which Recv cannot infer, the
// receiver being the struct itself — and the send side has the same hole. The
// test asserts the current behaviour rather than the desired one; if it starts
// failing, the window was closed and this should become the guarantee.
func TestStream_RecvAfterClose_ReallocWindowStillOpen(t *testing.T) {
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	s := c.allocStream(4, 65535)
	s.id = 5
	c.streams[5] = s
	s.push(StreamEvent{Type: EventHeaders})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := s.Recv(ctx); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	s.mu.Lock()
	s.connDone = true
	s.mu.Unlock()
	_ = s.Close()

	s2 := c.allocStream(4, 65535)
	if s2 != s {
		t.Skip("the pool did not hand back the same struct")
	}
	s2.id = 7
	s2.push(StreamEvent{Type: EventData}) // the second request's event

	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	ev, err := s.Recv(rctx) // the STALE reference, from the first request
	if err != nil {
		t.Fatalf("stale Recv = %v; the realloc window is closed — make this the guarantee", err)
	}
	if ev.Type != EventData {
		t.Fatalf("stale Recv = %v, want the second request's data (documenting the open window)", ev.Type)
	}
	t.Log("known gap: a stale reference re-entering Recv after re-allocation receives the next request's events")
}
