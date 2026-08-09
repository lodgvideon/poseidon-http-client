package conn

import (
	"context"
	"errors"
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
	ref := s.ref()
	s.push(StreamEvent{Type: EventHeaders})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if ev, err := ref.Recv(ctx); err != nil || ev.Type != EventHeaders {
		t.Fatalf("first Recv = %v, %v", ev.Type, err)
	}

	// The reader is now between calls. Close lands in the gap and, with
	// connDone already set, pools the struct immediately.
	s.mu.Lock()
	s.connDone = true
	s.mu.Unlock()
	_ = ref.Close()

	start := time.Now()
	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer rcancel()
	// The recycle retired this lifetime, so the handle is stale rather than
	// merely closed. Either way the point of this test stands: it refuses
	// immediately instead of registering and parking.
	if _, err := ref.Recv(rctx); !errors.Is(err, ErrStaleStream) && !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("Recv after Close = %v, want ErrStaleStream or ErrStreamClosed", err)
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

// TestStream_RecvAfterClose_ReallocIsRefused is the guarantee the previous
// version of this test asked for.
//
// It used to assert the opposite — that a stale reference re-entering Recv
// after the struct had been handed to another request received THAT request's
// events — and its own comment said: "if it starts failing, the window was
// closed and this should become the guarantee". It is closed, so it is.
//
// It also used to t.Skip when sync.Pool handed back a different struct, which
// meant the guard could ship with its own regression test never having run. It
// no longer needs the pool to cooperate: the lifetime is retired by the recycle
// itself, so a handle from the finished request is refused whether or not the
// struct is claimed again.
func TestStream_RecvAfterClose_ReallocIsRefused(t *testing.T) {
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	s := c.allocStream(4, 65535)
	s.id = 5
	c.streams[5] = s
	refA := s.ref() // the handle the first request holds
	s.push(StreamEvent{Type: EventHeaders})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := refA.Recv(ctx); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	s.mu.Lock()
	s.connDone = true
	s.mu.Unlock()
	_ = refA.Close()

	// Stage the re-allocation that used to leak the next request's events. If
	// the pool hands back this very struct, the stale handle is now pointed
	// straight at request B's live stream — which is the case that mattered.
	s2 := c.allocStream(4, 65535)
	s2.id = 7
	c.streams[7] = s2
	s2.push(StreamEvent{Type: EventData}) // the second request's event

	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	if _, err := refA.Recv(rctx); !errors.Is(err, ErrStaleStream) {
		t.Fatalf("stale Recv = %v, want ErrStaleStream", err)
	}
}
