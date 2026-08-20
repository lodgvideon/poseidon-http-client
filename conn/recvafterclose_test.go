package conn

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	ev, err := ref.Recv(ctx)
	require.NoError(t, err, "first Recv")
	require.Equal(t, EventHeaders, ev.Type, "first Recv delivered the wrong event")

	// The reader is now between calls. Close lands in the gap and, with
	// connDone already set, pools the struct immediately.
	s.mu.Lock()
	s.connDone = true
	s.mu.Unlock()
	_ = ref.Close()

	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer rcancel()
	start := time.Now()
	_, rerr := ref.Recv(rctx)
	elapsed := time.Since(start)
	s.mu.Lock()
	ra := s.recvActive
	s.mu.Unlock()

	// The recycle retired this lifetime, so the handle is stale rather than
	// merely closed. Either way the point of this test stands: it refuses
	// immediately instead of registering and parking.
	assert.Truef(t, errors.Is(rerr, ErrStaleStream) || errors.Is(rerr, ErrStreamClosed),
		"Recv after Close = %v, want ErrStaleStream or ErrStreamClosed", rerr)
	assert.Lessf(t, elapsed, time.Second,
		"Recv after Close took %v — it parked on the orphaned channel instead of refusing", elapsed)
	assert.Zerof(t, ra,
		"recvActive = %d on a pooled struct; the next request to claim it would never be recycled", ra)
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
	_, err := refA.Recv(ctx)
	require.NoError(t, err, "first Recv")
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
	_, staleErr := refA.Recv(rctx)

	assert.Truef(t, errors.Is(staleErr, ErrStaleStream),
		"stale Recv = %v, want ErrStaleStream — a handle from the finished request must not "+
			"reach the events of whichever request claimed the struct next", staleErr)
}
