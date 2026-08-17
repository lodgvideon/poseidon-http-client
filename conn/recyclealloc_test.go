package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestRecycleStream_ReusesTheEventChannel pins that a healthy stream costs
// nothing to recycle.
//
// The channel used to be replaced on every recycle, in the path whose whole
// purpose is to avoid allocating. At grpc's default of 272 slots that is 24 KiB
// per RPC — the single largest allocation the gRPC client made — and the
// justification, orphaning a stale reference from the previous lifetime, does
// not hold: no writer in this package captures the channel.
func TestRecycleStream_ReusesTheEventChannel(t *testing.T) {
	s := newStream(1, 272, nil, 65535)
	before := s.events
	s.push(StreamEvent{Type: EventHeaders})

	// No testify inside the measured closure: it reflects and allocates, and
	// AllocsPerRun counts the whole process. Every assertion is outside it.
	got := testing.AllocsPerRun(50, func() {
		s.mu.Lock()
		s.resetForPoolLocked()
		s.mu.Unlock()
	})

	require.Zerof(t, got,
		"resetForPoolLocked allocates %.0f times per recycle on a healthy stream", got)
	assert.True(t, s.events == before, "the event channel was replaced although nothing closed it")
	// The drain still happened: the pushed event must not survive into the
	// next lifetime.
	assert.Emptyf(t, s.events, "%d event(s) survived the recycle", len(s.events))
}

// TestRecycleStream_ReplacesAChannelShutdownClosed is the other half, and the
// reason a blanket reuse is unsafe. shutdownStreams is the only site that
// closes s.events, and a closed channel survives a drain — len and receive both
// keep working — so a struct pooled without repair hands the next request a
// stream whose first Recv reports ErrStreamClosed and whose first delivery
// panics the connection reader.
func TestRecycleStream_ReplacesAChannelShutdownClosed(t *testing.T) {
	s := newStream(1, 4, nil, 65535)
	before := s.events
	s.eventsClosed.Store(true)
	close(s.events)

	s.mu.Lock()
	s.resetForPoolLocked()
	s.mu.Unlock()

	require.False(t, s.events == before, "a closed channel was carried into the next lifetime")
	assert.False(t, s.eventsClosed.Load(), "eventsClosed was not re-armed for the new lifetime")
	// Usable again: a send must neither block nor panic.
	select {
	case s.events <- StreamEvent{Type: EventHeaders}:
	default:
		assert.Fail(t, "the replacement channel would not accept an event")
	}
}

// TestRecycleStream_ReplacesSignalChannelsOnlyWhenClosed applies the same rule
// to the two signal channels. Their existing "was it closed" flags are exactly
// the test, so no new state is needed.
func TestRecycleStream_ReplacesSignalChannelsOnlyWhenClosed(t *testing.T) {
	s := newStream(1, 4, nil, 65535)
	reset, end := s.resetSignal, s.endSignal

	s.mu.Lock()
	s.resetForPoolLocked()
	s.mu.Unlock()

	assert.True(t, s.resetSignal == reset, "an unclosed resetSignal was replaced")
	assert.True(t, s.endSignal == end, "an unclosed endSignal was replaced")

	s.signalReset(frame.ErrCodeCancel)
	s.signalEnd()
	s.mu.Lock()
	s.resetForPoolLocked()
	s.mu.Unlock()

	assert.False(t, s.resetSignal == reset, "a closed resetSignal was carried into the next lifetime")
	assert.False(t, s.endSignal == end, "a closed endSignal was carried into the next lifetime")
	assert.False(t, s.resetSignalled.Load(), "the reset signal flag was not re-armed")
	assert.False(t, s.endSignalled.Load(), "the end signal flag was not re-armed")
	assert.Equalf(t, frame.ErrCode(0), frame.ErrCode(s.resetCode.Load()),
		"resetCode = %v, want cleared", frame.ErrCode(s.resetCode.Load()))
}

// TestRecycleStream_SurvivesShutdownThenReuse drives the real path end to end,
// which the flag-setting tests above cannot: it is what makes shutdownStreams'
// record of the close load-bearing rather than decorative. Removing that record
// leaves the unit tests green and poisons the pool.
//
// The state is the ordinary RFC 9113 §8.1 shape, not a contrivance. The server
// answered in full — remoteEnded — before the client half-closed its request,
// so localEnded is still false, markStreamDone has not evicted the stream, and
// shutdownStreams still finds it when the reader dies. grpc.Stream.CloseSend
// then issues exactly the empty END_STREAM DATA used here, and that write path
// checks only c.closed, not readerGone, so it still succeeds on a connection
// whose reader has just gone.
func TestRecycleStream_SurvivesShutdownThenReuse(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.fr = frame.NewFramer(&buf, nil) // writer first

	s := newStream(5, 8, c, 65535)
	s.remoteEnded = true // the reader already delivered the response's END_STREAM
	c.streams[5] = s
	c.inflight = 1

	c.shutdownStreams() // reader dies: event queued, channel closed

	require.NoError(t, s.ref().SendData(context.Background(), nil, true), "SendData(END_STREAM)")
	require.NoError(t, s.ref().Close(), "Close")

	// The struct is pooled now. A receive on a closed empty channel completes
	// at once with ok=false; on an open one it blocks, so default fires.
	select {
	case _, ok := <-s.events:
		require.Truef(t, ok,
			"the recycled struct carries a closed events channel: the next request's first "+
				"Recv would report ErrStreamClosed before anything was sent")
		require.FailNow(t, "a dead lifetime's event survived the recycle")
	default:
	}

	// And the next lifetime can actually be delivered to. A send on a closed
	// channel panics, and this runs on the connection reader goroutine.
	s.id = 7
	require.NotPanicsf(t, func() {
		assert.True(t, s.push(StreamEvent{Type: EventHeaders}), "push into the next lifetime failed")
	}, "delivering the next request's response panicked")
}
