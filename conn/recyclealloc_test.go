package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"

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

	got := testing.AllocsPerRun(50, func() {
		s.mu.Lock()
		s.resetForPoolLocked()
		s.mu.Unlock()
	})
	if got != 0 {
		t.Fatalf("resetForPoolLocked allocates %.0f times per recycle on a healthy stream", got)
	}
	if s.events != before {
		t.Fatal("the event channel was replaced although nothing closed it")
	}
	// The drain still happened: the pushed event must not survive into the
	// next lifetime.
	if n := len(s.events); n != 0 {
		t.Fatalf("%d event(s) survived the recycle", n)
	}
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

	if s.events == before {
		t.Fatal("a closed channel was carried into the next lifetime")
	}
	if s.eventsClosed.Load() {
		t.Fatal("eventsClosed was not re-armed for the new lifetime")
	}
	// Usable again: a send must neither block nor panic.
	select {
	case s.events <- StreamEvent{Type: EventHeaders}:
	default:
		t.Fatal("the replacement channel would not accept an event")
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
	if s.resetSignal != reset || s.endSignal != end {
		t.Fatal("an unclosed signal channel was replaced")
	}

	s.signalReset(frame.ErrCodeCancel)
	s.signalEnd()
	s.mu.Lock()
	s.resetForPoolLocked()
	s.mu.Unlock()
	if s.resetSignal == reset {
		t.Fatal("a closed resetSignal was carried into the next lifetime")
	}
	if s.endSignal == end {
		t.Fatal("a closed endSignal was carried into the next lifetime")
	}
	if s.resetSignalled.Load() || s.endSignalled.Load() {
		t.Fatal("the signal flags were not re-armed")
	}
	if code := frame.ErrCode(s.resetCode.Load()); code != 0 {
		t.Fatalf("resetCode = %v, want cleared", code)
	}
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

	if err := s.ref().SendData(context.Background(), nil, true); err != nil {
		t.Fatalf("SendData(END_STREAM): %v", err)
	}
	if err := s.ref().Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The struct is pooled now. A receive on a closed empty channel completes
	// at once with ok=false; on an open one it blocks, so default fires.
	select {
	case _, ok := <-s.events:
		if !ok {
			t.Fatal("the recycled struct carries a closed events channel: the next request's first Recv would report ErrStreamClosed before anything was sent")
		}
		t.Fatal("a dead lifetime's event survived the recycle")
	default:
	}

	// And the next lifetime can actually be delivered to. A send on a closed
	// channel panics, and this runs on the connection reader goroutine.
	s.id = 7
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("delivering the next request's response panicked: %v", r)
		}
	}()
	if !s.push(StreamEvent{Type: EventHeaders}) {
		t.Fatal("push into the next lifetime failed")
	}
}
