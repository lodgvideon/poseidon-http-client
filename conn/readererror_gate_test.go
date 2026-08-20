package conn

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestReaderStreamError_IDGateRefusesRecycledStruct is the regression test for
// the readerLoop *StreamError arm.
//
// A *Stream is pooled. Between the reader's lookupStream and its delivery, the
// application can finish the request, Close it, and a fresh NewStream can claim
// the same struct. The old arm called push, which has no id gate, so the dead
// lifetime's EventReset landed in the NEXT request's channel — a reset that
// request was never sent. endWithReset re-checks s.id under s.mu and refuses.
//
// The recycle is simulated directly (assign a new id) rather than raced, so the
// property is pinned deterministically instead of depending on a window. Both
// directions are asserted, in two subtests: a gate that refused unconditionally
// would satisfy the first alone.
func TestReaderStreamError_IDGateRefusesRecycledStruct(t *testing.T) {
	t.Run("a struct recycled before delivery is refused", func(t *testing.T) {
		var wire bytes.Buffer
		c := &Conn{fr: frame.NewFramer(&wire, nil)} // writer first, then reader
		c.fcOutCond = sync.NewCond(&c.fcOutMu)      // the live path broadcasts on it
		s := &Stream{id: 5, events: make(chan StreamEvent, 4), resetSignal: make(chan struct{})}
		// The map still points at the struct — that IS the interleaving: the reader's
		// lookup found it, and it was recycled into a new request before the
		// delivery. The recycled lifetime carries a different id.
		c.streams = map[uint32]*Stream{5: s}
		s.id = 7

		c.resetStreamOnError(&StreamError{StreamID: 5, Code: frame.ErrCodeFlowControlError})

		select {
		case ev := <-s.events:
			assert.Failf(t, "a dead lifetime's reset leaked into the next request",
				"the next request received %+v — a reset it was never sent", ev)
		default:
		}
		assert.False(t, s.closed, "the recycled stream was marked closed by the previous lifetime's reset")
		assert.False(t, s.remoteEnded, "the recycled stream was marked remote-ended by the previous lifetime's reset")
		assert.False(t, s.localEnded, "the recycled stream was marked local-ended by the previous lifetime's reset")
		// The peer is still told, because the id it named did misbehave.
		assert.Equal(t, 13, wire.Len(), "want 13 bytes: one RST_STREAM for the offending id")
	})

	t.Run("the live id is delivered to", func(t *testing.T) {
		var wire bytes.Buffer
		c := &Conn{fr: frame.NewFramer(&wire, nil)}
		c.fcOutCond = sync.NewCond(&c.fcOutMu)
		s := &Stream{id: 7, events: make(chan StreamEvent, 4), resetSignal: make(chan struct{})}
		c.streams = map[uint32]*Stream{7: s}

		c.resetStreamOnError(&StreamError{StreamID: 7, Code: frame.ErrCodeFlowControlError})

		select {
		case ev := <-s.events:
			assert.Equalf(t, EventReset, ev.Type, "delivered %+v, want EventReset", ev)
			assert.Equalf(t, frame.ErrCodeFlowControlError, ev.RSTCode,
				"delivered %+v, want FLOW_CONTROL_ERROR", ev)
			assert.Truef(t, ev.EndStream, "delivered %+v, want EndStream set", ev)
		default:
			assert.Fail(t, "the live stream received nothing")
		}
		assert.True(t, s.closed, "terminal flag closed not set on the reset stream")
		assert.True(t, s.remoteEnded, "terminal flag remoteEnded not set on the reset stream")
		assert.True(t, s.localEnded, "terminal flag localEnded not set on the reset stream")
	})
}

// TestReaderStreamError_ClosedStreamNeedsSendWake pins why the arm broadcasts
// after endWithReset.
//
// endWithReset sets s.closed, and acquireSendCredits re-checks that flag only at
// the top of its loop — which it reaches only via a cond wake. Neither
// endWithReset nor rstStream broadcasts, so without the explicit wake a writer
// already parked on flow-control credit sleeps through the reset and wakes only
// when its own context expires, then writes DATA onto a stream the peer has
// reset (RFC 9113 §6.4). OnRSTStream calls wakeSendWaiters after its own
// endWithReset for exactly this reason.
//
// This is a synchronisation property, so the control arm is built into the test
// rather than left to a mutation: the middle block sets the flag WITHOUT any
// wake and asserts nothing moves, which is what makes the final block's release
// attributable to the broadcast and not to the flag.
func TestReaderStreamError_ClosedStreamNeedsSendWake(t *testing.T) {
	var wire bytes.Buffer
	c := &Conn{fr: frame.NewFramer(&wire, nil)} // writer first, then reader
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.peerConnSendWindow = 0 // no connection credit: any writer must park

	s := &Stream{id: 3, events: make(chan StreamEvent, 4), resetSignal: make(chan struct{})}
	s.sendWindow = 0

	parked := make(chan error, 1)
	go func() {
		_, err := c.acquireSendCredits(t.Context(), s, s.gen.Load(), 16, 0)
		parked <- err
	}()

	// Let the writer reach cond.Wait.
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-parked:
		require.FailNowf(t, "writer did not park", "acquireSendCredits returned %v", err)
	default:
	}

	// Control arm: prove the flag alone cannot release the writer. Set it exactly
	// as resetStreamOnError's endWithReset would, and confirm nothing moves.
	s.endWithReset(3, frame.ErrCodeFlowControlError)
	select {
	case err := <-parked:
		require.FailNowf(t, "the flag alone released the writer",
			"writer returned %v before any wake, so the wake below proves nothing", err)
	case <-time.After(150 * time.Millisecond):
	}

	// Now run the real reader arm. It must broadcast, which is what releases
	// the writer — mutating that wake out of resetStreamOnError fails here.
	c.streams = map[uint32]*Stream{3: s}
	c.resetStreamOnError(&StreamError{StreamID: 3, Code: frame.ErrCodeFlowControlError})

	select {
	case err := <-parked:
		assert.Equalf(t, ErrStreamClosed, err, "parked writer returned %v, want ErrStreamClosed", err)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "the wake did not release the parked writer")
	}
	// Exactly one RST_STREAM reached the wire. The old arm let a cleanly
	// enqueued push leave the stream looking open, so the application's later
	// Close sent a second one — with CANCEL, misreporting the flow-control
	// error that actually killed the stream.
	require.Equal(t, 13, wire.Len(), "want 13 bytes: exactly one RST_STREAM frame")
	code := frame.ErrCode(binary.BigEndian.Uint32(wire.Bytes()[9:13]))
	assert.Equalf(t, frame.ErrCodeFlowControlError, code,
		"RST code = %v, want FLOW_CONTROL_ERROR — not the error that killed the stream", code)
}
