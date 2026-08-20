package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPushIfID_RefusesRecycledParent pins the id gate on PUSH_PROMISE delivery.
//
// handlePushPromiseBlock resolves the parent by id and then does real work
// before delivering — reserving the promised stream, taking a block from the
// pool, copying every header field. A *Stream is pooled, so in that window the
// application can finish the parent request, Close it, and a fresh NewStream can
// claim the struct. An ungated push then announces the promise to whichever
// request now owns it.
//
// The recycle is simulated by assigning a new id, so the property is pinned
// deterministically rather than by racing the window. Both directions are
// covered: a gate that always refused would satisfy the second subtest alone.
func TestPushIfID_RefusesRecycledParent(t *testing.T) {
	ev := StreamEvent{Type: EventPushPromise, PushStreamID: 4}

	t.Run("the live parent is delivered to", func(t *testing.T) {
		s := &Stream{id: 3, events: make(chan StreamEvent, 4), resetSignal: make(chan struct{})}

		ok := s.pushIfID(3, ev)

		require.True(t, ok, "pushIfID refused the live parent")
		select {
		case got := <-s.events:
			assert.Equal(t, EventPushPromise, got.Type, "the live parent received the wrong event type")
			assert.Equalf(t, uint32(4), got.PushStreamID,
				"delivered %+v, want the promise", got)
		default:
			assert.Fail(t, "the live parent received nothing")
		}
	})

	t.Run("a parent recycled before delivery is refused", func(t *testing.T) {
		s := &Stream{id: 3, events: make(chan StreamEvent, 4), resetSignal: make(chan struct{})}
		// Recycled and re-issued to another request between the lookup and here.
		s.id = 7

		ok := s.pushIfID(3, ev)

		assert.False(t, ok, "pushIfID delivered to a recycled struct; the gate did not hold")
		select {
		case got := <-s.events:
			assert.Failf(t, "a promise leaked into the next lifetime",
				"the next request was announced %+v — a promise it never asked for", got)
		default:
		}
		assert.False(t, s.closed, "the refused delivery closed the new lifetime's stream")
	})
}

// TestPushIfID_FullChannelStillResets pins that the gate does not swallow
// pushLocked's own overflow path: a live parent whose event channel is full must
// still be reset, exactly as an ungated push would.
//
// The reset arrives through resetSignal rather than as a buffered EventReset,
// and that is forced rather than incidental: the channel holds exactly one slot
// and it is already occupied, so pushLocked's second send hits its default arm
// too. The assertion here used to be
// `!s.resetSignalled.Load() && len(s.events) < cap(s.events)`, whose right half
// is false by construction on this fixture — the whole conjunction could never
// fire, so the reset half of this test's name was unasserted.
func TestPushIfID_FullChannelStillResets(t *testing.T) {
	s := &Stream{id: 3, events: make(chan StreamEvent, 1), resetSignal: make(chan struct{}), w: &fakeStreamWriter{}}
	s.events <- StreamEvent{Type: EventData} // fill it

	ok := s.pushIfID(3, StreamEvent{Type: EventPushPromise, PushStreamID: 4})

	assert.False(t, ok, "pushIfID reported success on a full channel")
	assert.True(t, s.closed, "overflow on a live parent did not close the stream")
	assert.True(t, s.resetSignalled.Load(),
		"neither an EventReset nor a reset signal reached the caller: with the one event slot "+
			"already taken, resetSignal is the only way the overflow can be reported")
}
