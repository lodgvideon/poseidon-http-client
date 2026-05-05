package streamstate

import (
	"errors"
	"testing"
)

func TestTransition_Table(t *testing.T) {
	cases := []struct {
		from State
		ev   Event
		want State
		err  bool
	}{
		// Idle
		{Idle, EventSendHeaders, Open, false},
		{Idle, EventRecvHeaders, Open, false},
		{Idle, EventRecvPushPromise, ReservedRemote, false},
		{Idle, EventSendEndStream, Idle, true},
		{Idle, EventRecvRSTStream, Idle, true},

		// Open
		{Open, EventSendEndStream, HalfClosedLocal, false},
		{Open, EventRecvEndStream, HalfClosedRemote, false},
		{Open, EventSendRSTStream, Closed, false},
		{Open, EventRecvRSTStream, Closed, false},
		{Open, EventSendHeaders, Open, false},
		{Open, EventRecvHeaders, Open, false},

		// HalfClosedLocal
		{HalfClosedLocal, EventRecvEndStream, Closed, false},
		{HalfClosedLocal, EventRecvRSTStream, Closed, false},
		{HalfClosedLocal, EventSendRSTStream, Closed, false},
		{HalfClosedLocal, EventRecvHeaders, HalfClosedLocal, false},
		{HalfClosedLocal, EventSendEndStream, HalfClosedLocal, true},

		// HalfClosedRemote
		{HalfClosedRemote, EventSendEndStream, Closed, false},
		{HalfClosedRemote, EventRecvRSTStream, Closed, false},
		{HalfClosedRemote, EventSendRSTStream, Closed, false},
		{HalfClosedRemote, EventSendHeaders, HalfClosedRemote, false},
		{HalfClosedRemote, EventRecvEndStream, HalfClosedRemote, true},

		// ReservedLocal
		{ReservedLocal, EventSendHeaders, HalfClosedRemote, false},
		{ReservedLocal, EventSendRSTStream, Closed, false},
		{ReservedLocal, EventRecvRSTStream, Closed, false},

		// ReservedRemote
		{ReservedRemote, EventRecvHeaders, HalfClosedLocal, false},
		{ReservedRemote, EventSendRSTStream, Closed, false},
		{ReservedRemote, EventRecvRSTStream, Closed, false},

		// Closed
		{Closed, EventSendHeaders, Closed, true},
		{Closed, EventRecvHeaders, Closed, true},
		{Closed, EventRecvEndStream, Closed, true},
	}
	for _, tc := range cases {
		got, err := Transition(tc.from, tc.ev)
		if tc.err {
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("(%v,%v) want ErrInvalidTransition, got (%v, %v)", tc.from, tc.ev, got, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("(%v,%v) unexpected err: %v", tc.from, tc.ev, err)
		}
		if got != tc.want {
			t.Fatalf("(%v,%v) = %v, want %v", tc.from, tc.ev, got, tc.want)
		}
	}
}
