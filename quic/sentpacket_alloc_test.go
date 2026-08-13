//go:build !race

package quic

import (
	"testing"
	"time"
)

// TestOnSent_SteadyStateDoesNotAllocate is the behavioural half of the guard in
// sentpacket_size_test.go. That one pins a number; this one pins the consequence,
// so the win survives a change that keeps the struct small but reintroduces an
// allocation some other way.
//
// The map is pre-filled and the loop deletes as it inserts, so the window stays
// at inFlight entries and nothing here measures map growth — Go maps never shrink,
// so after the warm-up the table is stable and an insert either reuses a slot or
// allocates because the element is too big to live in one. rf is hoisted out of
// the loop for the same reason: &retransFrame{...} inside it would allocate the
// frame and mask what is being measured.
func TestOnSent_SteadyStateDoesNotAllocate(t *testing.T) {
	const inFlight = 64

	s := &sentSpace{}
	rf := retransFrame{kind: retransStream, streamID: 4, offset: 128, data: make([]byte, 64)}
	now := time.Unix(1700000000, 0)

	var pn uint64
	for ; pn < inFlight; pn++ {
		s.onSent(pn, now, true, &rf)
	}

	got := testing.AllocsPerRun(2000, func() {
		s.onSent(pn, now, true, &rf)
		delete(s.packets, pn-inFlight)
		pn++
	})

	if len(s.packets) != inFlight {
		t.Fatalf("window drifted to %d, want %d — the loop is not in steady state, "+
			"so this measured growth rather than insertion", len(s.packets), inFlight)
	}
	if got != 0 {
		t.Errorf("onSent allocates %.2f objects per sent packet, want 0.\n"+
			"sizeof(sentPacket) crossing 128 bytes is the usual cause; see "+
			"TestSentPacketStaysInlineInMap.", got)
	}
}

// TestOnSent_NilFrameDoesNotAllocate covers the other arm: packets carrying no
// retransmittable frame take a different branch in onSent.
func TestOnSent_NilFrameDoesNotAllocate(t *testing.T) {
	const inFlight = 64

	s := &sentSpace{}
	now := time.Unix(1700000000, 0)

	var pn uint64
	for ; pn < inFlight; pn++ {
		s.onSent(pn, now, false, nil)
	}

	got := testing.AllocsPerRun(2000, func() {
		s.onSent(pn, now, false, nil)
		delete(s.packets, pn-inFlight)
		pn++
	})

	if len(s.packets) != inFlight {
		t.Fatalf("window drifted to %d, want %d", len(s.packets), inFlight)
	}
	if got != 0 {
		t.Errorf("onSent(nil frame) allocates %.2f objects per sent packet, want 0", got)
	}
}
