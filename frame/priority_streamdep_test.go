package frame

import (
	"context"
	"testing"
)

// RFC 7540 §6.3 gives PRIORITY a 1-bit E flag and a 31-bit Stream Dependency in
// the same 32-bit word. Every other 31-bit field this framer writes is masked
// before it goes on the wire — the promised id in PUSH_PROMISE, the last-stream-id
// in GOAWAY, the increment in WINDOW_UPDATE, the frame header's own stream id.
// The two priority-write sites were not: they OR'd in the E bit and wrote
// StreamDep whole.
//
// So a StreamDep with its high bit set — out of range for a 31-bit field, and
// reachable because Priority.StreamDep is a public uint32 — set E on the wire
// whichever way Exclusive was. The read side masks correctly, so a round trip
// silently returned a different Priority than it was given (#517).

// TestFramer_Priority_HighBitStreamDep_DoesNotForgeExclusive drives both write
// paths — WritePriority, and the priority section HEADERS carries when
// FlagHeadersPriority is set — because the same unmasked expression was written
// out twice and a fix applied to one is a fix applied to neither.
func TestFramer_Priority_HighBitStreamDep_DoesNotForgeExclusive(t *testing.T) {
	// The high bit is the E flag's position. A caller that puts it in StreamDep
	// is out of range; what must not happen is it being read back as Exclusive.
	const outOfRangeDep = 0x80000001

	t.Run("WritePriority", func(t *testing.T) {
		fr, _ := newFramerWithBuffer()
		if err := fr.WritePriority(1, Priority{StreamDep: outOfRangeDep, Exclusive: false, Weight: 32}); err != nil {
			t.Fatalf("write: %v", err)
		}
		h := &recordingHandler{}
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			t.Fatalf("read: %v", err)
		}
		if h.priorityVal.Exclusive {
			t.Errorf("Exclusive came back true for a frame written with Exclusive=false — "+
				"the high bit of StreamDep (%#x) was written into the E flag's position",
				uint32(outOfRangeDep))
		}
		if got := h.priorityVal.StreamDep; got != outOfRangeDep&0x7fffffff {
			t.Errorf("StreamDep = %#x, want %#x (masked to 31 bits)", got, uint32(outOfRangeDep&0x7fffffff))
		}
	})

	t.Run("HEADERS priority section", func(t *testing.T) {
		fr, _ := newFramerWithBuffer()
		p := Priority{StreamDep: outOfRangeDep, Exclusive: false, Weight: 32}
		if err := fr.WriteHeaders(WriteHeadersParams{
			StreamID:      1,
			EndHeaders:    true,
			Priority:      &p,
			BlockFragment: []byte{0x82},
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
		h := &recordingHandler{}
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			t.Fatalf("read: %v", err)
		}
		// OnHeaders records into h.prio; priorityVal is only set by OnPriority.
		// Reading the wrong field here made this subtest fail against a zero
		// Priority — a failure that looked like evidence and was not.
		if h.prio == nil {
			t.Fatal("the HEADERS frame carried no priority section")
		}
		if h.prio.Exclusive {
			t.Errorf("Exclusive came back true for a HEADERS priority section written with " +
				"Exclusive=false — the high bit of StreamDep was written into the E flag")
		}
		if got := h.prio.StreamDep; got != outOfRangeDep&0x7fffffff {
			t.Errorf("StreamDep = %#x, want %#x (masked to 31 bits)", got, uint32(outOfRangeDep&0x7fffffff))
		}
	})
}

// TestFramer_Priority_ExclusiveStillRoundTrips is the over-correction guard:
// masking StreamDep must not cost the E flag its meaning when the caller really
// did ask for it.
func TestFramer_Priority_ExclusiveStillRoundTrips(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WritePriority(1, Priority{StreamDep: 9, Exclusive: true, Weight: 7}); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !h.priorityVal.Exclusive {
		t.Error("Exclusive was dropped: a masked StreamDep must not clear the E flag")
	}
	if h.priorityVal.StreamDep != 9 || h.priorityVal.Weight != 7 {
		t.Errorf("prio = %+v, want StreamDep 9 weight 7", h.priorityVal)
	}
}
