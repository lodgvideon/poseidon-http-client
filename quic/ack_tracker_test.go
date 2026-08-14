package quic

import (
	"reflect"
	"testing"
)

// ackDecoder reconstructs the set of acknowledged packet numbers from a parsed
// ACK frame, mirroring the RFC 9000 §19.3 decode.
type ackDecoder struct {
	nopFrameHandler
	acked  map[uint64]bool
	prevLo uint64
}

func (d *ackDecoder) OnAck(largest, _, first uint64) error {
	lo := largest - first
	for pn := lo; pn <= largest; pn++ {
		d.acked[pn] = true
	}
	d.prevLo = lo
	return nil
}

func (d *ackDecoder) OnAckRange(gap, length uint64) error {
	largest := d.prevLo - gap - 2
	lo := largest - length
	for pn := lo; pn <= largest; pn++ {
		d.acked[pn] = true
	}
	d.prevLo = lo
	return nil
}

func ackedSet(t *testing.T, pns []uint64) map[uint64]bool {
	t.Helper()
	var a ackTracker
	for _, pn := range pns {
		a.receive(pn, true)
	}
	buf := a.appendACK(nil, 0)
	dec := &ackDecoder{acked: map[uint64]bool{}}
	if err := ParseFrames(buf, dec); err != nil {
		t.Fatalf("ParseFrames(%x): %v", buf, err)
	}
	return dec.acked
}

// TestAckTracker_RoundTrip receives assorted (including out-of-order and
// duplicate) packet numbers, builds the ACK frame, and checks it decodes back to
// exactly the received set.
func TestAckTracker_RoundTrip(t *testing.T) {
	cases := [][]uint64{
		{0, 1, 2},               // one contiguous range
		{5},                     // single
		{0, 1, 3, 4},            // two ranges, gap at 2
		{2, 0, 1},               // out of order → merges to [0,2]
		{0, 2, 4, 6, 8},         // five singleton ranges
		{10, 11, 12, 0, 1, 5},   // three ranges, arbitrary order
		{100, 101, 102, 100, 1}, // duplicate 100
	}
	for _, pns := range cases {
		want := map[uint64]bool{}
		for _, pn := range pns {
			want[pn] = true
		}
		got := ackedSet(t, pns)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("receive %v → acked %v, want %v", pns, got, want)
		}
	}
}

// TestAckTracker_BoundedRanges: a peer that never fills the gaps (every other
// packet number) must not make the tracker retain unbounded ranges or build an
// ACK frame too large for one datagram. The largest received is always kept.
func TestAckTracker_BoundedRanges(t *testing.T) {
	var a ackTracker
	const n = 500
	for i := uint64(0); i < n; i++ {
		a.receive(i*2, true) // 0,2,4,… — every PN its own range, gaps never fill
	}
	if len(a.ranges) > maxAckRanges {
		t.Fatalf("ranges = %d, want <= %d", len(a.ranges), maxAckRanges)
	}
	if l, ok := a.largest(); !ok || l != (n-1)*2 {
		t.Fatalf("largest = %d,%v, want %d,true", l, ok, (n-1)*2)
	}
	buf := a.appendACK(nil, 0)
	if len(buf) > 1200 {
		t.Fatalf("ACK frame is %d bytes, must fit one datagram", len(buf))
	}
	dec := &ackDecoder{acked: map[uint64]bool{}}
	if err := ParseFrames(buf, dec); err != nil {
		t.Fatalf("ParseFrames(%x): %v", buf, err)
	}
	if !dec.acked[(n-1)*2] {
		t.Fatal("ACK frame must acknowledge the largest received packet")
	}
}

// TestAckTracker_SeenSpansWholeRange pins that seen() decides membership of a
// RANGE, not of its endpoints. Every §12.3 replay arm in
// conformance_recvpath_test.go replays a packet number that is its own singleton
// range, where lo == hi and the two are indistinguishable, so judging membership by
// either endpoint alone — `pn == r.lo` or `pn == r.hi` in place of `pn >= r.lo &&
// pn <= r.hi` — left the whole quic suite green while every interior number of a
// contiguous run read as new and its packet was processed a second time. A
// contiguous run is the ordinary shape on the wire: an unreordered flight fills one
// range, and the number an attacker replays out of it is usually not an end of it.
//
// Dropping one of the two bounds instead (`pn >= r.lo` alone, `pn <= r.hi` alone) is
// already caught by TestConformance_RFC9000_Sec123_ReorderedPacketNotDiscarded,
// whose gap-inside-the-retained-window assertion those over-discard. So is the
// truncation floor below the retained window, which is why nothing here truncates —
// asserted, so that every false below means "provably new", never "undecidable".
//
// The identical containment test in receive() is NOT under-pinned the same way and
// is deliberately not duplicated here: mutating it to either endpoint alone breaks
// insert()'s no-overlap precondition, which TestAckTracker_OrderedInsertMatchesOracle_Random
// and TestAckTracker_InsertKeepsInvariant both catch (verified in both directions).
func TestAckTracker_SeenSpansWholeRange(t *testing.T) {
	var a ackTracker
	// 4..8 arrives out of order so insert's merge cases build the run rather than a
	// single append; 12,13 is a two-wide range and 20 the singleton the arms use.
	for _, pn := range []uint64{6, 7, 5, 8, 4, 13, 12, 20} {
		a.receive(pn, true)
	}
	if a.truncated {
		t.Fatal("the fixture must not truncate: every want=false below is meant to be provably new")
	}
	if got, want := len(a.ranges), 3; got != want {
		t.Fatalf("ranges = %v, want %d ranges ([20,20] [12,13] [4,8])", a.ranges, want)
	}
	for pn, want := range map[uint64]bool{
		3:  false, // below every range, and nothing was discarded: provably new
		4:  true,  // lower endpoint of the run
		5:  true,  // interior
		6:  true,  // interior
		7:  true,  // interior
		8:  true,  // upper endpoint of the run
		9:  false, // gap above the run
		11: false, // gap below the next range
		12: true,  // lower endpoint of the two-wide range
		13: true,  // upper endpoint of the two-wide range
		14: false, // gap
		19: false, // gap
		20: true,  // the singleton
		21: false, // above the largest received
	} {
		if got := a.seen(pn); got != want {
			t.Errorf("seen(%d) = %v, want %v (ranges %v)", pn, got, want, a.ranges)
		}
	}
}

func TestAckTracker_PendingAndLargest(t *testing.T) {
	var a ackTracker
	if _, ok := a.largest(); ok || a.ackPending() {
		t.Fatal("empty tracker should have no largest and no pending ack")
	}
	a.receive(3, false) // non-ack-eliciting: recorded but owes no ACK
	if a.ackPending() {
		t.Fatal("non-ack-eliciting packet should not make an ACK pending")
	}
	if l, ok := a.largest(); !ok || l != 3 {
		t.Fatalf("largest = %d,%v, want 3,true", l, ok)
	}
	a.receive(5, true) // ack-eliciting
	if !a.ackPending() {
		t.Fatal("ack-eliciting packet should make an ACK pending")
	}
	if l, _ := a.largest(); l != 5 {
		t.Fatalf("largest = %d, want 5", l)
	}
	_ = a.appendACK(nil, 0)
	if a.ackPending() {
		t.Fatal("appendACK should clear the pending flag")
	}
}
