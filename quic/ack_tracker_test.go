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
