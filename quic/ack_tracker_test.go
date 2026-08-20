package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoErrorf(t, ParseFrames(buf, dec), "ParseFrames(%x)", buf)
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

		assert.Equalf(t, want, got, "receive %v → acked %v, want %v", pns, got, want)
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
	buf := a.appendACK(nil, 0)

	require.LessOrEqualf(t, len(a.ranges), maxAckRanges,
		"ranges = %d, want <= %d", len(a.ranges), maxAckRanges)
	l, ok := a.largest()
	require.Truef(t, ok, "largest = %d,%v, want %d,true", l, ok, uint64((n-1)*2))
	assert.Equalf(t, uint64((n-1)*2), l, "largest = %d,%v, want %d,true", l, ok, uint64((n-1)*2))
	require.LessOrEqualf(t, len(buf), 1200, "ACK frame is %d bytes, must fit one datagram", len(buf))
	dec := &ackDecoder{acked: map[uint64]bool{}}
	require.NoErrorf(t, ParseFrames(buf, dec), "ParseFrames(%x)", buf)
	assert.True(t, dec.acked[(n-1)*2], "ACK frame must acknowledge the largest received packet")
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

	require.False(t, a.truncated,
		"the fixture must not truncate: every want=false below is meant to be provably new")
	require.Lenf(t, a.ranges, 3, "ranges = %v, want %d ranges ([20,20] [12,13] [4,8])", a.ranges, 3)

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
		assert.Equalf(t, want, a.seen(pn), "seen(%d) = %v, want %v (ranges %v)",
			pn, a.seen(pn), want, a.ranges)
	}
}

func TestAckTracker_PendingAndLargest(t *testing.T) {
	var a ackTracker

	t.Run("empty", func(t *testing.T) {
		_, ok := a.largest()

		require.Falsef(t, ok || a.ackPending(),
			"empty tracker should have no largest and no pending ack")
	})

	t.Run("non-ack-eliciting", func(t *testing.T) {
		a.receive(3, false) // recorded but owes no ACK

		l, ok := a.largest()

		assert.False(t, a.ackPending(), "non-ack-eliciting packet should not make an ACK pending")
		require.Truef(t, ok, "largest = %d,%v, want 3,true", l, ok)
		assert.Equalf(t, uint64(3), l, "largest = %d,%v, want 3,true", l, ok)
	})

	t.Run("ack-eliciting", func(t *testing.T) {
		a.receive(5, true)

		l, _ := a.largest()

		assert.True(t, a.ackPending(), "ack-eliciting packet should make an ACK pending")
		assert.Equalf(t, uint64(5), l, "largest = %d, want 5", l)
	})

	t.Run("appendACK-clears-pending", func(t *testing.T) {
		_ = a.appendACK(nil, 0)

		assert.False(t, a.ackPending(), "appendACK should clear the pending flag")
	})
}
