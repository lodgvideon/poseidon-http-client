package quic

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// oracleTracker is the pre-#345 implementation, kept verbatim as a differential
// oracle. The ordered insert that replaced it must agree with it on every input:
// the range set, the truncation floor, and the truncated flag.
type oracleTracker struct {
	ranges    []pnRange
	lowWater  uint64
	truncated bool
}

func (a *oracleTracker) receive(pn uint64) {
	for _, r := range a.ranges {
		if pn >= r.lo && pn <= r.hi {
			return
		}
	}
	a.ranges = append(a.ranges, pnRange{pn, pn})
	sort.Slice(a.ranges, func(i, j int) bool { return a.ranges[i].hi > a.ranges[j].hi })
	merged := a.ranges[:1]
	for _, r := range a.ranges[1:] {
		last := &merged[len(merged)-1]
		if r.hi+1 >= last.lo {
			if r.lo < last.lo {
				last.lo = r.lo
			}
		} else {
			merged = append(merged, r)
		}
	}
	if len(merged) > maxAckRanges {
		if lw := merged[maxAckRanges].hi + 1; !a.truncated || lw > a.lowWater {
			a.lowWater, a.truncated = lw, true
		}
		merged = merged[:maxAckRanges]
	}
	a.ranges = merged
}

func compareTrackers(t *testing.T, label string, got *ackTracker, want *oracleTracker, seq []uint64) {
	t.Helper()
	if !reflect.DeepEqual(got.ranges, want.ranges) {
		t.Fatalf("%s: ranges diverged\n got: %v\nwant: %v\nseq: %v", label, got.ranges, want.ranges, seq)
	}
	if got.truncated != want.truncated || got.lowWater != want.lowWater {
		t.Fatalf("%s: truncation diverged: got (lowWater=%d truncated=%v), want (lowWater=%d truncated=%v)\nseq: %v",
			label, got.lowWater, got.truncated, want.lowWater, want.truncated, seq)
	}
}

func runBoth(t *testing.T, label string, seq []uint64) {
	t.Helper()
	var got ackTracker
	var want oracleTracker
	for _, pn := range seq {
		got.receive(pn, true)
		want.receive(pn)
		compareTrackers(t, label, &got, &want, seq)
	}
}

// TestAckTracker_OrderedInsertMatchesOracle_Fixtures covers the shapes the
// rewrite could plausibly get wrong, each checked after EVERY packet rather than
// only at the end, so a transient divergence cannot cancel itself out.
func TestAckTracker_OrderedInsertMatchesOracle_Fixtures(t *testing.T) {
	cases := map[string][]uint64{
		"in order":                  {0, 1, 2, 3, 4, 5},
		"starts at zero then gap":   {0, 2, 4, 6},
		"fills a hole from below":   {0, 2, 1},
		"fills a hole from above":   {0, 2, 3, 1},
		"joins two ranges":          {0, 1, 3, 4, 2},
		"strictly descending":       {9, 8, 7, 6, 5},
		"duplicates":                {5, 5, 5, 4, 5, 6, 6},
		"reorder far below":         {100, 101, 102, 1, 2},
		"adjacent to zero":          {1, 0},
		"zero last":                 {3, 2, 1, 0},
		"single":                    {7},
		"wide gaps then bridge":     {0, 10, 20, 5, 15, 25},
		"pn zero after high ranges": {1000, 1001, 0},
	}
	for name, seq := range cases {
		t.Run(name, func(t *testing.T) { runBoth(t, name, seq) })
	}
}

// TestAckTracker_OrderedInsertMatchesOracle_Truncation drives well past
// maxAckRanges with permanent gaps, which is the only way the truncation floor
// and the truncated flag move.
func TestAckTracker_OrderedInsertMatchesOracle_Truncation(t *testing.T) {
	var seq []uint64
	for i := uint64(0); i < uint64(maxAckRanges)*4; i++ {
		seq = append(seq, i*2) // every other number: one range each, never merging
	}
	runBoth(t, "even-only", seq)

	// Then arrive below the floor, and fill some holes, so ranges merge while
	// truncation is already in effect.
	var got ackTracker
	var want oracleTracker
	for _, pn := range seq {
		got.receive(pn, true)
		want.receive(pn)
	}
	for _, pn := range []uint64{1, 3, 5, 7, 9, 200, 201, 0} {
		got.receive(pn, true)
		want.receive(pn)
		compareTrackers(t, "post-truncation", &got, &want, []uint64{pn})
	}
}

// TestAckTracker_OrderedInsertMatchesOracle_Random is the broad net: random
// packet numbers in a small window so merges, duplicates and reordering all
// happen densely, plus a wide window that exercises truncation.
func TestAckTracker_OrderedInsertMatchesOracle_Random(t *testing.T) {
	for _, span := range []uint64{8, 64, 512, 4096} {
		for seed := int64(0); seed < 40; seed++ {
			rng := rand.New(rand.NewSource(seed))
			seq := make([]uint64, 300)
			for i := range seq {
				seq[i] = uint64(rng.Int63n(int64(span)))
			}
			var got ackTracker
			var want oracleTracker
			for _, pn := range seq {
				got.receive(pn, true)
				want.receive(pn)
			}
			compareTrackers(t, "random", &got, &want, seq)
		}
	}
}

// TestAckTracker_InsertKeepsInvariant asserts the structural contract the
// ordered insert relies on for correctness — sorted descending by upper bound,
// non-overlapping, and non-ADJACENT. Adjacency is the subtle one: two ranges
// that touch must be one range, or a later insert between them merges the wrong
// pair.
func TestAckTracker_InsertKeepsInvariant(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	var a ackTracker
	for i := 0; i < 5000; i++ {
		a.receive(uint64(rng.Int63n(2000)), true)
		for j := 1; j < len(a.ranges); j++ {
			prev, cur := a.ranges[j-1], a.ranges[j]
			if cur.hi >= prev.lo {
				t.Fatalf("step %d: ranges overlap or touch: %v then %v (full: %v)", i, prev, cur, a.ranges)
			}
			if cur.hi+1 == prev.lo {
				t.Fatalf("step %d: adjacent ranges left unmerged: %v then %v", i, prev, cur)
			}
		}
		for _, r := range a.ranges {
			if r.lo > r.hi {
				t.Fatalf("step %d: inverted range %v", i, r)
			}
		}
	}
}

// BenchmarkAckTrackerReceive holds the steady-state receive path to zero
// allocations. The sort.Slice it replaced took a reflect-based swapper, which
// allocated on every packet the peer sent; quic is inside the bench-gate's
// enforced scope but had no benchmark covering this path at all.
//
// The tracker is warmed to its steady state first — permanently gapped packet
// numbers, so it sits at maxAckRanges and every arrival is the worst case, a
// range of its own that displaces the oldest. Measuring from an empty tracker
// would report the one-off slice growth instead of the per-packet cost.
func BenchmarkAckTrackerReceive(b *testing.B) {
	var a ackTracker
	var pn uint64
	for i := 0; i < maxAckRanges*4; i++ {
		a.receive(pn, true)
		pn += 2
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.receive(pn, true)
		pn += 2
	}
}

// BenchmarkAckTrackerReceiveInOrder is the common case: contiguous packet
// numbers, which all merge into the single top range.
func BenchmarkAckTrackerReceiveInOrder(b *testing.B) {
	var a ackTracker
	var pn uint64
	for i := 0; i < 64; i++ {
		a.receive(pn, true)
		pn++
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.receive(pn, true)
		pn++
	}
}
