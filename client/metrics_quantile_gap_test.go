package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHistogramSnapshot_Quantile_Boundaries is the boundary-value table
// Quantile's documented 0 ≤ q ≤ 1 domain deserves (#873).
//
// q = 0 is the one value where the `if target == 0 { target = 1 }` floor is
// load-bearing, and nothing exercised it: metrics_test.go calls only 0.5 and
// 0.99, and coverage_test.go's q=0 case asserts merely that the result is
// non-zero — which the unfloored 1ns satisfies. Deleting the floor is
// SURVIVOR 2/2 against the whole package.
//
// Without the floor, target is 0 and the accumulation loop's first iteration
// tests `cum >= target` as `0 >= 0`, true even when Buckets[0] is empty: the
// answer becomes bucket 0's upper edge (1ns) regardless of where the
// observations actually are. Every assertion below is a concrete duration
// rather than "non-zero", because "non-zero" is exactly what 1ns is.
func TestHistogramSnapshot_Quantile_Boundaries(t *testing.T) {
	t.Parallel()
	// One thousand observations, all in the same high bucket. Observe puts
	// d into bucket floor(log2(ns)), whose upper edge is 2^(i+1)-1.
	const observed = 1 << 20 // 1048576ns → bucket 20, upper edge 2^21-1
	var h Histogram
	for range 1000 {
		h.Observe(observed * time.Nanosecond)
	}
	s := h.Snapshot()
	const wantEdge = time.Duration(1<<21 - 1)
	require.EqualValuesf(t, 1000, s.Count, "the fixture recorded %d observations", s.Count)
	require.Zerof(t, s.Buckets[0],
		"bucket 0 is not empty, so an unfloored q=0 would land on it legitimately and "+
			"the boundary under test would be unobservable")

	cases := []struct {
		name string
		q    float64
		want time.Duration
	}{
		{"q=0 is the smallest observation's bucket, not bucket 0", 0, wantEdge},
		{"q=1 is the largest observation's bucket", 1, wantEdge},
		{"q in the middle", 0.5, wantEdge},
		{"q below the domain clamps to 0", -0.5, wantEdge},
		{"q above the domain clamps to 1", 1.5, wantEdge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got := s.Quantile(c.q)

			assert.Equalf(t, c.want, got,
				"Quantile(%v) = %v, want %v — every observation is in one bucket, so every "+
					"quantile is that bucket's edge; 1ns here means the accumulation started "+
					"from target 0 and returned bucket 0's edge without a single observation "+
					"in it", c.q, got, c.want)
		})
	}
}

// TestHistogramSnapshot_Quantile_EmptyAndSpread is the rest of the domain: the
// empty snapshot, and a histogram whose observations are spread so the quantiles
// are genuinely different from one another. Without this arm the table above is
// satisfied by a Quantile that returns one fixed bucket edge.
func TestHistogramSnapshot_Quantile_EmptyAndSpread(t *testing.T) {
	t.Parallel()
	var empty HistogramSnapshot

	assert.Zerof(t, empty.Quantile(0.5),
		"Quantile on a snapshot with no observations = %v, want 0 — a latency dashboard "+
			"with no traffic must read as absent, not as 1ns", empty.Quantile(0.5))

	var h Histogram
	for range 990 {
		h.Observe(1 << 10 * time.Nanosecond) // bucket 10, edge 2^11-1
	}
	for range 10 {
		h.Observe(1 << 30 * time.Nanosecond) // bucket 30, edge 2^31-1
	}
	s := h.Snapshot()

	lo, hi := s.Quantile(0), s.Quantile(1)

	assert.EqualValuesf(t, time.Duration(1<<11-1), lo,
		"Quantile(0) = %v over 990 fast and 10 slow observations, want the fast bucket's "+
			"edge", lo)
	assert.EqualValuesf(t, time.Duration(1<<31-1), hi,
		"Quantile(1) = %v, want the slow bucket's edge; a p100 that reports the fast "+
			"bucket hides the tail the histogram exists to show", hi)
	assert.Lessf(t, lo, hi,
		"Quantile(0) = %v and Quantile(1) = %v are not ordered, so the quantile does not "+
			"depend on q at all", lo, hi)
}
