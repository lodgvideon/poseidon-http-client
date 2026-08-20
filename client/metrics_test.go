// client/metrics_test.go
package client

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCounters_AtomicityUnderLoad(t *testing.T) {
	t.Parallel()

	var c Counters
	const goroutines, perG = 64, 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				c.RequestsStarted.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.EqualValuesf(t, goroutines*perG, c.RequestsStarted.Load(),
		"RequestsStarted = %d, want %d — a lost increment means the counter is not "+
			"atomic and every rate a load generator reports is understated",
		c.RequestsStarted.Load(), goroutines*perG)
}

func TestCounters_Snapshot(t *testing.T) {
	t.Parallel()

	var c Counters
	c.RequestsStarted.Store(7)
	c.DialsAttempted.Store(2)
	c.GoAwaysReceived.Store(1)

	s := c.Snapshot()

	// Distinct values per field: an equal-value fixture cannot tell a
	// mis-wired field from a correct one.
	assert.EqualValues(t, 7, s.RequestsStarted, "Snapshot must copy RequestsStarted from its own counter")
	assert.EqualValues(t, 2, s.DialsAttempted, "Snapshot must copy DialsAttempted from its own counter")
	assert.EqualValues(t, 1, s.GoAwaysReceived, "Snapshot must copy GoAwaysReceived from its own counter")
}

func TestHistogram_BucketBoundaries(t *testing.T) {
	t.Parallel()

	// Boundary values around each power-of-two bucket edge: the first value of
	// a bucket, an interior value, and the last value before the next edge.
	cases := []struct {
		ns      int64
		wantIdx int
	}{
		{1, 0},     // [1,2)
		{2, 1},     // [2,4)
		{3, 1},     // [2,4)
		{1023, 9},  // [512,1024)
		{1024, 10}, // [1024,2048)
		{1 << 30, 30},
	}

	for _, tc := range cases {
		var h Histogram

		h.Observe(time.Duration(tc.ns))

		assert.EqualValuesf(t, 1, h.buckets[tc.wantIdx].Load(),
			"Observe(%d ns): bucket[%d] = %d, want 1", tc.ns, tc.wantIdx, h.buckets[tc.wantIdx].Load())
		// Adjacent buckets must be 0 — an off-by-one in the index calculation
		// would otherwise be invisible whenever it lands on a neighbour.
		for i := 0; i < 64; i++ {
			if i == tc.wantIdx {
				continue
			}
			assert.EqualValuesf(t, 0, h.buckets[i].Load(),
				"Observe(%d ns): bucket[%d] = %d, want 0 (only [%d] should be set)",
				tc.ns, i, h.buckets[i].Load(), tc.wantIdx)
		}
	}
}

func TestHistogram_ObserveBelowOne(t *testing.T) {
	t.Parallel()

	var h Histogram

	// 0 and negative durations clamp to bucket 0.
	h.Observe(0)
	h.Observe(-5)

	assert.EqualValuesf(t, 2, h.buckets[0].Load(),
		"bucket[0] = %d, want 2 — a sub-1ns duration must clamp, not index out of range",
		h.buckets[0].Load())
}

func TestHistogram_Snapshot(t *testing.T) {
	t.Parallel()

	var h Histogram
	for i := 0; i < 100; i++ {
		h.Observe(100 * time.Microsecond)
	}

	s := h.Snapshot()

	assert.EqualValuesf(t, 100, s.Count, "Count = %d, want 100", s.Count)
	assert.EqualValuesf(t, int64(100*100*time.Microsecond), s.Sum,
		"Sum = %d, want %d", s.Sum, int64(100*100*time.Microsecond))
	assert.Equalf(t, 100*time.Microsecond, s.Mean(),
		"Mean = %v, want %v — Mean must divide Sum by Count", s.Mean(), 100*time.Microsecond)
}

func TestHistogram_Quantile(t *testing.T) {
	t.Parallel()

	// 90 observations in bucket 8 (500ns); 10 in bucket 19 (1ms).
	var h Histogram
	for i := 0; i < 90; i++ {
		h.Observe(500 * time.Nanosecond) // bucket 8
	}
	for i := 0; i < 10; i++ {
		h.Observe(time.Millisecond) // bucket 19 (1ms = 10^6 ns; log2(10^6) ≈ 19.93 → bucket 19)
	}

	s := h.Snapshot()

	q50 := s.Quantile(0.5)
	assert.Truef(t, q50 >= 256*time.Nanosecond && q50 <= 1024*time.Nanosecond,
		"Quantile(0.5) = %v, want bucket 8 upper edge (≤1024ns)", q50)
	q99 := s.Quantile(0.99)
	assert.GreaterOrEqualf(t, q99, 524288*time.Nanosecond,
		"Quantile(0.99) = %v, want bucket 19 (≥524288ns)", q99)
}

func TestHistogram_QuantileEmpty(t *testing.T) {
	t.Parallel()

	var h Histogram

	s := h.Snapshot()

	assert.EqualValuesf(t, 0, s.Quantile(0.5),
		"Quantile on empty = %v, want 0 — an unobserved histogram must not invent a latency",
		s.Quantile(0.5))
	assert.EqualValuesf(t, 0, s.Mean(),
		"Mean on empty = %v, want 0 — Mean must not divide by a zero Count", s.Mean())
}

func TestMetrics_AcquireLatencyRecorded(t *testing.T) {
	// Tested via integration in Task 12's full-flow test; here just
	// confirm the histogram exists and is zero by default.
	var m Metrics

	got := m.Latency.Acquire.Snapshot().Count

	require.EqualValuesf(t, 0, got,
		"acquire histogram not zero on init: Count = %d — Snapshot must report observed "+
			"counts, never fabricate them", got)
}
