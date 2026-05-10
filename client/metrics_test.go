// client/metrics_test.go
package client

import (
	"sync"
	"testing"
	"time"
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
	if got := c.RequestsStarted.Load(); got != goroutines*perG {
		t.Errorf("RequestsStarted = %d, want %d", got, goroutines*perG)
	}
}

func TestCounters_Snapshot(t *testing.T) {
	t.Parallel()
	var c Counters
	c.RequestsStarted.Store(7)
	c.DialsAttempted.Store(2)
	c.GoAwaysReceived.Store(1)
	s := c.Snapshot()
	if s.RequestsStarted != 7 || s.DialsAttempted != 2 || s.GoAwaysReceived != 1 {
		t.Errorf("snapshot mismatch: %+v", s)
	}
}

func TestHistogram_BucketBoundaries(t *testing.T) {
	t.Parallel()
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
		if got := h.buckets[tc.wantIdx].Load(); got != 1 {
			t.Errorf("Observe(%d ns): bucket[%d] = %d, want 1", tc.ns, tc.wantIdx, got)
		}
		// Adjacent buckets must be 0.
		for i := 0; i < 64; i++ {
			if i == tc.wantIdx {
				continue
			}
			if got := h.buckets[i].Load(); got != 0 {
				t.Errorf("Observe(%d ns): bucket[%d] = %d, want 0 (only [%d] should be set)",
					tc.ns, i, got, tc.wantIdx)
			}
		}
	}
}

func TestHistogram_ObserveBelowOne(t *testing.T) {
	t.Parallel()
	// 0 and negative durations clamp to bucket 0.
	var h Histogram
	h.Observe(0)
	h.Observe(-5)
	if got := h.buckets[0].Load(); got != 2 {
		t.Errorf("bucket[0] = %d, want 2", got)
	}
}

func TestHistogram_Snapshot(t *testing.T) {
	t.Parallel()
	var h Histogram
	for i := 0; i < 100; i++ {
		h.Observe(100 * time.Microsecond)
	}
	s := h.Snapshot()
	if s.Count != 100 {
		t.Errorf("Count = %d, want 100", s.Count)
	}
	if s.Sum != int64(100*100*time.Microsecond) {
		t.Errorf("Sum = %d, want %d", s.Sum, int64(100*100*time.Microsecond))
	}
	if mean := s.Mean(); mean != 100*time.Microsecond {
		t.Errorf("Mean = %v, want %v", mean, 100*time.Microsecond)
	}
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
	if q50 < 256*time.Nanosecond || q50 > 1024*time.Nanosecond {
		t.Errorf("Quantile(0.5) = %v, want bucket 8 upper edge (≤1024ns)", q50)
	}
	q99 := s.Quantile(0.99)
	if q99 < 524288*time.Nanosecond {
		t.Errorf("Quantile(0.99) = %v, want bucket 19 (≥524288ns)", q99)
	}
}

func TestHistogram_QuantileEmpty(t *testing.T) {
	t.Parallel()
	var h Histogram
	s := h.Snapshot()
	if got := s.Quantile(0.5); got != 0 {
		t.Errorf("Quantile on empty = %v, want 0", got)
	}
	if got := s.Mean(); got != 0 {
		t.Errorf("Mean on empty = %v, want 0", got)
	}
}

func TestMetrics_AcquireLatencyRecorded(t *testing.T) {
	// Tested via integration in Task 12's full-flow test; here just
	// confirm the histogram exists and is zero by default.
	var m Metrics
	if m.Latency.Acquire.Snapshot().Count != 0 {
		t.Error("acquire histogram not zero on init")
	}
}
