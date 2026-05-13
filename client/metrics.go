// client/metrics.go
package client

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// Counters is the lock-free integer-counter struct embedded in Metrics.
// All fields are updated via atomic.Add and read via Snapshot for a
// value-safe copy. Direct field access is goroutine-safe but the
// resulting tuple is not torn-coherent across reads — use Snapshot
// when you need a consistent view.
type Counters struct {
	RequestsStarted   atomic.Int64
	RequestsSucceeded atomic.Int64 // status code received (any)
	RequestsErrored   atomic.Int64 // Do returned non-nil err
	Retries           atomic.Int64
	DialsAttempted    atomic.Int64
	DialsFailed       atomic.Int64
	ConnsClosed       atomic.Int64 // sum across all CloseReason values
	GoAwaysReceived   atomic.Int64
}

// CountersSnapshot is a frozen, value-copyable view of Counters.
// Returned by Counters.Snapshot.
type CountersSnapshot struct {
	RequestsStarted   int64
	RequestsSucceeded int64
	RequestsErrored   int64
	Retries           int64
	DialsAttempted    int64
	DialsFailed       int64
	ConnsClosed       int64
	GoAwaysReceived   int64
}

// Snapshot returns a value-copyable view. Each field is read with one
// atomic.Load; field-to-field consistency is best-effort (counters may
// have been updated between Loads).
func (c *Counters) Snapshot() CountersSnapshot {
	return CountersSnapshot{
		RequestsStarted:   c.RequestsStarted.Load(),
		RequestsSucceeded: c.RequestsSucceeded.Load(),
		RequestsErrored:   c.RequestsErrored.Load(),
		Retries:           c.Retries.Load(),
		DialsAttempted:    c.DialsAttempted.Load(),
		DialsFailed:       c.DialsFailed.Load(),
		ConnsClosed:       c.ConnsClosed.Load(),
		GoAwaysReceived:   c.GoAwaysReceived.Load(),
	}
}

// Histogram is a lock-free log2-bucket latency histogram.
//
// Bucket i holds observations with floor(log2(ns)) == i. 64 buckets
// span [1ns, 2^63 ns). One Observe is one bits.Len64 + 3 atomic.Add;
// no allocation. 0 / negative durations clamp to bucket 0.
type Histogram struct {
	buckets [64]atomic.Int64
	sum     atomic.Int64 // ns
	count   atomic.Int64
}

// Observe records a single duration.
func (h *Histogram) Observe(d time.Duration) {
	n := int64(d)
	if n < 1 {
		n = 1
	}
	idx := bits.Len64(uint64(n)) - 1
	h.buckets[idx].Add(1)
	h.sum.Add(n)
	h.count.Add(1)
}

// HistogramSnapshot is a frozen view of Histogram state.
type HistogramSnapshot struct {
	Buckets [64]int64
	Sum     int64 // ns
	Count   int64
}

// Snapshot copies the current bucket counts, sum, and count.
// Field-to-field consistency is best-effort; a concurrent Observe may
// update sum and count between individual atomic Loads.
func (h *Histogram) Snapshot() HistogramSnapshot {
	var s HistogramSnapshot
	for i := range h.buckets {
		s.Buckets[i] = h.buckets[i].Load()
	}
	s.Sum = h.sum.Load()
	s.Count = h.count.Load()
	return s
}

// Mean returns the arithmetic mean of all observations, or 0 on empty.
func (s HistogramSnapshot) Mean() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return time.Duration(s.Sum / s.Count)
}

// Quantile returns the upper edge of the bucket containing the q-th
// observation (0 ≤ q ≤ 1). Bucket-edge approximation: precise to a
// factor of 2. Returns 0 if no observations recorded.
func (s HistogramSnapshot) Quantile(q float64) time.Duration {
	if s.Count == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	target := int64(float64(s.Count) * q)
	if target == 0 {
		target = 1
	}
	var cum int64
	for i, n := range s.Buckets {
		cum += n
		if cum >= target {
			// Upper edge of bucket i is 2^(i+1) - 1 ns.
			if i >= 63 {
				// Bucket 63 is unreachable via Observe (max int64 maps to bucket 62),
				// but guard against int64(1)<<64 overflow.
				return time.Duration(math.MaxInt64)
			}
			return time.Duration((int64(1) << (i + 1)) - 1)
		}
	}
	// Should be unreachable when count > 0; guard anyway.
	return time.Duration((1 << 62) - 1)
}

// Metrics aggregates Counters and per-event-class latency histograms
// for a single Client. Pass-through pointer; do not value-copy.
type Metrics struct {
	Counters Counters
	Latency  struct {
		Request Histogram
		Dial    Histogram
		Acquire Histogram
	}
}

// MetricsSnapshot is a frozen, value-copyable view of Metrics.
type MetricsSnapshot struct {
	Counters CountersSnapshot
	Latency  struct {
		Request HistogramSnapshot
		Dial    HistogramSnapshot
		Acquire HistogramSnapshot
	}
}

// Snapshot copies counters and histograms into a value-safe struct.
func (m *Metrics) Snapshot() MetricsSnapshot {
	var s MetricsSnapshot
	s.Counters = m.Counters.Snapshot()
	s.Latency.Request = m.Latency.Request.Snapshot()
	s.Latency.Dial = m.Latency.Dial.Snapshot()
	s.Latency.Acquire = m.Latency.Acquire.Snapshot()
	return s
}
