package prometheus

import "github.com/lodgvideon/poseidon-http-client/client"

// Bucket-boundary range for the histograms republished by [Collector].
//
// The client's histogram has 64 log2 buckets spanning [1ns, 2^63 ns). Most
// of that range is dead weight for an HTTP client: sub-microsecond buckets
// never fill and the 2^40+ buckets describe durations no request survives.
// We publish the useful window and let observations outside it fall into
// the first bucket or into +Inf, which Prometheus derives from the count.
const (
	// MinBucketExp is the exponent of the lowest published boundary:
	// 2^10-1 ns ≈ 1.02 µs. Anything faster lands in this first bucket.
	MinBucketExp = 10
	// MaxBucketExp is the exponent of the highest published boundary:
	// 2^35-1 ns ≈ 34.36 s. Anything slower is counted only by +Inf.
	MaxBucketExp = 35
)

// nsPerSecond converts the client's nanosecond histogram to the seconds
// Prometheus expects.
const nsPerSecond = 1e9

// log2Histogram converts a client histogram snapshot into the (count, sum,
// cumulative-buckets) triple that prom.NewConstHistogram takes.
//
// The client's bucket i holds observations with floor(log2(ns)) == i — that
// is, ns in [2^i, 2^(i+1)-1]. So every observation at or below 2^e-1 ns sits
// in buckets 0..e-1, and the cumulative count for the boundary le = 2^e-1 is
// exactly the sum of those buckets. The counts are therefore exact; only the
// boundary spacing (a factor of 2) is coarse.
func log2Histogram(s client.HistogramSnapshot) (count uint64, sum float64, buckets map[float64]uint64) {
	buckets = make(map[float64]uint64, MaxBucketExp-MinBucketExp+1)

	var cum int64
	for e := 0; e <= MaxBucketExp; e++ {
		// Buckets 0..e-1 cover everything <= 2^e-1 ns, so accumulate
		// bucket e-1 before emitting the boundary for exponent e.
		if e > 0 {
			cum += s.Buckets[e-1]
		}
		if e < MinBucketExp {
			continue
		}
		buckets[boundarySeconds(e)] = uint64(max(cum, 0))
	}

	return uint64(max(s.Count, 0)), float64(max(s.Sum, 0)) / nsPerSecond, buckets
}

// boundarySeconds returns the le boundary for exponent e in seconds:
// the upper edge of client bucket e-1, i.e. 2^e-1 nanoseconds.
func boundarySeconds(e int) float64 {
	return float64(int64(1)<<e-1) / nsPerSecond
}
