package prometheus

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotWith builds a HistogramSnapshot with the given per-bucket counts,
// deriving Count from them. Sum is set by the caller where it matters.
func snapshotWith(counts map[int]int64) client.HistogramSnapshot {
	var s client.HistogramSnapshot
	for i, n := range counts {
		s.Buckets[i] = n
		s.Count += n
	}
	return s
}

func TestLog2Histogram_BoundariesLandOnLog2Edges(t *testing.T) {
	snapshot := client.HistogramSnapshot{}

	_, _, buckets := log2Histogram(snapshot)

	require.Len(t, buckets, MaxBucketExp-MinBucketExp+1,
		"exactly the [MinBucketExp, MaxBucketExp] window is published; the rest of the client's 64 log2 buckets is dead weight")
	// The boundary for exponent e is the upper edge of client bucket e-1,
	// i.e. 2^e-1 nanoseconds expressed in seconds. Computed here rather than
	// via boundarySeconds so the test does not re-derive its own subject.
	for _, e := range []int{MinBucketExp, 20, MaxBucketExp} {
		want := float64(int64(1)<<e-1) / 1e9
		assert.Containsf(t, buckets, want,
			"no boundary for exponent %d (le=%v); Prometheus bucket edges must sit on the client's log2 edges or the counts stop being exact", e, want)
	}
	assert.NotContains(t, buckets, boundarySeconds(MinBucketExp-1),
		"a boundary below MinBucketExp must not be published; sub-microsecond buckets never fill")
	assert.NotContains(t, buckets, boundarySeconds(MaxBucketExp+1),
		"a boundary above MaxBucketExp must not be published; those buckets describe durations no request survives")
}

func TestLog2Histogram_CumulativeCountsAreExact(t *testing.T) {
	// One observation in bucket 10 (ns in [2^10, 2^11-1]), two in bucket 20,
	// three in bucket 30.
	s := snapshotWith(map[int]int64{10: 1, 20: 2, 30: 3})
	s.Sum = 5_000_000_000 // 5s
	// A bucket at exponent e accumulates client buckets 0..e-1, so the
	// observation in bucket 10 is counted from exponent 11 onward.
	cases := []struct {
		exp  int
		want uint64
	}{
		{10, 0}, // nothing at or below 2^10-1
		{11, 1}, // the bucket-10 observation
		{20, 1},
		{21, 3}, // + the two in bucket 20
		{30, 3},
		{31, 6}, // + the three in bucket 30
		{MaxBucketExp, 6},
	}

	count, sum, buckets := log2Histogram(s)

	assert.Equal(t, uint64(6), count, "count must be the total observation count, independent of the published window")
	assert.Equal(t, float64(5), sum, "sum must be converted from nanoseconds to the seconds Prometheus expects")
	for _, c := range cases {
		got, ok := buckets[boundarySeconds(c.exp)]
		require.Truef(t, ok, "exponent %d: no such boundary", c.exp)
		assert.Equalf(t, c.want, got,
			"exponent %d: cumulative counts are exact — bucket e must be the sum of client buckets 0..e-1", c.exp)
	}
}

func TestLog2Histogram_FastObservationsFoldIntoFirstBucket(t *testing.T) {
	// 300ns lands in client bucket 8, below the lowest published boundary.
	// It must still be counted, in the very first bucket.
	s := snapshotWith(map[int]int64{8: 7})

	_, _, buckets := log2Histogram(s)

	assert.Equal(t, uint64(7), buckets[boundarySeconds(MinBucketExp)],
		"sub-microsecond observations fold into the first published bucket rather than being dropped")
}

func TestLog2Histogram_SlowObservationsCountedOnlyByInf(t *testing.T) {
	// Client bucket 40 is ~18 minutes — past the highest published boundary.
	s := snapshotWith(map[int]int64{40: 2})

	count, _, buckets := log2Histogram(s)

	assert.Equal(t, uint64(2), count,
		"an observation past the published window must still reach the count; Prometheus derives +Inf from count minus the last bucket")
	assert.Equal(t, uint64(0), buckets[boundarySeconds(MaxBucketExp)],
		"the last finite bucket must not claim an observation slower than its boundary")
}

func TestLog2Histogram_EmptyIsAllZero(t *testing.T) {
	snapshot := client.HistogramSnapshot{}

	count, sum, buckets := log2Histogram(snapshot)

	assert.Equal(t, uint64(0), count, "an unobserved histogram reports a zero count, not a missing one")
	assert.Equal(t, float64(0), sum, "an unobserved histogram reports a zero sum, not a missing one")
	for le, n := range buckets {
		assert.Equalf(t, uint64(0), n, "bucket le=%v must be 0 before any observation", le)
	}
}

// A negative Sum or Count can only come from a corrupt snapshot, but the
// conversion must not produce a negative-to-unsigned wraparound.
func TestLog2Histogram_NegativeInputClampsToZero(t *testing.T) {
	s := client.HistogramSnapshot{Count: -1, Sum: -1}

	count, sum, _ := log2Histogram(s)

	assert.Equal(t, uint64(0), count,
		"a negative count must clamp to 0, not wrap to 18446744073709551615 and poison the scrape")
	assert.Equal(t, float64(0), sum,
		"a negative sum must clamp to 0 rather than be republished as a negative duration")
}
