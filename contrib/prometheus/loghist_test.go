package prometheus

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
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
	_, _, buckets := log2Histogram(client.HistogramSnapshot{})

	if got, want := len(buckets), MaxBucketExp-MinBucketExp+1; got != want {
		t.Fatalf("bucket count = %d, want %d", got, want)
	}
	// The boundary for exponent e is the upper edge of client bucket e-1,
	// i.e. 2^e-1 nanoseconds expressed in seconds.
	for _, e := range []int{MinBucketExp, 20, MaxBucketExp} {
		want := float64(int64(1)<<e-1) / 1e9
		if _, ok := buckets[want]; !ok {
			t.Errorf("no boundary for exponent %d (le=%v)", e, want)
		}
	}
	if _, ok := buckets[boundarySeconds(MinBucketExp-1)]; ok {
		t.Errorf("boundary below MinBucketExp should not be published")
	}
	if _, ok := buckets[boundarySeconds(MaxBucketExp+1)]; ok {
		t.Errorf("boundary above MaxBucketExp should not be published")
	}
}

func TestLog2Histogram_CumulativeCountsAreExact(t *testing.T) {
	// One observation in bucket 10 (ns in [2^10, 2^11-1]), two in bucket 20,
	// three in bucket 30.
	s := snapshotWith(map[int]int64{10: 1, 20: 2, 30: 3})
	s.Sum = 5_000_000_000 // 5s

	count, sum, buckets := log2Histogram(s)

	if count != 6 {
		t.Errorf("count = %d, want 6", count)
	}
	if sum != 5 {
		t.Errorf("sum = %v seconds, want 5", sum)
	}

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
	for _, c := range cases {
		got, ok := buckets[boundarySeconds(c.exp)]
		if !ok {
			t.Fatalf("exponent %d: no such boundary", c.exp)
		}
		if got != c.want {
			t.Errorf("exponent %d: cumulative = %d, want %d", c.exp, got, c.want)
		}
	}
}

func TestLog2Histogram_FastObservationsFoldIntoFirstBucket(t *testing.T) {
	// 300ns lands in client bucket 8, below the lowest published boundary.
	// It must still be counted, in the very first bucket.
	s := snapshotWith(map[int]int64{8: 7})

	_, _, buckets := log2Histogram(s)

	if got := buckets[boundarySeconds(MinBucketExp)]; got != 7 {
		t.Errorf("first bucket = %d, want 7 (sub-microsecond observations fold here)", got)
	}
}

func TestLog2Histogram_SlowObservationsCountedOnlyByInf(t *testing.T) {
	// Client bucket 40 is ~18 minutes — past the highest published boundary.
	s := snapshotWith(map[int]int64{40: 2})

	count, _, buckets := log2Histogram(s)

	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
	if got := buckets[boundarySeconds(MaxBucketExp)]; got != 0 {
		t.Errorf("last finite bucket = %d, want 0; the observation belongs to +Inf only", got)
	}
}

func TestLog2Histogram_EmptyIsAllZero(t *testing.T) {
	count, sum, buckets := log2Histogram(client.HistogramSnapshot{})

	if count != 0 || sum != 0 {
		t.Errorf("count/sum = %d/%v, want 0/0", count, sum)
	}
	for le, n := range buckets {
		if n != 0 {
			t.Errorf("bucket le=%v = %d, want 0", le, n)
		}
	}
}

// A negative Sum or Count can only come from a corrupt snapshot, but the
// conversion must not produce a negative-to-unsigned wraparound.
func TestLog2Histogram_NegativeInputClampsToZero(t *testing.T) {
	s := client.HistogramSnapshot{Count: -1, Sum: -1}

	count, sum, _ := log2Histogram(s)

	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
	if sum != 0 {
		t.Errorf("sum = %v, want 0", sum)
	}
}
