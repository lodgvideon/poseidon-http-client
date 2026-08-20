package bufx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadBufPool_RoundTrip(t *testing.T) {
	const ask = 4096

	p := GetReadBuf(ask)
	require.GreaterOrEqualf(t, cap(*p), ask,
		"cap = %d, want >= %d — a buffer smaller than the ask is not usable by the caller at all", cap(*p), ask)
	*p = (*p)[:ask]
	for i := range *p {
		(*p)[i] = byte(i)
	}
	PutReadBuf(p)

	p2 := GetReadBuf(ask)
	defer PutReadBuf(p2)

	require.GreaterOrEqualf(t, cap(*p2), ask,
		"cap after reuse = %d, want >= %d — a recycled buffer that no longer satisfies the ask makes the pool worse than allocating outright",
		cap(*p2), ask)
}

func TestReadBufPool_GrowsWhenSmaller(t *testing.T) {
	const ask = 64 << 10 // past defaultReadBufSize, so nothing recycled can serve it

	p := GetReadBuf(ask)
	defer PutReadBuf(p)

	require.GreaterOrEqualf(t, cap(*p), ask,
		"cap = %d, want >= %d — an ask the pool cannot serve must be satisfied by a fresh allocation, not by handing back a short buffer the caller will overrun",
		cap(*p), ask)
}

// TestGetReadBuf_ReturnsZeroLengthOnBothPaths pins the half of GetReadBuf's
// documented postcondition nothing asserted: "the returned slice has length 0".
// Both existing pool tests check cap() only.
//
// The two return paths reach length 0 by different means — the grow path builds
// make([]byte, 0, min), the pooled path re-slices a buffer whose length is
// whatever the last caller left — so they are the classic pair that drifts
// apart. frame.NewFramer, the only production consumer today, re-slices what it
// gets and would not notice; the next consumer that appends to the returned
// slice would silently prepend a byte of stale pooled data.
func TestGetReadBuf_ReturnsZeroLengthOnBothPaths(t *testing.T) {
	const pooled = 4096    // below defaultReadBufSize: served by the pool
	const grown = 64 << 10 // above it: nothing recycled can serve this

	fromPool := GetReadBuf(pooled)
	fresh := GetReadBuf(grown)

	t.Cleanup(func() { PutReadBuf(fromPool); PutReadBuf(fresh) })
	assert.Zerof(t, len(*fromPool),
		"the pooled path returned len %d, want 0: the previous owner's length must not survive into the next caller, or an append writes after bytes it never wrote",
		len(*fromPool))
	assert.Zerof(t, len(*fresh),
		"the grow path returned len %d, want 0: the two paths must agree, or the contract holds only for asks the pool happens to serve",
		len(*fresh))
}

// TestPutReadBuf_NilIsIgnored executes the guard in PutReadBuf, which no test
// reached. It is not decorative: sync.Pool.Put early-returns only on a nil
// *interface*, and a typed nil (*[]byte)(nil) boxes into a non-nil any and is
// stored happily. The next GetReadBuf — in whatever unrelated goroutine draws
// that entry — then dereferences it for cap(*p) and panics.
//
// The nil ask is a live shape: frame.Close and frame.SetReadBuffer both nil out
// their buffer pointer around putting it back.
func TestPutReadBuf_NilIsIgnored(t *testing.T) {
	const ask = 4096
	// Empty the pool before contributing to it. sync.Pool serves its per-P
	// private slot ahead of everything else, so a buffer an earlier test handed
	// back would satisfy the Get below and the entry this test puts in would
	// never be examined at all. Measured, not assumed: without this drain the
	// guard-deleted mutant SURVIVES the suite, because the poisoned entry is
	// still sitting behind a perfectly good one.
	for range 8 {
		_ = GetReadBuf(ask)
	}
	var nothing *[]byte

	PutReadBuf(nothing)

	p := GetReadBuf(ask)
	t.Cleanup(func() { PutReadBuf(p) })
	require.NotNilf(t, p,
		"GetReadBuf returned nil after PutReadBuf(nil); a poisoned pool entry surfaces in an unrelated caller that merely drew it, which is the worst shape of bug to trace back here")
	assert.GreaterOrEqualf(t, cap(*p), ask,
		"cap = %d, want >= %d after a nil Put; the pool must be no worse off for having been offered nothing", cap(*p), ask)
}

func BenchmarkReadBufPool_GetPut(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := GetReadBuf(4096)
		PutReadBuf(p)
	}
}

// TestGetReadBuf_ServesTheFramerAsk is the gate on the size. The pool's one
// production consumer is frame.NewFramer, which needs a whole maximum HTTP/2
// frame — 16384 bytes of payload plus the 9-byte header. A pool whose New is
// nine bytes short of that can never satisfy it: every cold Get allocated, threw
// the result away, and allocated again.
//
// The constant is duplicated here rather than imported: internal/bufx must not
// depend on frame, so the test states the demand it is sized for and fails if
// either side moves.
func TestGetReadBuf_ServesTheFramerAsk(t *testing.T) {
	const framerAsk = 16<<10 + 9 // frame: defaultMaxFrameSize + FrameHeaderSize

	// A buffer straight from New, with nothing recycled to mask the size.
	fresh := readBufPool.New().(*[]byte)

	assert.GreaterOrEqualf(t, cap(*fresh), framerAsk,
		"the pool's New yields cap %d, below the %d its only consumer asks for — "+
			"every cold NewFramer pays two allocations", cap(*fresh), framerAsk)
}

// There is deliberately NO test that the undersized buffer returns to the pool.
// sync.Pool retention is not observable: the GC may empty a pool at any point,
// and a goroutine may migrate between per-P caches, so a "the buffer is still
// there" assertion passes or fails on timing rather than on the code. A first
// draft of exactly that test went green locally and red on CI under -race. The
// put-back is a strict improvement with no cost; the size above is the part that
// is both load-bearing and checkable.
