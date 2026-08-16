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
