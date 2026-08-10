package bytesx

import (
	"testing"
)

func TestReadBufPool_RoundTrip(t *testing.T) {
	p := GetReadBuf(4096)
	if cap(*p) < 4096 {
		t.Fatalf("cap = %d, want >= 4096", cap(*p))
	}
	*p = (*p)[:4096]
	for i := range *p {
		(*p)[i] = byte(i)
	}
	PutReadBuf(p)

	p2 := GetReadBuf(4096)
	if cap(*p2) < 4096 {
		t.Fatalf("cap after reuse = %d, want >= 4096", cap(*p2))
	}
	PutReadBuf(p2)
}

func TestReadBufPool_GrowsWhenSmaller(t *testing.T) {
	p := GetReadBuf(64 << 10)
	if cap(*p) < 64<<10 {
		t.Fatalf("cap = %d, want >= %d", cap(*p), 64<<10)
	}
	PutReadBuf(p)
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
// The constant is duplicated here rather than imported: internal/bytesx must not
// depend on frame, so the test states the demand it is sized for and fails if
// either side moves.
func TestGetReadBuf_ServesTheFramerAsk(t *testing.T) {
	const framerAsk = 16<<10 + 9 // frame: defaultMaxFrameSize + FrameHeaderSize

	// A buffer straight from New, with nothing recycled to mask the size.
	fresh := readBufPool.New().(*[]byte)
	if cap(*fresh) < framerAsk {
		t.Errorf("the pool's New yields cap %d, below the %d its only consumer asks for — "+
			"every cold NewFramer pays two allocations", cap(*fresh), framerAsk)
	}
}

// There is deliberately NO test that the undersized buffer returns to the pool.
// sync.Pool retention is not observable: the GC may empty a pool at any point,
// and a goroutine may migrate between per-P caches, so a "the buffer is still
// there" assertion passes or fails on timing rather than on the code. A first
// draft of exactly that test went green locally and red on CI under -race. The
// put-back is a strict improvement with no cost; the size above is the part that
// is both load-bearing and checkable.
