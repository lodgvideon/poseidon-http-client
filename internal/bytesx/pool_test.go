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

// TestGetReadBuf_UndersizedBufferGoesBack pins the other half. When a caller
// asks for more than the pooled buffer holds, the buffer it did not use must
// return to the pool: dropping it discards a live allocation and leaves the pool
// no fuller, so a run of oversized asks drains it one buffer at a time.
func TestGetReadBuf_UndersizedBufferGoesBack(t *testing.T) {
	// Seed a buffer that is deliberately smaller than the ask below.
	small := make([]byte, 0, 128)
	readBufPool.Put(&small)

	big := GetReadBuf(1 << 20) // forces the too-small branch
	if cap(*big) < 1<<20 {
		t.Fatalf("GetReadBuf(1 MiB) returned cap %d", cap(*big))
	}

	// The seeded buffer must still be reachable. sync.Pool gives no ordering
	// guarantee, so this asks only that SOMETHING is there to take, and takes a
	// few to make a dropped buffer show up as an all-fresh sequence.
	seenPooled := false
	for i := 0; i < 4; i++ {
		p := readBufPool.Get().(*[]byte)
		if cap(*p) == 128 {
			seenPooled = true
			break
		}
	}
	if !seenPooled {
		t.Error("the undersized buffer was not returned to the pool; it was allocated, " +
			"rejected, and dropped")
	}
}
