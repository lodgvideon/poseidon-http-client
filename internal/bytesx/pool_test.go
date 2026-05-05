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
