package http1_test

// Cost of obs-fold accumulation.
//
// maxHeaderListBytes bounds the header BYTES a server may send. It says nothing
// about the work those bytes buy — and the first cut of the §5.2 unfolding
// joined each continuation with `field += " " + rest`, which reallocates and
// copies everything accumulated so far on every fold. Bounded input, quadratic
// cost:
//
//	wire  86 KB ->    83 MiB allocated
//	wire 344 KB ->  1281 MiB allocated
//	wire 688 KB ->  5067 MiB allocated
//
// Every one of those responses is legal and under the cap. This test is the
// bound that the byte cap cannot express.

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// foldedResponse builds a response whose single header value is continued over
// n obs-fold lines of 40 octets each — each fold tiny and legal, the block well
// under maxHeaderListBytes.
func foldedResponse(n int) string {
	var sb strings.Builder
	sb.WriteString("HTTP/1.1 200 OK\r\nX-A: v\r\n")
	for i := 0; i < n; i++ {
		sb.WriteString(" ")
		sb.WriteString(strings.Repeat("p", 40))
		sb.WriteString("\r\n")
	}
	sb.WriteString("Content-Length: 0\r\n\r\n")
	return sb.String()
}

// allocsForFolds returns bytes allocated parsing a response with n folds.
func allocsForFolds(t *testing.T, n int) uint64 {
	t.Helper()
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	ex := wireExchange(t, "GET", foldedResponse(n))
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse with %d folds: %v", n, err)
	}
	runtime.ReadMemStats(&m1)
	return m1.TotalAlloc - m0.TotalAlloc
}

// TestObsFoldAccumulation_IsLinearInBytesReceived pins that doubling the folded
// bytes does not more than double the memory spent joining them.
//
// The quadratic version overshot this by ~4x per doubling; the assertion allows
// 2.5x, which is loose enough for allocator noise and the fixed per-response
// overhead, and nowhere near loose enough to admit O(n²).
func TestObsFoldAccumulation_IsLinearInBytesReceived(t *testing.T) {
	const small, large = 4000, 8000

	a := allocsForFolds(t, small)
	b := allocsForFolds(t, large)
	ratio := float64(b) / float64(a)
	t.Logf("%d folds: %d KiB   %d folds: %d KiB   ratio=%.2fx (linear ~2.0, quadratic ~4.0)",
		small, a>>10, large, b>>10, ratio)

	if ratio > 2.5 {
		t.Errorf("allocations grew %.2fx for a 2x larger folded header (%d KiB -> %d KiB); "+
			"want <= 2.5x. Joining N continuations must cost O(total bytes), not O(N x total): "+
			"maxHeaderListBytes caps the bytes a server sends, so a superlinear join lets a "+
			"legal, capped response buy unbounded work",
			ratio, a>>10, b>>10)
	}
}

// TestObsFoldAccumulation_UnfoldsCorrectlyAtScale is the control: the cheap path
// must still produce the right value, so the bound above cannot be met by
// dropping folds on the floor.
func TestObsFoldAccumulation_UnfoldsCorrectlyAtScale(t *testing.T) {
	const n = 1000
	ex := wireExchange(t, "GET", foldedResponse(n))
	_, hdrs, err := ex.ReadResponse(context.Background())
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	got, ok := fieldValue(hdrs, "x-a")
	if !ok {
		t.Fatal("x-a missing")
	}
	want := "v" + strings.Repeat(" "+strings.Repeat("p", 40), n)
	if got != want {
		t.Errorf("x-a length = %d, want %d — the folds were not all joined "+
			"(first divergence matters more than the bytes: "+strconv.Itoa(len(got))+" vs "+
			strconv.Itoa(len(want))+")", len(got), len(want))
	}
}
