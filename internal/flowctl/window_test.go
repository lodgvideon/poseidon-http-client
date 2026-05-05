package flowctl

import "testing"

func TestFlowWindow_TryConsume(t *testing.T) {
	w := New(100)
	if !w.TryConsume(50) {
		t.Fatal("first 50 should fit")
	}
	if !w.TryConsume(50) {
		t.Fatal("second 50 should fit")
	}
	if w.TryConsume(1) {
		t.Fatal("window depleted; further consume must fail")
	}
}

func TestFlowWindow_Replenish(t *testing.T) {
	w := New(0)
	if got := w.Replenish(100); got != 100 {
		t.Fatalf("replenish = %d, want 100", got)
	}
	if !w.TryConsume(100) {
		t.Fatal("post-replenish 100 should fit")
	}
}

func TestFlowWindow_AdjustInitial_Positive(t *testing.T) {
	w := New(100)
	w.AdjustInitial(50)
	if w.Available() != 150 {
		t.Fatalf("available = %d, want 150", w.Available())
	}
	if !w.TryConsume(150) {
		t.Fatal("150 after +50 adjust should fit")
	}
}

func TestFlowWindow_AdjustInitial_Negative(t *testing.T) {
	w := New(10)
	if !w.TryConsume(10) {
		t.Fatal("consume 10 should fit")
	}
	w.AdjustInitial(-20)
	if w.Available() >= 0 {
		t.Fatalf("expected negative on overshoot; got %d", w.Available())
	}
}

func BenchmarkFlowWindow_TryConsume(b *testing.B) {
	w := New(int32(1) << 30)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.TryConsume(1)
	}
}
