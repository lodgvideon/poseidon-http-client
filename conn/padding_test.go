package conn

import (
	"testing"
)

func TestPaddingStrategy_Disabled(t *testing.T) {
	p := PaddingStrategy{}
	if p.Enabled() {
		t.Fatal("zero value should be disabled")
	}
	if p.PadBytes() != 0 {
		t.Fatalf("PadBytes = %d, want 0", p.PadBytes())
	}
	if p.ForHeaders() != 0 {
		t.Fatalf("ForHeaders = %d, want 0", p.ForHeaders())
	}
	if p.ForData() != 0 {
		t.Fatalf("ForData = %d, want 0", p.ForData())
	}
}

func TestPaddingStrategy_Fixed(t *testing.T) {
	p := PaddingStrategy{Min: 10, Max: 10}
	if !p.Enabled() {
		t.Fatal("should be enabled")
	}
	for i := 0; i < 100; i++ {
		got := p.PadBytes()
		if got != 10 {
			t.Fatalf("PadBytes = %d, want 10", got)
		}
	}
}

func TestPaddingStrategy_Range(t *testing.T) {
	p := PaddingStrategy{Min: 5, Max: 20}
	if !p.Enabled() {
		t.Fatal("should be enabled")
	}
	for i := 0; i < 200; i++ {
		got := p.PadBytes()
		if got < 5 || got > 20 {
			t.Fatalf("PadBytes = %d, want [5, 20]", got)
		}
	}
}

func TestPaddingStrategy_MaxLessThanMin(t *testing.T) {
	p := PaddingStrategy{Min: 15, Max: 5}
	// Max < Min → Min is used.
	for i := 0; i < 50; i++ {
		got := p.PadBytes()
		if got != 15 {
			t.Fatalf("PadBytes = %d, want 15 (Min)", got)
		}
	}
}

func TestPaddingStrategy_DataOnly(t *testing.T) {
	p := PaddingStrategy{Min: 8, Max: 16, DataOnly: true}
	if p.ForHeaders() != 0 {
		t.Fatalf("ForHeaders = %d, want 0 when DataOnly", p.ForHeaders())
	}
	if p.ForData() == 0 {
		t.Fatal("ForData should be non-zero when enabled")
	}
}

func TestPaddingStrategy_BothFrames(t *testing.T) {
	p := PaddingStrategy{Min: 4, Max: 12}
	if p.ForHeaders() == 0 {
		t.Fatal("ForHeaders should be non-zero")
	}
	if p.ForData() == 0 {
		t.Fatal("ForData should be non-zero")
	}
}
