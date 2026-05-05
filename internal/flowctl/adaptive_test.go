package flowctl

import "testing"

func TestAdaptiveSizer_GrowsOnHighUse(t *testing.T) {
	s := NewAdaptive(1000, 64, 4<<20)
	// consumed > 50% → grow
	got := s.Decide(800, 1000)
	if got <= 1000 {
		t.Fatalf("expected grow, got %d", got)
	}
}

func TestAdaptiveSizer_ShrinksOnLowUse(t *testing.T) {
	s := NewAdaptive(1000, 64, 4<<20)
	got := s.Decide(100, 1000)
	if got >= 1000 {
		t.Fatalf("expected shrink, got %d", got)
	}
}

func TestAdaptiveSizer_BoundedByMaxOnGrow(t *testing.T) {
	s := NewAdaptive(900, 64, 1000)
	got := s.Decide(900, 900)
	if got != 1000 {
		t.Fatalf("expected cap at 1000, got %d", got)
	}
}

func TestAdaptiveSizer_BoundedByMinOnShrink(t *testing.T) {
	s := NewAdaptive(100, 80, 4<<20)
	got := s.Decide(0, 100)
	if got != 80 {
		t.Fatalf("expected floor at 80, got %d", got)
	}
}

func TestAdaptiveSizer_StableInMiddleBand(t *testing.T) {
	s := NewAdaptive(1000, 64, 4<<20)
	got := s.Decide(400, 1000) // 40% — between 25% and 50%
	if got != 1000 {
		t.Fatalf("expected unchanged, got %d", got)
	}
}
