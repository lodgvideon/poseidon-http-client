package flowctl

// AdaptiveSizer resizes a flow-control window cap based on consumption
// patterns. Used on the receive side to grow the window when the consumer
// drains data fast, shrink when slow.
type AdaptiveSizer struct {
	current uint32
	min     uint32
	max     uint32
}

// NewAdaptive constructs a sizer with initial=current cap, bounded by min/max.
func NewAdaptive(initial, min, max uint32) *AdaptiveSizer {
	if min > initial {
		min = initial
	}
	if max < initial {
		max = initial
	}
	return &AdaptiveSizer{current: initial, min: min, max: max}
}

// Current returns the current cap.
func (s *AdaptiveSizer) Current() uint32 { return s.current }

// Decide returns the next cap based on how much the consumer drained
// since the last decision relative to capacity.
//   - usedFrac > 0.5 → grow by 1.5x (capped at max)
//   - usedFrac < 0.25 → shrink by 0.75x (floored at min)
//   - otherwise unchanged
func (s *AdaptiveSizer) Decide(consumedSinceLast, capacity uint32) uint32 {
	if capacity == 0 {
		return s.current
	}
	usedFrac := float64(consumedSinceLast) / float64(capacity)
	switch {
	case usedFrac > 0.5:
		newCap := uint32(float64(s.current) * 1.5)
		if newCap > s.max {
			newCap = s.max
		}
		s.current = newCap
	case usedFrac < 0.25:
		newCap := uint32(float64(s.current) * 0.75)
		if newCap < s.min {
			newCap = s.min
		}
		s.current = newCap
	}
	return s.current
}
