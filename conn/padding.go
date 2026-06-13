package conn

import "math/rand"

// PaddingStrategy controls how HTTP/2 frame padding is applied to outbound
// HEADERS and DATA frames. Padding randomises frame sizes to hinder traffic
// analysis (RFC 7540 §4.2, §6.1, §6.2).
//
// The zero value disables padding (default).
type PaddingStrategy struct {
	// Min is the minimum number of padding bytes (0–255).
	Min uint8

	// Max is the maximum number of padding bytes (0–255).
	// If Max < Min, Min is used. If both are 0, padding is disabled.
	Max uint8

	// DataOnly, when true, applies padding only to DATA frames.
	// When false (default), padding is applied to both HEADERS and DATA.
	DataOnly bool
}

// Enabled reports whether padding is active.
func (p PaddingStrategy) Enabled() bool {
	return p.Max > 0
}

// PadBytes returns a random padding length in [min, max].
// Returns 0 if padding is disabled.
func (p PaddingStrategy) PadBytes() uint8 {
	if !p.Enabled() {
		return 0
	}
	mx := p.Max
	if mx < p.Min {
		mx = p.Min
	}
	if mx == p.Min {
		return mx
	}
	return p.Min + uint8(rand.Intn(int(mx-p.Min)+1))
}

// ForHeaders returns the padding length for a HEADERS frame,
// respecting the DataOnly flag.
func (p PaddingStrategy) ForHeaders() uint8 {
	if p.DataOnly {
		return 0
	}
	return p.PadBytes()
}

// ForData returns the padding length for a DATA frame.
func (p PaddingStrategy) ForData() uint8 {
	return p.PadBytes()
}
