package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaddingStrategy_Disabled(t *testing.T) {
	p := PaddingStrategy{}

	enabled := p.Enabled()

	assert.False(t, enabled, "zero value should be disabled")
	assert.EqualValuesf(t, 0, p.PadBytes(), "PadBytes = %d, want 0", p.PadBytes())
	assert.EqualValuesf(t, 0, p.ForHeaders(), "ForHeaders = %d, want 0", p.ForHeaders())
	assert.EqualValuesf(t, 0, p.ForData(), "ForData = %d, want 0", p.ForData())
}

func TestPaddingStrategy_Fixed(t *testing.T) {
	p := PaddingStrategy{Min: 10, Max: 10}

	enabled := p.Enabled()

	assert.True(t, enabled, "should be enabled")
	for i := 0; i < 100; i++ {
		got := p.PadBytes()
		assert.EqualValuesf(t, 10, got, "PadBytes = %d, want 10", got)
	}
}

func TestPaddingStrategy_Range(t *testing.T) {
	p := PaddingStrategy{Min: 5, Max: 20}

	enabled := p.Enabled()

	assert.True(t, enabled, "should be enabled")
	for i := 0; i < 200; i++ {
		got := p.PadBytes()
		assert.GreaterOrEqualf(t, got, uint8(5), "PadBytes = %d, want [5, 20]", got)
		assert.LessOrEqualf(t, got, uint8(20), "PadBytes = %d, want [5, 20]", got)
	}
}

func TestPaddingStrategy_MaxLessThanMin(t *testing.T) {
	p := PaddingStrategy{Min: 15, Max: 5}

	// Max < Min → Min is used.
	for i := 0; i < 50; i++ {
		got := p.PadBytes()

		assert.EqualValuesf(t, 15, got, "PadBytes = %d, want 15 (Min)", got)
	}
}

func TestPaddingStrategy_DataOnly(t *testing.T) {
	p := PaddingStrategy{Min: 8, Max: 16, DataOnly: true}

	forHeaders, forData := p.ForHeaders(), p.ForData()

	assert.EqualValuesf(t, 0, forHeaders, "ForHeaders = %d, want 0 when DataOnly", forHeaders)
	assert.NotZero(t, forData, "ForData should be non-zero when enabled")
}

func TestPaddingStrategy_BothFrames(t *testing.T) {
	p := PaddingStrategy{Min: 4, Max: 12}

	forHeaders, forData := p.ForHeaders(), p.ForData()

	assert.NotZero(t, forHeaders, "ForHeaders should be non-zero")
	assert.NotZero(t, forData, "ForData should be non-zero")
}

// TestPaddingStrategy_MinWithoutMax pins the equivalence class the table above
// never sampled: Min > 0 with Max == 0. The two documented rules disagreed about
// it — "If both are 0, padding is disabled" implies {10, 0} is enabled, while
// Enabled() returns Max > 0, which makes it disabled — so the behaviour was
// undecided by the tests and mis-stated in prose (#826). Max is the switch, and
// that is what is pinned here: the current answer, not a new one.
func TestPaddingStrategy_MinWithoutMax(t *testing.T) {
	p := PaddingStrategy{Min: 10, Max: 0}

	enabled := p.Enabled()

	assert.False(t, enabled,
		"PaddingStrategy{Min:10, Max:0}.Enabled() = true; Max is the switch, so a strategy "+
			"naming only a minimum must stay off rather than start padding every frame on a "+
			"connection whose owner never chose a maximum")
	assert.EqualValuesf(t, 0, p.PadBytes(), "PadBytes = %d, want 0 while disabled", p.PadBytes())
	assert.EqualValuesf(t, 0, p.ForHeaders(), "ForHeaders = %d, want 0 while disabled", p.ForHeaders())
	assert.EqualValuesf(t, 0, p.ForData(), "ForData = %d, want 0 while disabled", p.ForData())
}

// TestPaddingStrategy_RangeIsInclusiveAtBothEnds is what "a random padding
// length in [min, max]" has to mean. TestPaddingStrategy_Range asserts only
// 5 <= got <= 20 over 200 draws, which a generator that can never emit Max
// satisfies — and Max is the end an operator sizing frames against
// MAX_FRAME_SIZE budgets against (#826).
func TestPaddingStrategy_RangeIsInclusiveAtBothEnds(t *testing.T) {
	const (
		lo = uint8(5)
		hi = uint8(20)
		// 200 draws from a 16-wide range: the chance a given end is never drawn
		// is (15/16)^200, about 3 in a million, so this is deterministic in
		// every sense that matters to a test suite.
		draws = 200
	)
	p := PaddingStrategy{Min: lo, Max: hi}

	seen := map[uint8]int{}
	for i := 0; i < draws; i++ {
		seen[p.PadBytes()]++
	}

	assert.NotZerof(t, seen[lo],
		"%d draws from [%d, %d] never produced %d; the lower bound is documented inclusive",
		draws, lo, hi, lo)
	assert.NotZerof(t, seen[hi],
		"%d draws from [%d, %d] never produced %d; the upper bound is documented inclusive, "+
			"and it is the one an operator budgets against MAX_FRAME_SIZE", draws, lo, hi, hi)
	for got := range seen {
		assert.Truef(t, got >= lo && got <= hi, "PadBytes = %d, outside [%d, %d]", got, lo, hi)
	}
}

// TestPaddingStrategy_ZeroMinIsItsOwnClass covers the last strategy class no
// test distinguished: Min == 0 with Max > 0, the only enabled strategy that may
// legitimately return 0, so some of its frames go out unpadded.
func TestPaddingStrategy_ZeroMinIsItsOwnClass(t *testing.T) {
	const hi = uint8(4)
	p := PaddingStrategy{Min: 0, Max: hi}

	seen := map[uint8]int{}
	for i := 0; i < 200; i++ {
		seen[p.PadBytes()]++
	}

	assert.True(t, p.Enabled(), "Max > 0 must be enabled whatever Min is")
	assert.NotZerof(t, seen[0],
		"200 draws from [0, %d] never produced 0; with Min unset the strategy has to be "+
			"able to emit an unpadded frame", hi)
	assert.NotZerof(t, seen[hi], "200 draws from [0, %d] never produced %d", hi, hi)
}
