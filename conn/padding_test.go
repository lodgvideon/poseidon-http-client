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
