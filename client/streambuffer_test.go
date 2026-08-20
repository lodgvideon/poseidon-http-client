package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDefaultStreamEventBuffer pins the sizing and, more importantly, the
// reason for its shape: the divisor is what stops a caller who raised
// MaxFrameSize for throughput from silently multiplying what one stream can
// pin, since every queued DATA event holds a buffer of up to one frame.
func TestDefaultStreamEventBuffer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		frameSize uint32
		want      int
		retained  string
	}{
		{"unset frame size uses conn's advertised default", 0, 64, "1 MiB"},
		{"default 16 KiB", 16384, 64, "1 MiB"},
		{"32 KiB halves the slots", 32768, 32, "1 MiB"},
		{"1 MiB frames hit the floor", 1 << 20, 16, "16 MiB — the caller opted in"},
		{"tiny frames hit the cap", 512, 64, "32 KiB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := defaultStreamEventBuffer(tc.frameSize)

			assert.Equalf(t, tc.want, got,
				"defaultStreamEventBuffer(%d) = %d, want %d (retains %s per stream)",
				tc.frameSize, got, tc.want, tc.retained)
		})
	}
}

// TestDefaultStreamEventBuffer_CoversTheChunkedShape is the regression this
// exists for: the 10-event response from #344 must fit at the default.
func TestDefaultStreamEventBuffer_CoversTheChunkedShape(t *testing.T) {
	const eventsInTheReport = 10 // HEADERS + 8 flushed chunks + the END marker

	got := defaultStreamEventBuffer(0)

	assert.GreaterOrEqualf(t, got, eventsInTheReport,
		"default is %d slots; the reported response needs %d — a caller draining promptly "+
			"would still overflow the buffer and take an RST(CANCEL)", got, eventsInTheReport)
}
