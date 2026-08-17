package http3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_DoStream_StreamedBodyNotCappedByRetainedLimit pins that a
// DATA chunk handed off on the streaming path (DoStream) does not count against
// maxResponseBytes, the cap on bytes retained together in memory. dispatchFrame
// added every DATA payload to rb.total before the onData handoff, so a streamed
// response whose body summed past the cap (128 MiB) was aborted with
// ErrResponseTooLarge even though only one frame is ever retained — contradicting
// the cap's own rationale and the BodyReader contract (peak retained memory is one
// frame, not the whole body). The buffered Do path, which does retain the body,
// stays capped.
func TestConformance_DoStream_StreamedBodyNotCappedByRetainedLimit(t *testing.T) {
	c := &Client{}
	var streamed int
	rb := &respBuilder{
		resp:   &Response{Status: 200},
		onData: func(p []byte) error { streamed += len(p); return nil },
		total:  maxResponseBytes - 10, // already near the retained cap
	}

	// A DATA frame that would push rb.total past maxResponseBytes if it were
	// counted — but on the streaming path it is handed off, not retained.
	err := c.dispatchFrame(rb, FrameData, make([]byte, 1000))

	require.NoErrorf(t, err,
		"dispatchFrame(streaming DATA past the retained cap) = %v, want nil — a "+
			"handed-off chunk is not retained and must not count toward maxResponseBytes", err)
	assert.Equalf(t, 1000, streamed,
		"streamed = %d bytes, want 1000 handed to the BodyReader", streamed)
}

// TestConformance_Do_BufferedBodyStillCapped is the guard for the other half: the
// buffered Do path (onData nil) accumulates the body in memory, so it must stay
// capped at maxResponseBytes.
func TestConformance_Do_BufferedBodyStillCapped(t *testing.T) {
	c := &Client{}
	rb := &respBuilder{resp: &Response{Status: 200}, total: maxResponseBytes - 10} // onData nil: buffered

	err := c.dispatchFrame(rb, FrameData, make([]byte, 1000))

	assert.Equalf(t, ErrResponseTooLarge, err,
		"dispatchFrame(buffered DATA past the cap) = %v, want ErrResponseTooLarge — "+
			"the buffered body is retained and must stay capped", err)
}
