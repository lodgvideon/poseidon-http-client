package frame

// RFC 9113 §6.10 field-block continuity is frame-type-agnostic: a PUSH_PROMISE
// without END_HEADERS opens a field block exactly as a HEADERS without
// END_HEADERS does, so an interleaved frame during a spanning PUSH_PROMISE block
// is the same connection error. The conn-layer §6.10 tests only open the block
// with HEADERS; these pin the PUSH_PROMISE case at the Framer.

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// ppOpenBlock is a PUSH_PROMISE on stream 1 promising id 2, WITHOUT END_HEADERS:
// it opens a field block that only a CONTINUATION on stream 1 may continue.
func ppOpenBlock() []byte {
	return frameBytes(5, FramePushPromise, 0, 1, []byte{0, 0, 0, 2, 0x82})
}

// TestConformance_RFC9113_Sec6_10_PushPromiseBlockInterleave_Rejected pins that a
// non-CONTINUATION frame after an unterminated PUSH_PROMISE block is a §6.10
// violation (ErrContinuationExpected), which the reader loop maps to a connection
// error PROTOCOL_ERROR.
func TestConformance_RFC9113_Sec6_10_PushPromiseBlockInterleave_Rejected(t *testing.T) {
	data := frameBytes(1, FrameData, 0, 1, []byte{0x00}) // a DATA frame, not the required CONTINUATION
	fr := NewFramer(nil, bytes.NewReader(append(ppOpenBlock(), data...)))
	h := &recordingHandler{}
	_, err := fr.ReadFrame(context.Background(), h)
	require.NoError(t, err, "PUSH_PROMISE (opens block)")

	_, err = fr.ReadFrame(context.Background(), h)

	require.ErrorIsf(t, err, ErrContinuationExpected,
		"interleaved DATA after an open PUSH_PROMISE block: err = %v, want ErrContinuationExpected (§6.10)", err)
}

// TestConformance_RFC9113_Sec6_10_PushPromiseBlockContinuation_Accepted is the
// over-rejection guard: the conformant spanning case — a CONTINUATION with
// END_HEADERS on the same stream — must reassemble cleanly.
func TestConformance_RFC9113_Sec6_10_PushPromiseBlockContinuation_Accepted(t *testing.T) {
	cont := frameBytes(1, FrameContinuation, FlagContinuationEndHeaders, 1, []byte{0x82})
	fr := NewFramer(nil, bytes.NewReader(append(ppOpenBlock(), cont...)))
	h := &recordingHandler{}
	_, err := fr.ReadFrame(context.Background(), h)
	require.NoError(t, err, "PUSH_PROMISE")

	_, err = fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err,
		"CONTINUATION on the same stream: %v — a conformant spanning PUSH_PROMISE block must be accepted", err)
}
