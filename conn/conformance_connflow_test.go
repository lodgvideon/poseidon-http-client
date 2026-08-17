package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC7540_Sec6_9_DataOnEvictedStream_AccountsConnWindow pins that
// a DATA frame for a stream no longer in the registry — reset by us, or already
// closed and evicted — still charges the connection-level flow-control window.
// RFC 7540 §6.9: "A receiver that receives a flow-controlled frame MUST always
// account for its contribution against the connection flow-control window ... This
// is necessary even if the frame is in error." §5.1: "Flow-controlled frames
// (i.e., DATA) received after sending RST_STREAM are counted toward the connection
// flow-control window."
//
// OnData used to return nil for an unknown stream, charging nothing, so the peer's
// connection send window shrank permanently on every cancelled stream and a
// long-lived pooled connection eventually stalled.
func TestConformance_RFC7540_Sec6_9_DataOnEvictedStream_AccountsConnWindow(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())

	// No stream is registered for id 5: the reset/evicted case.
	err := h.OnData(frame.FrameHeader{Type: frame.FrameData, StreamID: 5, Length: 100}, make([]byte, 100), 0)

	require.NoError(t, err, "OnData")
	assert.EqualValuesf(t, 100, m.connRecvOnly,
		"connection window charged %d bytes, want 100 — DATA on an evicted "+
			"stream must still count against the connection flow-control window "+
			"(RFC 7540 §6.9, §5.1), or the peer's send window leaks", m.connRecvOnly)
}
