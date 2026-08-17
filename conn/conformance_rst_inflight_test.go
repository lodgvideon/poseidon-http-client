package conn

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC7540_Sec51_PeerRSTReleasesInflightSlot pins that a peer
// RST_STREAM on a stream whose local half is still open (opened with
// SendHeaders(endStream=false), mid-upload) releases its inflight slot and
// evicts it from the registry immediately.
//
// RFC 7540 §5.1: RST_STREAM moves the stream to "closed" in both directions,
// so the client can no longer send on it and the slot it held under
// MAX_CONCURRENT_STREAMS must be returned. markStreamDone releases only when
// s.localEnded && s.remoteEnded; OnRSTStream set remoteEnded but not
// localEnded, so a half-open stream's slot leaked until the caller happened to
// call Stream.Close() — c.inflight climbed monotonically toward the cap and the
// streams map grew unbounded. The sibling onGoAwayReceived already forces
// localEnded=true for exactly this reason.
func TestConformance_RFC7540_Sec51_PeerRSTReleasesInflightSlot(t *testing.T) {
	c := newGoAwayConn()
	h := newConnHandler(c, hpack.NewDecoder())

	// A half-open stream: HEADERS sent without END_STREAM, no END_STREAM/RST
	// observed yet, holding one inflight slot.
	s := newStream(1, 8, c, 65535)
	s.id = 1
	c.streams[1] = s
	c.inflight++

	fh := frame.FrameHeader{Type: frame.FrameRSTStream, StreamID: 1}

	err := h.OnRSTStream(fh, frame.ErrCodeCancel)

	require.NoError(t, err, "OnRSTStream")
	assert.Zerof(t, c.inflight, "inflight = %d, want 0 — a peer RST_STREAM on a half-open stream must "+
		"release its slot, not hold it until Stream.Close()", c.inflight)
	assert.NotContains(t, c.streams, uint32(1), "stream 1 still registered after RST_STREAM; want evicted")
	// The caller still observes the reset.
	select {
	case ev := <-s.events:
		assert.Equalf(t, EventReset, ev.Type, "event = %+v, want EventReset(CANCEL)", ev)
		assert.Equalf(t, frame.ErrCodeCancel, ev.RSTCode, "event = %+v, want EventReset(CANCEL)", ev)
	case <-time.After(time.Second):
		assert.Fail(t, "no reset event delivered")
	}
}
