package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC7541_Sec2_2_EvictedStreamHeaderBlockKeepsDecoderSynced pins
// that a HEADERS block for a stream that is no longer in the registry (reset or
// completed and evicted, RFC 7540 §5.1) is still decoded, keeping the
// connection-wide HPACK decoder in sync. RFC 7541 §2.2: HPACK is a stateful
// connection-wide context. Skipping the decode of a block that used
// Literal-with-Incremental-Indexing leaves the decoder's dynamic table missing
// the entry the peer's encoder added, so a later block that references it by
// index resolves the wrong field or fails with COMPRESSION_ERROR — a
// connection-wide teardown from one evicted stream.
func TestConformance_RFC7541_Sec2_2_EvictedStreamHeaderBlockKeepsDecoderSynced(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	enc := hpack.NewEncoder()

	headers := func() []hpack.HeaderField {
		return []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
			{Name: []byte("x-cached"), Value: []byte("yes")}, // non-static: incremental indexing
		}
	}

	// Stream 1 is NOT registered — the evicted/reset case. Its block inserts
	// x-cached into the encoder's (and, once decoded, the decoder's) dynamic table.
	block1 := enc.EncodeBlock(nil, headers())
	require.NoError(t, h.OnHeaders(frame.FrameHeader{
		Type: frame.FrameHeaders, StreamID: 1,
		Flags: frame.FlagHeadersEndHeaders | frame.FlagHeadersEndStream,
	}, block1, nil, 0), "evicted-stream HEADERS returned error")
	// Live stream 3. The encoder now references x-cached by dynamic index; the
	// decoder can resolve it only if it decoded block1.
	s := m.addStream(3)
	block2 := enc.EncodeBlock(nil, headers())

	err := h.OnHeaders(frame.FrameHeader{
		Type: frame.FrameHeaders, StreamID: 3,
		Flags: frame.FlagHeadersEndHeaders | frame.FlagHeadersEndStream,
	}, block2, nil, 0)

	require.NoErrorf(t, err, "live-stream HEADERS returned error: %v — the decoder desynced because "+
		"the evicted stream's block was dropped undecoded", err)
	ev := <-s.events
	var found bool
	for _, f := range ev.Headers {
		if string(f.Name) == "x-cached" && string(f.Value) == "yes" {
			found = true
		}
	}
	assert.Truef(t, found, "x-cached: yes missing from stream 3 response — the dynamic-index "+
		"reference resolved wrongly, so the decoder was out of sync: %+v", ev.Headers)
}
