package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC7540_Sec6_6_PushPromiseSpanningContinuation_Reassembled pins
// that a PUSH_PROMISE header block split across CONTINUATION frames (legal per RFC
// 7540 §6.6 / §6.10) is reassembled before decoding, not decoded truncated.
// OnPushPromise used to decode its fragment unconditionally, ignoring
// END_HEADERS: a spanning promise (a conformant server splitting a large one, or
// a hostile peer clearing END_HEADERS) decoded as an incomplete HPACK block,
// returning COMPRESSION_ERROR that tore the whole connection down.
func TestConformance_RFC7540_Sec6_6_PushPromiseSpanningContinuation_Reassembled(t *testing.T) {
	m := newFakeStreamMap()
	m.pushEnabled = true
	h := newConnHandler(m, hpack.NewDecoder())
	parent := m.addStream(1)
	// The push accept path checks the promise's :authority against the request
	// that triggered it (RFC 9113 §8.4); a real parent carries this from its
	// request HEADERS, so mirror that for the fake.
	parent.authorityBuf = []byte("example.com")

	enc := hpack.NewEncoder()
	block := enc.EncodeBlock(nil, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/pushed.css")},
	})
	mid := len(block) / 2

	// PUSH_PROMISE fragment without END_HEADERS — must buffer, not decode.
	if err := h.OnPushPromise(frame.FrameHeader{
		Type: frame.FramePushPromise, StreamID: 1, Flags: 0,
	}, 2, frame.HeaderBlock(block[:mid]), 0); err != nil {
		t.Fatalf("PUSH_PROMISE fragment returned error: %v (it must be buffered, not decoded)", err)
	}

	// CONTINUATION completes the block.
	if err := h.OnContinuation(frame.FrameHeader{
		Type: frame.FrameContinuation, StreamID: 1, Flags: frame.FlagContinuationEndHeaders,
	}, frame.HeaderBlock(block[mid:])); err != nil {
		t.Fatalf("PUSH_PROMISE CONTINUATION returned error: %v — a spanning promise must "+
			"reassemble, not decode truncated and tear the connection down", err)
	}

	ev := <-parent.events
	if ev.Type != EventPushPromise {
		t.Fatalf("event = %s, want EventPushPromise", ev.Type)
	}
	if ev.PushStreamID != 2 {
		t.Fatalf("PushStreamID = %d, want 2", ev.PushStreamID)
	}
	if len(ev.Headers) != 4 {
		t.Fatalf("promised header count = %d, want 4 — the reassembled block decoded short",
			len(ev.Headers))
	}
	if ev.Slab != nil {
		GetHeaderSlabPool().Put(ev.Slab)
	}
}
