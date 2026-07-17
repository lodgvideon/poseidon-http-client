package http3

import "testing"

// TestConformance_RFC9204_Sec42_DecoderStreamCoalescedBytesNotDropped pins that
// decoder-stream instruction bytes arriving COALESCED with the stream-type varint
// in one STREAM frame are not dropped. RFC 9204 §4.2 lets a server pipeline its
// first decoder instruction with the type byte; those bytes are already off the
// stream (Recv will not return them again), so routeUni must stash them into
// qpackDecBuf or the decoder stream desyncs.
//
// The coalesced instruction is a single Insert Count Increment of 1 (0x01) sent
// before any dynamic-table entry has been inserted — increasing the Known
// Received Count past the number of insertions is a decoder-stream error
// (RFC 9204 §4.4.3). That error is observable ONLY if the coalesced byte reached
// the parser; if routeUni drops it, serviceControl sees nothing and returns nil.
func TestConformance_RFC9204_Sec42_DecoderStreamCoalescedBytesNotDropped(t *testing.T) {
	control := &fakeStream{id: 3, recvChunks: [][]byte{
		serverControl([]Setting{{SettingQPACKMaxTableCapacity, qpackDynamicTableCapacity}}),
	}}
	// Stream-type varint for the decoder stream, coalesced with 0x01 (Insert
	// Count Increment by 1) in the same first chunk.
	server := &fakeStream{id: 7, recvChunks: [][]byte{qpackUniStream(StreamTypeQPACKDecoder, 0x01)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{control, server}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, qpackDynamicTableCapacity}})
	if err != nil {
		t.Fatal(err)
	}

	serr := client.serviceControl()
	if serr != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control — a coalesced Insert Count "+
			"Increment past the insert count is a decoder-stream error (RFC 9204 §4.4.3), "+
			"observable only if the coalesced bytes were not dropped", serr)
	}
	if conn.closeCode != H3QpackDecoderStreamError {
		t.Fatalf("close code = %#x, want QPACK_DECODER_STREAM_ERROR (%#x)", conn.closeCode, H3QpackDecoderStreamError)
	}
}
