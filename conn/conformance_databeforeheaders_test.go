package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC7540_Sec8_1_DataBeforeResponseHeaders_Malformed pins that a
// DATA frame arriving before the response HEADERS is a malformed message. RFC 7540
// §8.1 defines a response as HEADERS followed by optional DATA; a body with no
// preceding response is not one, and §8.1.2.6 routes a malformed message to a
// stream error of type PROTOCOL_ERROR. Without this guard client.Do returned
// (Response{Status:0, Body:<server bytes>}, nil) — a nil error carrying an
// attacker-controlled body and an impossible status code. The HTTP/3 sibling has
// always rejected DATA before the response.
func TestConformance_RFC7540_Sec8_1_DataBeforeResponseHeaders_Malformed(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	m.addStream(1) // fresh stream: no response HEADERS received yet

	fh := frame.FrameHeader{Type: frame.FrameData, StreamID: 1, Length: 5, Flags: frame.FlagDataEndStream}
	err := h.OnData(fh, []byte("hello"), 0)
	wantStreamProtocolError(t, err, "DATA before response HEADERS")
}

// TestConformance_RFC7540_Sec8_1_DataAfterResponseHeaders_Accepted is the
// over-rejection guard: once the response HEADERS have arrived, DATA is a valid
// body and must be delivered.
func TestConformance_RFC7540_Sec8_1_DataAfterResponseHeaders_Accepted(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	if err := deliverBlock(t, h, 1, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}, false); err != nil {
		t.Fatalf("response HEADERS rejected: %v", err)
	}
	<-s.events // consume the EventHeaders

	fh := frame.FrameHeader{Type: frame.FrameData, StreamID: 1, Length: 5, Flags: frame.FlagDataEndStream}
	if err := h.OnData(fh, []byte("hello"), 0); err != nil {
		t.Fatalf("DATA after response HEADERS rejected: %v — a body following the response is valid", err)
	}
	ev := <-s.events
	if ev.Type != EventData {
		t.Fatalf("event = %s, want EventData", ev.Type)
	}
}
