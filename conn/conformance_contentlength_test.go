package conn

// Conformance tests for RFC 7540 §8.1.2.6's Content-Length rule: "A request or
// response is also malformed if the value of a content-length header field does
// not equal the sum of the DATA frame payload lengths that form the body."
//
// http1 enforces its equivalent ("premature EOF: got %d of %d bytes") and http3
// enforces RFC 9114 §4.1.2's identical rule (contentLengthMatches). HTTP/2, the
// oldest of the three, checked nothing — a server could declare any length and
// send any number of DATA bytes, and the caller got a response whose declared
// framing contradicted its own body with no error. §8.1.2.6: "Clients MUST NOT
// accept a malformed response ... Malformed requests or responses that are
// detected MUST be treated as a stream error (Section 5.4.2) of type
// PROTOCOL_ERROR."
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// respFields builds a final response block: :status plus the given name/value
// pairs.
func respFields(status string, kv ...string) []hpack.HeaderField {
	out := []hpack.HeaderField{{Name: []byte(":status"), Value: []byte(status)}}
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, hpack.HeaderField{Name: []byte(kv[i]), Value: []byte(kv[i+1])})
	}
	return out
}

// deliverData feeds one DATA frame; end sets END_STREAM.
func deliverData(h *connHandler, id uint32, payload []byte, end bool) error {
	fh := frame.FrameHeader{Type: frame.FrameData, Length: uint32(len(payload)), StreamID: id}
	if end {
		fh.Flags = frame.FlagDataEndStream
	}
	return h.OnData(fh, payload, 0)
}

// drain empties a stream's event channel so a later OnData/emit does not block.
func drain(s *Stream) {
	for len(s.events) > 0 {
		ev := <-s.events
		if ev.DataSlab != nil {
			dataBufPool.Put(ev.DataSlab)
		}
	}
}

// wantCLStreamError asserts the §8.1.2.6 remedy: a STREAM PROTOCOL_ERROR.
func wantCLStreamError(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: accepted, want a stream error — RFC 7540 §8.1.2.6: a response "+
			"whose Content-Length does not equal the DATA received is malformed and "+
			"\"Clients MUST NOT accept a malformed response\"", what)
	}
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("%s: error = %v (%T), want *StreamError — §8.1.2.6 requires a STREAM "+
			"error, so one malformed response does not kill the pooled connection", what, err, err)
	}
	if se.Code != frame.ErrCodeProtocolError {
		t.Errorf("%s: code = %v, want PROTOCOL_ERROR", what, se.Code)
	}
}

// TestConformance_RFC7540_Sec8_1_2_6_ContentLengthMismatch_Malformed pins that a
// declared Content-Length not equal to the DATA received is a stream error.
func TestConformance_RFC7540_Sec8_1_2_6_ContentLengthMismatch_Malformed(t *testing.T) {
	t.Run("over_long_body", func(t *testing.T) {
		m := newFakeStreamMap()
		m.bufSize = 64
		h := newConnHandler(m, hpack.NewDecoder())
		s := m.addStream(1)
		if err := deliverBlock(t, h, 1, respFields("200", "content-length", "5"), false); err != nil {
			t.Fatalf("headers: %v", err)
		}
		drain(s)
		// Declared 5, send 10 with END_STREAM.
		wantCLStreamError(t, deliverData(h, 1, make([]byte, 10), true), "declared 5, received 10")
	})

	t.Run("short_body_truncation", func(t *testing.T) {
		m := newFakeStreamMap()
		m.bufSize = 64
		h := newConnHandler(m, hpack.NewDecoder())
		s := m.addStream(1)
		if err := deliverBlock(t, h, 1, respFields("200", "content-length", "10"), false); err != nil {
			t.Fatalf("headers: %v", err)
		}
		drain(s)
		wantCLStreamError(t, deliverData(h, 1, make([]byte, 5), true), "declared 10, received 5")
	})

	t.Run("declared_body_but_empty", func(t *testing.T) {
		m := newFakeStreamMap()
		m.bufSize = 64
		h := newConnHandler(m, hpack.NewDecoder())
		m.addStream(1)
		// END_STREAM on the HEADERS block: 0 DATA bytes, Content-Length 5.
		wantCLStreamError(t, deliverBlock(t, h, 1, respFields("200", "content-length", "5"), true),
			"declared 5, received 0")
	})

	t.Run("invalid_content_length_with_body", func(t *testing.T) {
		// A non-1*DIGIT Content-Length is a legal HPACK literal but an invalid
		// declaration; §8.6 makes it 1*DIGIT, so it is malformed on its own once
		// the body confirms the framing was consulted.
		m := newFakeStreamMap()
		m.bufSize = 64
		h := newConnHandler(m, hpack.NewDecoder())
		s := m.addStream(1)
		if err := deliverBlock(t, h, 1, respFields("200", "content-length", "notanumber"), false); err != nil {
			t.Fatalf("headers: %v", err)
		}
		drain(s)
		wantCLStreamError(t, deliverData(h, 1, make([]byte, 5), true), "content-length notanumber")
	})
}

// TestConformance_RFC7540_Sec8_1_2_6_ContentLengthMatch_Accepted is the
// over-rejection guard: an exact match, and every exempt shape, must be accepted.
func TestConformance_RFC7540_Sec8_1_2_6_ContentLengthMatch_Accepted(t *testing.T) {
	t.Run("exact_match", func(t *testing.T) {
		m := newFakeStreamMap()
		m.bufSize = 64
		h := newConnHandler(m, hpack.NewDecoder())
		s := m.addStream(1)
		if err := deliverBlock(t, h, 1, respFields("200", "content-length", "5"), false); err != nil {
			t.Fatalf("headers: %v", err)
		}
		drain(s)
		if err := deliverData(h, 1, make([]byte, 5), true); err != nil {
			t.Errorf("declared 5, received 5: %v — an exact match is not malformed", err)
		}
	})

	t.Run("no_content_length", func(t *testing.T) {
		m := newFakeStreamMap()
		m.bufSize = 64
		h := newConnHandler(m, hpack.NewDecoder())
		s := m.addStream(1)
		if err := deliverBlock(t, h, 1, respFields("200"), false); err != nil {
			t.Fatalf("headers: %v", err)
		}
		drain(s)
		if err := deliverData(h, 1, make([]byte, 12345), true); err != nil {
			t.Errorf("no Content-Length: %v — nothing to check against", err)
		}
	})

	t.Run("204_with_content_length_exempt", func(t *testing.T) {
		m := newFakeStreamMap()
		m.bufSize = 64
		h := newConnHandler(m, hpack.NewDecoder())
		m.addStream(1)
		// §8.1.2.6: a no-payload status "can have a non-zero content-length header
		// field, even though no content is included in DATA frames".
		if err := deliverBlock(t, h, 1, respFields("204", "content-length", "5"), true); err != nil {
			t.Errorf("204 + Content-Length: %v — no-payload statuses are exempt from the DATA check", err)
		}
	})

	t.Run("head_response_content_length_exempt", func(t *testing.T) {
		m := newFakeStreamMap()
		m.bufSize = 64
		h := newConnHandler(m, hpack.NewDecoder())
		s := m.addStream(1)
		s.reqIsHead = true // the request was HEAD (RFC 9110 §9.3.2)
		// A HEAD response carries the GET Content-Length but no body.
		if err := deliverBlock(t, h, 1, respFields("200", "content-length", "100"), true); err != nil {
			t.Errorf("HEAD response + Content-Length 100, no body: %v — HEAD responses are exempt", err)
		}
	})
}
