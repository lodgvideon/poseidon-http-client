package conn

// Conformance tests for response-field validation, RFC 7540 §10.3 and §8.1.2.2.
//
// The receive path had none. emitHeaderBlock decoded an HPACK block straight
// into the fields handed to the caller, so a server could put anything an HPACK
// literal can encode into a response header and this client passed it on.
//
// HPACK is length-prefixed, so a CR/LF in a value cannot split anything on the
// HTTP/2 wire — which is exactly why this is easy to skip. §10.3 says why not to:
//
//	"HTTP/2 allows header field values that are not valid. While most of the
//	 values that can be encoded will not alter header field parsing, carriage
//	 return (CR, ASCII 0xd), line feed (LF, ASCII 0xa), and the zero character
//	 (NUL, ASCII 0x0) might be exploited by an attacker if they are translated
//	 verbatim. Any request or response that contains a character not permitted in
//	 a header field value MUST be treated as malformed (Section 8.1.2.6)."
//
// The MUST binds any receiver; the intermediary is the motivation, not the
// addressee. §8.1.2.6 settles the direction — "Clients MUST NOT accept a
// malformed response" — and the remedy: a stream error of type PROTOCOL_ERROR,
// so one hostile response costs one stream and the pooled connection survives.
//
// http1 (#252) and http3 (http3/validate.go) both enforce this. HTTP/2, the
// oldest of the three, never did.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// wantStreamProtocolError asserts the §8.1.2.6 remedy exactly: a STREAM error of
// type PROTOCOL_ERROR. A connection error would be over-reaction — it would let
// one malformed response kill every other stream on a pooled connection.
func wantStreamProtocolError(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: accepted, want a stream error — RFC 7540 §8.1.2.6: "+
			"\"Clients MUST NOT accept a malformed response.\"", what)
	}
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("%s: error = %v (%T), want *StreamError — §8.1.2.6 requires a "+
			"STREAM error, not a connection error: one malformed response must not "+
			"kill the other streams on a pooled connection", what, err, err)
	}
	if se.Code != frame.ErrCodeProtocolError {
		t.Errorf("%s: code = %v, want PROTOCOL_ERROR (§8.1.2.6)", what, se.Code)
	}
}

func fields(kv ...string) []hpack.HeaderField {
	out := []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, hpack.HeaderField{Name: []byte(kv[i]), Value: []byte(kv[i+1])})
	}
	return out
}

// TestConformance_RFC7540_Sec10_3_ResponseFieldValueCRLFNUL_Malformed pins the
// three bytes §10.3 names.
func TestConformance_RFC7540_Sec10_3_ResponseFieldValueCRLFNUL_Malformed(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"CR", "a\rb"},
		{"LF", "a\nb"},
		{"CRLF_injects_a_field", "a\r\nx-injected: pwned"},
		{"NUL", "a\x00b"},
		{"trailing_CR", "a\r"},
		{"leading_LF", "\na"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)
			err := deliverBlock(t, h, 1, fields("x-junk", tc.value), false)
			wantStreamProtocolError(t, err, "value "+tc.name)
		})
	}
}

// TestConformance_RFC7540_Sec10_3_ResponseFieldNameInvalid_Malformed pins
// §10.3's other half: "Requests or responses containing invalid header field
// names MUST be treated as malformed (Section 8.1.2.6)."
//
// Upper case is called out by §8.1.2.6 itself ("the inclusion of uppercase
// header field names"); the rest are non-token bytes an HPACK literal can carry
// but the HTTP/1.1 field-name grammar cannot.
func TestConformance_RFC7540_Sec10_3_ResponseFieldNameInvalid_Malformed(t *testing.T) {
	for _, tc := range []struct{ name, field string }{
		{"uppercase", "X-Junk"},
		{"space", "x junk"},
		{"colon_inside", "x:junk"},
		{"CR_in_name", "x\rjunk"},
		{"empty", ""},
		{"non_ascii", "x-\xff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)
			err := deliverBlock(t, h, 1, fields(tc.field, "v"), false)
			wantStreamProtocolError(t, err, "name "+tc.name)
		})
	}
}

// TestConformance_RFC7540_Sec8_1_2_2_ConnectionSpecificResponseField_Malformed
// pins that §8.1.2.2 binds the receive path too: "any message containing
// connection-specific header fields MUST be treated as malformed".
//
// The client already refuses to SEND these
// (client/conformance_forbidden_headers_test.go). Transfer-Encoding is the one
// that matters most: it means nothing to HTTP/2 framing, so accepting it can
// only mislead whatever reads the field set next.
//
// "te" is included at ANY value: §8.1.2.2's exception — "the TE header field,
// which MAY be present in an HTTP/2 request" — is request-only, so a response
// may not carry te at all.
func TestConformance_RFC7540_Sec8_1_2_2_ConnectionSpecificResponseField_Malformed(t *testing.T) {
	for _, tc := range []struct{ name, field, value string }{
		{"connection", "connection", "close"},
		{"transfer_encoding", "transfer-encoding", "chunked"},
		{"keep_alive", "keep-alive", "timeout=5"},
		{"upgrade", "upgrade", "websocket"},
		{"proxy_connection", "proxy-connection", "close"},
		{"te_trailers_is_request_only", "te", "trailers"},
		{"te_other", "te", "gzip"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)
			err := deliverBlock(t, h, 1, fields(tc.field, tc.value), false)
			wantStreamProtocolError(t, err, tc.name)
		})
	}
}

// TestConformance_RFC7540_Sec10_3_LegalResponseFieldsAccepted is the
// over-rejection guard, and it is the half that decides whether this validation
// is worth having: a client that rejects real responses is worse than one that
// forwards a CR.
//
// field-content admits SP and HTAB internally and obs-text (0x80-0xFF), and a
// token name admits every tchar. All of these are ordinary traffic.
func TestConformance_RFC7540_Sec10_3_LegalResponseFieldsAccepted(t *testing.T) {
	for _, tc := range []struct{ name, field, value string }{
		{"plain", "content-type", "text/plain"},
		{"value_with_SP", "content-type", "text/plain; charset=utf-8"},
		{"value_with_HTAB", "x-junk", "a\tb"},
		{"value_obs_text", "x-junk", "caf\xc3\xa9"},
		{"value_high_bit", "x-junk", "\x80\xff"},
		{"empty_value", "x-junk", ""},
		{"tchar_name", "x-a!#$%&'*+-.^_`|~9", "v"},
		{"digits_in_name", "x-123", "v"},
		{"long_value", "x-junk", string(make([]byte, 0, 8))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)
			if err := deliverBlock(t, h, 1, fields(tc.field, tc.value), false); err != nil {
				t.Errorf("%s: rejected a legal field (%q: %q): %v — over-rejection here "+
					"breaks ordinary responses, which is a worse failure than the one "+
					"this validation prevents", tc.name, tc.field, tc.value, err)
			}
		})
	}
}

// TestConformance_RFC9113_Sec8_2_1_ResponseFieldValueEdgeWhitespace_Malformed
// pins the value rule RFC 9113 §8.2.1 added over 7540: "A field value MUST NOT
// start or end with an ASCII whitespace character (ASCII SP or HTAB, 0x20 or
// 0x09)." §8.2.1 makes a violating response malformed, so §8.1.2.6's remedy — a
// stream error PROTOCOL_ERROR — applies. Same smuggling motivation as the
// embedded-delimiter rule: " x" reserialized to HTTP/1.1 shifts the field.
func TestConformance_RFC9113_Sec8_2_1_ResponseFieldValueEdgeWhitespace_Malformed(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"leading_SP", " x"},
		{"trailing_SP", "x "},
		{"leading_HTAB", "\tx"},
		{"trailing_HTAB", "x\t"},
		{"both_ends_SP", " x "},
		{"only_SP", " "},
		{"only_HTAB", "\t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)
			err := deliverBlock(t, h, 1, fields("x-junk", tc.value), false)
			wantStreamProtocolError(t, err, "edge-whitespace value "+tc.name)
		})
	}
}

// TestConformance_RFC9113_Sec8_2_1_InternalWhitespaceValueAccepted is the
// over-rejection guard for the edge rule: whitespace is legal INSIDE a value and
// bounded by visible characters, so these must still pass. Rejecting them would
// break ordinary responses (a value like "text/plain; charset=utf-8").
func TestConformance_RFC9113_Sec8_2_1_InternalWhitespaceValueAccepted(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"internal_SP", "a b"},
		{"internal_HTAB", "a\tb"},
		{"content_type", "text/plain; charset=utf-8"},
		{"visible_edges", "ab"},
		{"empty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)
			if err := deliverBlock(t, h, 1, fields("x-junk", tc.value), false); err != nil {
				t.Errorf("%s: rejected a legal value (%q): %v — internal whitespace is "+
					"legal, only leading/trailing SP/HTAB is malformed (§8.2.1)", tc.name, tc.value, err)
			}
		})
	}
}
