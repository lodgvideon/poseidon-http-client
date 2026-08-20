package http3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

func hf(name, value string) header.Field {
	return header.Field{Name: []byte(name), Value: []byte(value)}
}

func decodeAll(t *testing.T, section []byte) []header.Field {
	t.Helper()
	var dec qpack.Decoder
	var out []header.Field
	err := dec.DecodeFieldSection(section, nil, func(name, value []byte) error {
		out = append(out, header.Field{Name: append([]byte(nil), name...), Value: append([]byte(nil), value...)})
		return nil
	})
	require.NoErrorf(t, err, "decode: %v", err)
	return out
}

// encodeSection encodes a field section the way a server would, for decode tests.
func encodeSection(fields ...header.Field) []byte {
	var enc qpack.Encoder
	return enc.EncodeFieldSection(nil, fields)
}

// TestConformance_RFC9114_Sec431_RequestPseudoHeadersFirst verifies a request
// encodes its pseudo-headers before the regular headers (§4.3.1) inside a
// HEADERS frame, and round-trips through QPACK.
func TestConformance_RFC9114_Sec431_RequestPseudoHeadersFirst(t *testing.T) {
	var enc qpack.Encoder
	req := &Request{
		Method: "GET", Scheme: "https", Authority: "example.com", Path: "/index.html",
		Headers: []header.Field{hf("accept", "text/html")},
	}
	want := []struct{ name, value string }{
		{":method", "GET"}, {":scheme", "https"}, {":authority", "example.com"},
		{":path", "/index.html"}, {"accept", "text/html"},
	}

	frame, err := req.EncodeHeaders(&enc, nil, ^uint64(0))

	require.NoError(t, err, "EncodeHeaders on a well-formed request")
	typ, length, n, perr := ParseFrameHeader(frame)
	require.NoErrorf(t, perr, "frame header = (%#x,%v)", typ, perr)
	require.Equalf(t, FrameHeaders, typ, "frame type = %#x, want HEADERS (%#x)", typ, FrameHeaders)
	fields := decodeAll(t, frame[n:n+int(length)])
	require.Lenf(t, fields, len(want), "got %d fields, want %d", len(fields), len(want))
	sawRegular := false
	for i, w := range want {
		assert.Equalf(t, w.name, string(fields[i].Name),
			"field %d = %s:%s, want %s:%s", i, fields[i].Name, fields[i].Value, w.name, w.value)
		assert.Equalf(t, w.value, string(fields[i].Value),
			"field %d = %s:%s, want %s:%s", i, fields[i].Name, fields[i].Value, w.name, w.value)
		if fields[i].Name[0] == ':' {
			assert.Falsef(t, sawRegular, "pseudo-header %q after a regular header", w.name)
		} else {
			sawRegular = true
		}
	}
}

// TestRequest_OmitsEmptyAuthority verifies :authority is left out of the field
// section when empty (RFC 9114 §4.3.1) rather than sent as an empty value. A Host
// header satisfies the §4.3.1 authority requirement so the request is well-formed.
func TestRequest_OmitsEmptyAuthority(t *testing.T) {
	var enc qpack.Encoder
	req := &Request{Method: "GET", Scheme: "https", Path: "/", Headers: []header.Field{hf("host", "example.com")}}

	frame, err := req.EncodeHeaders(&enc, nil, ^uint64(0))

	require.NoError(t, err, "EncodeHeaders on a request whose authority comes from the Host header")
	_, length, n, perr := ParseFrameHeader(frame)
	require.NoErrorf(t, perr, "the encoded HEADERS frame must parse: %v", perr)
	for _, f := range decodeAll(t, frame[n:n+int(length)]) {
		assert.NotEqual(t, ":authority", string(f.Name),
			":authority must be omitted when empty rather than sent with an empty value")
	}
}

// TestConformance_RFC9114_Sec42_RequestValidation rejects requests the client
// must not generate (§4.2, §4.3.1): missing required pseudo-headers, and
// invalid or connection-specific regular headers.
func TestConformance_RFC9114_Sec42_RequestValidation(t *testing.T) {
	var enc qpack.Encoder
	cases := []struct {
		name string
		req  *Request
	}{
		// Each case must be refused by the rule it NAMES. missing_method and
		// missing_path carry an Authority for that reason: without one, an https
		// request with no Host header is already refused by the §4.3.1 authority rule,
		// so both cases stayed green with the required-pseudo-header guard deleted
		// (#809). missing_scheme needs no authority — with Scheme "" the authority
		// rule does not apply — and is the control that shows the fixture has teeth.
		{"missing_method", &Request{Scheme: "https", Authority: "h", Path: "/"}},
		{"missing_scheme", &Request{Method: "GET", Path: "/"}},
		{"missing_path", &Request{Method: "GET", Scheme: "https", Authority: "h"}},
		{"uppercase_header", &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/", Headers: []header.Field{hf("X-Foo", "1")}}},
		{"connection_specific", &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/", Headers: []header.Field{hf("connection", "keep-alive")}}},
		{"crlf_value", &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/", Headers: []header.Field{hf("x-h", "a\r\nb")}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.req.EncodeHeaders(&enc, nil, ^uint64(0))

			assert.Equalf(t, ErrH3Message, err,
				"err = %v, want ErrH3Message: the client would put a malformed request on the wire", err)
		})
	}
}

func TestConformance_RFC9114_Sec412_ResponseDecode(t *testing.T) {
	// A well-formed response: :status plus a regular header. "te" is NOT included
	// here — unlike a request, a response may not carry it at any value (§4.2), and
	// TestConformance_RFC9114_Sec4_2_ResponseTETrailers_Malformed pins that.
	section := encodeSection(hf(":status", "200"), hf("content-type", "text/plain"))
	var dec qpack.Decoder

	resp, _, err := DecodeResponseHeaders(&dec, nil, section)

	require.NoError(t, err, "DecodeResponseHeaders on a well-formed response")
	assert.Equalf(t, 200, resp.Status, "status = %d, want 200", resp.Status)
	require.Lenf(t, resp.Headers, 1, "headers = %+v, want exactly the one regular field", resp.Headers)
	assert.Equalf(t, "content-type", string(resp.Headers[0].Name), "headers = %+v", resp.Headers)
	assert.Equalf(t, "text/plain", string(resp.Headers[0].Value), "headers = %+v", resp.Headers)
}

// TestConformance_RFC9114_Sec412_MalformedResponse rejects each way a response
// message can violate the pseudo-header, field-name/value, or connection-field
// rules (§4.1.2, §4.2, §4.3.2).
func TestConformance_RFC9114_Sec412_MalformedResponse(t *testing.T) {
	cases := []struct {
		name    string
		section []byte
	}{
		{"missing_status", encodeSection(hf("content-type", "text/plain"))},
		{"pseudo_after_regular", encodeSection(hf("content-type", "x"), hf(":status", "200"))},
		{"unknown_pseudo", encodeSection(hf(":foo", "bar"))},
		{"uppercase_name", encodeSection(hf(":status", "200"), hf("Content-Type", "x"))},
		{"nondigit_status", encodeSection(hf(":status", "2xx"))},
		{"one_digit_status", encodeSection(hf(":status", "7"))},
		{"status_out_of_range", encodeSection(hf(":status", "600"))},
		{"duplicate_status", encodeSection(hf(":status", "200"), hf(":status", "204"))},
		{"crlf_in_value", encodeSection(hf(":status", "200"), hf("x-h", "a\r\nb"))},
		{"space_in_name", encodeSection(hf(":status", "200"), hf("x y", "v"))},
		{"connection_specific", encodeSection(hf(":status", "200"), hf("transfer-encoding", "chunked"))},
		{"te_not_trailers", encodeSection(hf(":status", "200"), hf("te", "gzip"))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var dec qpack.Decoder

			_, _, err := DecodeResponseHeaders(&dec, nil, c.section)

			assert.Equalf(t, ErrH3Message, err,
				"err = %v, want ErrH3Message: a malformed response must not reach the caller", err)
		})
	}
}
