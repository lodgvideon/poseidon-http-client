package http3

import (
	"testing"

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
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
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
	frame, err := req.EncodeHeaders(&enc, nil, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}

	typ, length, n, err := ParseFrameHeader(frame)
	if err != nil || typ != FrameHeaders {
		t.Fatalf("frame header = (%#x,%v)", typ, err)
	}
	fields := decodeAll(t, frame[n:n+int(length)])

	want := []struct{ name, value string }{
		{":method", "GET"}, {":scheme", "https"}, {":authority", "example.com"},
		{":path", "/index.html"}, {"accept", "text/html"},
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields, want %d", len(fields), len(want))
	}
	sawRegular := false
	for i, w := range want {
		if string(fields[i].Name) != w.name || string(fields[i].Value) != w.value {
			t.Fatalf("field %d = %s:%s, want %s:%s", i, fields[i].Name, fields[i].Value, w.name, w.value)
		}
		if fields[i].Name[0] == ':' && sawRegular {
			t.Fatalf("pseudo-header %q after a regular header", w.name)
		}
		if fields[i].Name[0] != ':' {
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
	if err != nil {
		t.Fatal(err)
	}
	_, length, n, _ := ParseFrameHeader(frame)
	for _, f := range decodeAll(t, frame[n:n+int(length)]) {
		if string(f.Name) == ":authority" {
			t.Fatal(":authority must be omitted when empty")
		}
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
		{"missing_method", &Request{Scheme: "https", Path: "/"}},
		{"missing_scheme", &Request{Method: "GET", Path: "/"}},
		{"missing_path", &Request{Method: "GET", Scheme: "https"}},
		{"uppercase_header", &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/", Headers: []header.Field{hf("X-Foo", "1")}}},
		{"connection_specific", &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/", Headers: []header.Field{hf("connection", "keep-alive")}}},
		{"crlf_value", &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/", Headers: []header.Field{hf("x-h", "a\r\nb")}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := c.req.EncodeHeaders(&enc, nil, ^uint64(0)); err != ErrH3Message {
				t.Fatalf("err = %v, want ErrH3Message", err)
			}
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
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
	if len(resp.Headers) != 1 || string(resp.Headers[0].Name) != "content-type" ||
		string(resp.Headers[0].Value) != "text/plain" {
		t.Fatalf("headers = %+v", resp.Headers)
	}
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
			if _, _, err := DecodeResponseHeaders(&dec, nil, c.section); err != ErrH3Message {
				t.Fatalf("err = %v, want ErrH3Message", err)
			}
		})
	}
}
