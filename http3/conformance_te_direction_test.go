package http3

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// TestConformance_RFC9114_Sec4_2_ResponseTETrailers_Malformed pins that "te" is
// forbidden in an HTTP/3 RESPONSE (and trailer section) at ANY value, including
// "trailers". RFC 9114 §4.2 scopes the te exception to a request — "the TE header
// field, which MAY be present in an HTTP/3 request header; when it is, it MUST NOT
// contain any value other than "trailers"" — so a response carrying te is a
// connection-specific field and "any message containing connection-specific
// fields MUST be treated as malformed". A single shared forbiddenField let the
// request-only exemption leak onto the response/trailer receive path, silently
// accepting the malformed response — the exact direction split conn/validate.go
// already made for HTTP/2.
func TestConformance_RFC9114_Sec4_2_ResponseTETrailers_Malformed(t *testing.T) {
	t.Run("response header section", func(t *testing.T) {
		section := encodeSection(hf(":status", "200"), hf("te", "trailers"))
		var dec qpack.Decoder
		if _, _, err := DecodeResponseHeaders(&dec, nil, section); err != ErrH3Message {
			t.Fatalf("DecodeResponseHeaders(te: trailers) = %v, want ErrH3Message — te is "+
				"forbidden on a response at any value (RFC 9114 §4.2)", err)
		}
	})

	t.Run("trailer section", func(t *testing.T) {
		section := encodeSection(hf("te", "trailers"))
		var dec qpack.Decoder
		if _, _, err := DecodeTrailers(&dec, nil, section); err != ErrH3Message {
			t.Fatalf("DecodeTrailers(te: trailers) = %v, want ErrH3Message", err)
		}
	})

	t.Run("response te with other value", func(t *testing.T) {
		section := encodeSection(hf(":status", "200"), hf("te", "gzip"))
		var dec qpack.Decoder
		if _, _, err := DecodeResponseHeaders(&dec, nil, section); err != ErrH3Message {
			t.Fatalf("DecodeResponseHeaders(te: gzip) = %v, want ErrH3Message", err)
		}
	})
}

// TestConformance_RFC9114_Sec4_2_RequestTETrailers_Allowed is the over-rejection
// guard: the §4.2 exception still holds for a REQUEST, so te: trailers must remain
// a legal outgoing request field.
func TestConformance_RFC9114_Sec4_2_RequestTETrailers_Allowed(t *testing.T) {
	if forbiddenRequestField([]byte("te"), []byte("trailers")) {
		t.Fatal("te: trailers rejected on a request — §4.2 permits it on an HTTP/3 request")
	}
	if !forbiddenRequestField([]byte("te"), []byte("gzip")) {
		t.Fatal("te: gzip accepted on a request — §4.2 permits only the value \"trailers\"")
	}
	// And the connection-specific fields stay forbidden in both directions.
	if !forbiddenRequestField([]byte("connection"), []byte("keep-alive")) {
		t.Fatal("connection field accepted on a request")
	}
}
