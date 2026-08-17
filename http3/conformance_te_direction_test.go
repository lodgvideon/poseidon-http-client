package http3

import (
	"testing"

	"github.com/stretchr/testify/assert"

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

		_, _, err := DecodeResponseHeaders(&dec, nil, section)

		assert.Equalf(t, ErrH3Message, err,
			"DecodeResponseHeaders(te: trailers) = %v, want ErrH3Message — te is "+
				"forbidden on a response at any value (RFC 9114 §4.2)", err)
	})

	t.Run("trailer section", func(t *testing.T) {
		section := encodeSection(hf("te", "trailers"))
		var dec qpack.Decoder

		_, _, err := DecodeTrailers(&dec, nil, section)

		assert.Equalf(t, ErrH3Message, err,
			"DecodeTrailers(te: trailers) = %v, want ErrH3Message — the trailer "+
				"section is on the response side of the direction split too", err)
	})

	t.Run("response te with other value", func(t *testing.T) {
		section := encodeSection(hf(":status", "200"), hf("te", "gzip"))
		var dec qpack.Decoder

		_, _, err := DecodeResponseHeaders(&dec, nil, section)

		assert.Equalf(t, ErrH3Message, err,
			"DecodeResponseHeaders(te: gzip) = %v, want ErrH3Message", err)
	})
}

// TestConformance_RFC9114_Sec4_2_RequestTETrailers_Allowed is the over-rejection
// guard: the §4.2 exception still holds for a REQUEST, so te: trailers must remain
// a legal outgoing request field.
func TestConformance_RFC9114_Sec4_2_RequestTETrailers_Allowed(t *testing.T) {
	cases := []struct {
		name      string
		field     string
		value     string
		forbidden bool
	}{
		{"te trailers", "te", "trailers", false},
		{"te other value", "te", "gzip", true},
		// And the connection-specific fields stay forbidden in both directions.
		{"connection", "connection", "keep-alive", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := forbiddenRequestField([]byte(tc.field), []byte(tc.value))

			assert.Equalf(t, tc.forbidden, got,
				"forbiddenRequestField(%q, %q) = %v — §4.2 permits te on an HTTP/3 "+
					"request only with the value \"trailers\", and forbids the "+
					"connection-specific fields outright",
				tc.field, tc.value, got)
		})
	}
}
