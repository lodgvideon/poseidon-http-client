package http1_test

// http1 validated OUTGOING request fields (#252) but handed an INCOMING response
// field to the caller verbatim. A NUL or a bare CR the server put in a value
// reached whatever copied it into an HTTP/1.1 message, a log, or a header of its
// own — RFC 9110 §5.5: "a recipient of CR, LF, or NUL within a field value MUST
// either reject the message or replace each of those characters with SP". conn
// (#263) and http3 both enforce this on their receive sides; http1 did not.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// TestConformance_RFC9110_Sec5_5_ResponseFieldValueCRNUL_Rejected pins that a
// response field carrying CR or NUL in its value is rejected and the connection
// condemned. (LF cannot appear in an http1 value — it terminates the line — but
// a bare CR and a NUL can.)
func TestConformance_RFC9110_Sec5_5_ResponseFieldValueCRNUL_Rejected(t *testing.T) {
	for _, tc := range []struct{ name, resp string }{
		{"NUL_in_value", "HTTP/1.1 200 OK\r\nX-Evil: a\x00b\r\nContent-Length: 0\r\n\r\n"},
		{"bare_CR_in_value", "HTTP/1.1 200 OK\r\nX-Evil: a\rb\r\nContent-Length: 0\r\n\r\n"},
		{"non_token_name", "HTTP/1.1 200 OK\r\nBad Name: v\r\nContent-Length: 0\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, "GET", tc.resp)

			_, _, err := ex.ReadResponse(context.Background())

			require.Error(t, err, "accepted a malformed response field; want rejection (RFC 9110 §5.5)")
			assert.ErrorIsf(t, err, http1.ErrInvalidHeaderBlock,
				"error = %v, want it to wrap ErrInvalidHeaderBlock", err)
			assert.False(t, ex.KeepAlive(), "KeepAlive() = true after a malformed response field, want false")
		})
	}
}

// TestConformance_RFC9110_Sec5_5_LegalResponseFieldsAccepted is the
// over-rejection guard: SP/HTAB inside a value, obs-text and high-bit bytes, and
// an empty value are all ordinary traffic and must still be accepted. §5.5 names
// exactly CR, LF and NUL — nothing else.
func TestConformance_RFC9110_Sec5_5_LegalResponseFieldsAccepted(t *testing.T) {
	for _, tc := range []struct{ name, resp string }{
		{"value_with_SP", "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 0\r\n\r\n"},
		{"value_with_HTAB", "HTTP/1.1 200 OK\r\nX-Junk: a\tb\r\nContent-Length: 0\r\n\r\n"},
		{"value_obs_text", "HTTP/1.1 200 OK\r\nX-Junk: caf\xc3\xa9\r\nContent-Length: 0\r\n\r\n"},
		{"value_high_bit", "HTTP/1.1 200 OK\r\nX-Junk: \x80\xff\r\nContent-Length: 0\r\n\r\n"},
		{"empty_value", "HTTP/1.1 200 OK\r\nX-Junk:\r\nContent-Length: 0\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, "GET", tc.resp)

			_, _, err := ex.ReadResponse(context.Background())

			assert.NoErrorf(t, err, "rejected a legal response: %v — §5.5 forbids only CR, LF and NUL", err)
			assert.True(t, ex.KeepAlive(), "KeepAlive() = false for a legal response, want true")
		})
	}
}
