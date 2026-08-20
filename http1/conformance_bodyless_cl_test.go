package http1_test

// Framing verdict for a bodyless status carrying a Content-Length — RFC 9110
// §8.6 / RFC 9112 §6.3 rule 1. Each adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9110_Sec8_6_204UnparseableContentLengthRejected pins that a
// 204 whose Content-Length does not parse is rejected before the bodyless-status
// framing check runs — which is why that check tests clValue directly without
// re-checking the parse error.
//
// This is the boundary the dead-sub-term cleanup rests on: checkBodylessStatusFraming
// keys 204 eviction on `clValue != 0`, and an unparseable value leaves clValue
// at 0. It is safe to test the value alone only because an unparseable
// Content-Length can never reach that check: with no Transfer-Encoding,
// resolveContentLength returns its error and ReadResponse aborts here first.
func TestConformance_RFC9110_Sec8_6_204UnparseableContentLengthRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
	}{
		{"non-numeric", "HTTP/1.1 204 No Content\r\nContent-Length: abc\r\n\r\n"},
		{"non-numeric with trailing octets", "HTTP/1.1 204 No Content\r\nContent-Length: abc\r\n\r\nSMUGGLED"},
		{"signed", "HTTP/1.1 204 No Content\r\nContent-Length: +5\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, "GET", tc.wire)

			_, _, err := ex.ReadResponse(context.Background())

			require.Error(t, err, "unparseable Content-Length on a 204 accepted; the value cannot be "+
				"trusted, so the response must be discarded, not framed by a 0 that only "+
				"means the parse failed")
			assert.False(t, ex.KeepAlive(),
				"KeepAlive() = true after an unparseable Content-Length; the stream "+
					"position is indeterminate and the connection must not be pooled")
		})
	}
}

// TestConformance_RFC9110_Sec8_6_204NonZeroContentLengthNotPooled is the other
// side: a 204 that declares a real body length has body-shaped octets the client
// will not read (rule 1 makes it bodyless), so the connection cannot be reused.
func TestConformance_RFC9110_Sec8_6_204NonZeroContentLengthNotPooled(t *testing.T) {
	// The declared 5 octets are never read, so they stay on the socket. Poolable
	// would mean the next request parses "HELLO" as its status line.
	ex := wireExchange(t, "GET", "HTTP/1.1 204 No Content\r\nContent-Length: 5\r\n\r\nHELLO")

	_, _, err := ex.ReadResponse(context.Background())

	require.NoErrorf(t, err, "ReadResponse = %v; a non-zero Content-Length on a 204 is a framing hazard, "+
		"not a parse error — the response head is well formed", err)
	assert.False(t, ex.KeepAlive(),
		"KeepAlive() = true for a 204 declaring a 5-octet body — those octets are "+
			"unread on the socket and would poison the next request")
}
