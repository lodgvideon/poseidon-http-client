package http1_test

// A "close" connection option is sticky across repeated Connection field lines.
//
// RFC 9110 §5.3 combines repeated field lines into one value, so "close" on one
// Connection line and "keep-alive" on the next is the single value
// "close, keep-alive" — the "close" option means the server will close the
// socket after the response, so the client MUST NOT reuse the connection. The
// parser processed each Connection line independently, so a later "keep-alive"
// line re-enabled reuse and the pool handed a closing socket to the next
// request. This adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// keepAliveAfter reads a full response (head + body) from a raw server reply
// and reports the pooling verdict KeepAlive() returns.
func keepAliveAfter(t *testing.T, rawResponse string) bool {
	t.Helper()
	ex := wireExchange(t, "GET", rawResponse)
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")
	// Drain the body so the verdict reflects a fully consumed exchange (an
	// unconsumed body condemns the connection on its own).
	buf := make([]byte, 256)
	for {
		_, done, rerr := ex.ReadBodyChunk(buf)
		if rerr != nil || done {
			break
		}
	}
	return ex.KeepAlive()
}

// TestConformance_RFC9110_Sec5_3_ConnectionCloseIsSticky pins that a "close"
// option wins over a "keep-alive" regardless of which Connection field line or
// list position it arrives in, and that the client does not over-reject a plain
// keep-alive.
func TestConformance_RFC9110_Sec5_3_ConnectionCloseIsSticky(t *testing.T) {
	const body = "Content-Length: 5\r\n\r\nhello"
	for _, tc := range []struct {
		name   string
		conn   string
		wantKA bool
	}{
		{"close_then_keepalive_two_lines", "Connection: close\r\nConnection: keep-alive\r\n", false},
		{"keepalive_then_close_two_lines", "Connection: keep-alive\r\nConnection: close\r\n", false},
		{"close_and_keepalive_one_line", "Connection: close, keep-alive\r\n", false},
		{"keepalive_alone_stays_reusable", "Connection: keep-alive\r\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := keepAliveAfter(t, "HTTP/1.1 200 OK\r\n"+tc.conn+body)

			require.Equalf(t, tc.wantKA, got,
				"KeepAlive() = %v, want %v — RFC 9110 §5.3 makes these Connection "+
					"lines one value; a close option must win:\n%s", got, tc.wantKA, tc.conn)
		})
	}
}
