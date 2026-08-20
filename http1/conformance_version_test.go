package http1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9110_Sec6_HigherMinorVersionStaysPersistent pins that a
// response with a higher minor version of major 1 (e.g. HTTP/1.2) is processed
// as HTTP/1.1 and therefore defaults to a persistent connection, not closed.
//
// RFC 9110 §6: a recipient that receives a message with a higher minor version
// it does not fully implement "SHOULD process the message as if it were in the
// highest minor version within that major version to which the recipient is
// conformant". For this client that is HTTP/1.1, whose default is persistent —
// so a bare HTTP/1.2 response (no Connection header) must stay poolable. Only
// HTTP/1.0 keeps the close-by-default behaviour.
func TestConformance_RFC9110_Sec6_HigherMinorVersionStaysPersistent(t *testing.T) {
	cases := []struct {
		proto         string
		wantKeepAlive bool
	}{
		{"HTTP/1.1", true},
		{"HTTP/1.2", true}, // higher minor of major 1 → processed as 1.1 → persistent
		{"HTTP/1.9", true},
		{"HTTP/1.0", false}, // 1.0 keeps close-by-default
	}
	for _, tc := range cases {
		t.Run(tc.proto, func(t *testing.T) {
			ex := wireExchange(t, "GET", tc.proto+" 200 OK\r\nContent-Length: 0\r\n\r\n")
			_, _, err := ex.ReadResponse(context.Background())
			require.NoErrorf(t, err, "ReadResponse(%s): %v", tc.proto, err)

			_, done, err := ex.ReadBodyChunk(make([]byte, 8))

			require.Truef(t, done && err == nil,
				"body should end immediately (Content-Length 0): done=%v err=%v", done, err)
			got := ex.KeepAlive()
			assert.Equalf(t, tc.wantKeepAlive, got,
				"proto %s: KeepAlive() = %v, want %v — a higher minor of major 1 is "+
					"processed as HTTP/1.1 (persistent); only HTTP/1.0 closes by default (RFC 9110 §6)",
				tc.proto, got, tc.wantKeepAlive)
		})
	}
}
