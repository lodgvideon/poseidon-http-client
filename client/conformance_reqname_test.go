package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestConformance_RFC9110_Sec5_6_2_OutgoingHeaderName_MustBeToken pins that the
// shared request validator refuses a header (or trailer) NAME that is not a
// token. validateRequest checked field VALUES for CR/LF/NUL (round 5) but never
// the name, so the HTTP/2 send path encoded a non-token name verbatim while the
// HTTP/1.1 path (validToken) and HTTP/3 path (validFieldName) both reject it. A
// name carrying CR/LF/NUL, a space or a colon is a request-splitting vector once
// an HTTP/2->HTTP/1.1 downgrading intermediary re-serialises it (RFC 7540 §10.3);
// RFC 9110 §5.6.2 confines a field name to `token`.
func TestConformance_RFC9110_Sec5_6_2_OutgoingHeaderName_MustBeToken(t *testing.T) {
	base := func() *Request { return &Request{Method: "GET", Path: "/"} }

	reject := []struct {
		name string
		hn   string
	}{
		{"CRLF injects a header line", "X-Evil\r\nSmuggled: 1"},
		{"bare CR", "X-Evil\ry"},
		{"bare LF", "X-Evil\ny"},
		{"NUL", "X-Evil\x00y"},
		{"space", "X Evil"},
		{"colon", "X:Evil"},
		{"empty name", ""},
		{"control byte", "X\x07Evil"},
	}
	for _, tc := range reject {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			req := base()
			req.Headers = []conn.HeaderField{{Name: []byte(tc.hn), Value: []byte("v")}}

			err := validateRequest(req)

			require.ErrorIs(t, err, ErrInvalidRequest,
				"a non-token header name must never reach the encoder (RFC 9110 §5.6.2)")
		})
	}

	// A trailer name is subject to the same rule.
	t.Run("reject/trailer name CRLF", func(t *testing.T) {
		req := base()
		req.Trailers = []conn.HeaderField{{Name: []byte("x\r\ny"), Value: []byte("v")}}

		err := validateRequest(req)

		require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a non-token trailer name")
	})

	// Over-rejection guard: ordinary names — including upper case (tokens are
	// case-insensitive), digits and every legal tchar — must be accepted.
	accept := []string{"content-type", "Content-Type", "X-Custom-Header", "x-req-id-9", "a!#$%&'*+-.^_`|~b"}
	for _, hn := range accept {
		t.Run("accept/"+hn, func(t *testing.T) {
			req := base()
			req.Headers = []conn.HeaderField{{Name: []byte(hn), Value: []byte("v")}}

			err := validateRequest(req)

			require.NoErrorf(t, err, "%q is a valid token name and must be accepted", hn)
		})
	}
}
