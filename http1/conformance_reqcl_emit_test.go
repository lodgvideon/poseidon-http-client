package http1_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// reqCL builds the pseudo-header prelude plus the given regular fields.
func reqCL(method string, extra ...header.Field) []header.Field {
	return append([]header.Field{
		{Name: []byte(":method"), Value: []byte(method)},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, extra...)
}

// TestConformance_RFC9110_Sec8_6_ContentLengthWithoutBodyRejected pins that a
// non-zero caller Content-Length on a request that sends no body (endStream) is
// refused, never emitted. RFC 9110 §8.6: "a sender MUST NOT forward a message
// with a Content-Length header field value that is known to be incorrect." The
// wire would declare N octets that are never written — a CL.0 desync (RFC 9112
// §11.2) on a reused connection.
func TestConformance_RFC9110_Sec8_6_ContentLengthWithoutBodyRejected(t *testing.T) {
	for _, cl := range []string{"5", "1", "999999", " 7 "} {
		t.Run(cl, func(t *testing.T) {
			ex, capture := rawCapture(t)

			err := ex.WriteRequest(context.Background(),
				reqCL("GET", header.Field{Name: []byte("Content-Length"), Value: []byte(cl)}), true)

			require.ErrorIsf(t, err, http1.ErrInvalidRequest,
				"WriteRequest(endStream, Content-Length %q) err = %v, want ErrInvalidRequest (RFC 9110 §8.6)", cl, err)
			wire := capture()
			assert.Emptyf(t, wire, "a rejected request must put no bytes on the wire, got:\n%q", wire)
		})
	}
}

// TestConformance_RFC9112_Sec6_1_DuplicateContentLengthRejected pins that two
// Content-Length field lines are refused on emit. RFC 9110 §5.3 makes
// Content-Length a singleton; two lines are the CL.CL smuggling primitive (RFC
// 9112 §11.2) when they disagree and are never legitimate to send. The parser
// already refuses this shape on receive; the sender must not generate it.
func TestConformance_RFC9112_Sec6_1_DuplicateContentLengthRejected(t *testing.T) {
	cases := map[string][2]string{
		"differing": {"5", "6"},
		"identical": {"5", "5"},
	}
	for name, vals := range cases {
		t.Run(name, func(t *testing.T) {
			ex, capture := rawCapture(t)

			err := ex.WriteRequest(context.Background(), reqCL("GET",
				header.Field{Name: []byte("content-length"), Value: []byte(vals[0])},
				header.Field{Name: []byte("Content-Length"), Value: []byte(vals[1])},
			), true)

			require.ErrorIsf(t, err, http1.ErrInvalidRequest,
				"WriteRequest(two Content-Length) err = %v, want ErrInvalidRequest", err)
			wire := capture()
			assert.Emptyf(t, wire, "a rejected request must put no bytes on the wire, got:\n%q", wire)
		})
	}
}

// TestConformance_RFC9110_Sec8_6_ContentLengthEmitGuardsAccept is the
// over-rejection guard: the guards must not touch a legitimately framed request.
// A single Content-Length that matches a body (endStream=false), an explicit
// zero-length declaration with no body, and a bodyless request with no
// Content-Length must all pass.
func TestConformance_RFC9110_Sec8_6_ContentLengthEmitGuardsAccept(t *testing.T) {
	t.Run("single CL with body follows", func(t *testing.T) {
		ex, capture := rawCapture(t)

		err := ex.WriteRequest(context.Background(),
			reqCL("POST", header.Field{Name: []byte("Content-Length"), Value: []byte("3")}), false)

		require.NoErrorf(t, err, "WriteRequest = %v, want nil", err)
		wire := capture()
		assert.Containsf(t, strings.ToLower(wire), "content-length: 3\r\n",
			"want the single Content-Length on the wire, got:\n%q", wire)
	})

	t.Run("explicit zero with no body", func(t *testing.T) {
		ex, capture := rawCapture(t)

		err := ex.WriteRequest(context.Background(),
			reqCL("POST", header.Field{Name: []byte("Content-Length"), Value: []byte("0")}), true)

		require.NoErrorf(t, err, "WriteRequest(endStream, Content-Length 0) = %v, want nil", err)
		wire := strings.ToLower(capture())
		assert.Equalf(t, 1, strings.Count(wire, "content-length:"),
			"want exactly one Content-Length line, got:\n%q", wire)
	})

	t.Run("bodyless no CL", func(t *testing.T) {
		ex, _ := rawCapture(t)

		err := ex.WriteRequest(context.Background(), reqCL("GET"), true)

		require.NoErrorf(t, err, "WriteRequest = %v, want nil", err)
	})
}
