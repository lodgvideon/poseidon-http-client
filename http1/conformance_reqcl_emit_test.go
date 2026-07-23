package http1_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// reqCL builds the pseudo-header prelude plus the given regular fields.
func reqCL(method string, extra ...hpack.HeaderField) []hpack.HeaderField {
	return append([]hpack.HeaderField{
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
				reqCL("GET", hpack.HeaderField{Name: []byte("Content-Length"), Value: []byte(cl)}), true)
			if !errors.Is(err, http1.ErrInvalidRequest) {
				t.Fatalf("WriteRequest(endStream, Content-Length %q) err = %v, want ErrInvalidRequest (RFC 9110 §8.6)", cl, err)
			}
			if wire := capture(); wire != "" {
				t.Errorf("a rejected request must put no bytes on the wire, got:\n%q", wire)
			}
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
				hpack.HeaderField{Name: []byte("content-length"), Value: []byte(vals[0])},
				hpack.HeaderField{Name: []byte("Content-Length"), Value: []byte(vals[1])},
			), true)
			if !errors.Is(err, http1.ErrInvalidRequest) {
				t.Fatalf("WriteRequest(two Content-Length) err = %v, want ErrInvalidRequest", err)
			}
			if wire := capture(); wire != "" {
				t.Errorf("a rejected request must put no bytes on the wire, got:\n%q", wire)
			}
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
			reqCL("POST", hpack.HeaderField{Name: []byte("Content-Length"), Value: []byte("3")}), false)
		if err != nil {
			t.Fatalf("WriteRequest = %v, want nil", err)
		}
		if wire := capture(); !strings.Contains(strings.ToLower(wire), "content-length: 3\r\n") {
			t.Errorf("want the single Content-Length on the wire, got:\n%q", wire)
		}
	})
	t.Run("explicit zero with no body", func(t *testing.T) {
		ex, capture := rawCapture(t)
		err := ex.WriteRequest(context.Background(),
			reqCL("POST", hpack.HeaderField{Name: []byte("Content-Length"), Value: []byte("0")}), true)
		if err != nil {
			t.Fatalf("WriteRequest(endStream, Content-Length 0) = %v, want nil", err)
		}
		wire := strings.ToLower(capture())
		if strings.Count(wire, "content-length:") != 1 {
			t.Errorf("want exactly one Content-Length line, got:\n%q", wire)
		}
	})
	t.Run("bodyless no CL", func(t *testing.T) {
		ex, _ := rawCapture(t)
		if err := ex.WriteRequest(context.Background(), reqCL("GET"), true); err != nil {
			t.Fatalf("WriteRequest = %v, want nil", err)
		}
	})
}
