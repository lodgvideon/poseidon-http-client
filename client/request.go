package client

import (
	"fmt"
	"io"
	"unicode"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Request describes one HTTP/2 request. Required fields: Method, Path.
// All other fields are optional and have safe zero-value behavior.
type Request struct {
	// Pseudo-headers (RFC 7540 §8.1.2.3). Method and Path are required;
	// Scheme and Authority default from the Client when empty.
	Method    string
	Scheme    string
	Authority string
	Path      string

	// Headers carries regular request headers. The slice is read once
	// during request build; the caller retains ownership. MUST NOT
	// include any name starting with ':' (validated up front).
	Headers []hpack.HeaderField

	// Body is the request body. At most one of Body / BodyReader is
	// honored; BodyReader takes precedence when non-nil.
	Body []byte
	// BodyReader streams the request body in DATA frames when non-nil.
	BodyReader io.Reader

	// WantBody opts the response body buffer in. When false, response
	// DATA frames are consumed (so flow-control refunds run) and the
	// payload is dropped before Do returns.
	WantBody bool
	// WantTrailers opts trailer capture in. When false, response
	// trailers are ignored.
	WantTrailers bool
}

// validateRequest enforces the up-front rules documented on Request.
// Returns an error wrapping ErrInvalidRequest with a human-readable
// detail.
func validateRequest(r *Request) error {
	if r == nil {
		return fmt.Errorf("%w: nil request", ErrInvalidRequest)
	}
	if r.Method == "" || containsAnyWhitespace(r.Method) {
		return fmt.Errorf("%w: method must be a non-empty token (no whitespace)", ErrInvalidRequest)
	}
	if r.Path == "" || containsAnyWhitespace(r.Path) {
		return fmt.Errorf("%w: path must be non-empty without whitespace", ErrInvalidRequest)
	}
	for i := range r.Headers {
		if len(r.Headers[i].Name) > 0 && r.Headers[i].Name[0] == ':' {
			return fmt.Errorf("%w: pseudo-header %q in regular Headers slice",
				ErrInvalidRequest, r.Headers[i].Name)
		}
	}
	return nil
}

// containsAnyWhitespace reports whether s contains any Unicode
// whitespace character. RFC 7230 §3.2.6 defines HTTP method and
// path-target tokens as having no whitespace at all (not just edges).
func containsAnyWhitespace(s string) bool {
	for _, r := range s {
		if unicode.IsSpace(r) {
			return true
		}
	}
	return false
}
