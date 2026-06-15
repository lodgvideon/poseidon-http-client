package client

import (
	"fmt"
	"io"
	"unicode"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
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

	// Protocol is the :protocol pseudo-header for extended CONNECT
	// (RFC 8441 §4). When non-empty, Method MUST be "CONNECT" and
	// the server MUST have advertised SETTINGS_ENABLE_CONNECT_PROTOCOL=1.
	// Example: "websocket" for WebSockets over HTTP/2.
	Protocol string

	// Headers carries regular request headers. The slice is read once
	// during request build; the caller retains ownership. MUST NOT
	// include any name starting with ':' (validated up front).
	Headers []conn.HeaderField

	// Body is the request body. At most one of Body / BodyReader is
	// honored; BodyReader takes precedence when non-nil.
	Body []byte
	// BodyReader streams the request body in DATA frames when non-nil.
	BodyReader io.Reader

	// ContentLength is the body byte count. When > 0 and BodyReader is
	// non-nil, a content-length header is emitted in the HEADERS frame.
	// Zero or negative: no content-length header. For Body []byte the
	// length is derived automatically; this field is ignored in that case.
	ContentLength int64

	// WantBody opts the response body buffer in. When false, response
	// DATA frames are consumed (so flow-control refunds run) and the
	// payload is dropped before Do returns.
	WantBody bool
	// WantTrailers opts trailer capture in. When false, response
	// trailers are ignored.
	WantTrailers bool

	// Trailers are sent as a HEADERS+END_STREAM frame after the request
	// body. Ignored when TrailerFunc is non-nil and returns non-nil.
	// MUST NOT contain pseudo-headers (names starting with ':').
	Trailers []conn.HeaderField

	// TrailerFunc, when non-nil, is called after the full body is sent.
	// Its return value replaces Trailers; if it returns nil, Trailers is
	// used as fallback. Must be idempotent: it is called twice per Do
	// invocation (once to announce trailer keys in the initial HEADERS
	// frame, once to send the actual values after the body), and may be
	// called again on retry. The two calls must return the same set of
	// keys, though values may differ — the first call reads only keys
	// (for the Trailer: announcement), the second sends both keys and
	// values (e.g. checksums computed after body flush).
	// MUST NOT return pseudo-headers — validated before sending.
	TrailerFunc func() []conn.HeaderField

	// StreamBody, when true, causes Do to return after the response HEADERS
	// frame arrives. The body is available via Response.BodyReader.
	// Caller MUST call Response.BodyReader.Close() (or Response.Reset())
	// before the next Do call. WantBody is ignored when StreamBody is true.
	StreamBody bool

	// Idempotent overrides automatic idempotency classification based
	// on Method. nil → classify by Method (GET, HEAD, OPTIONS, PUT,
	// DELETE, TRACE are idempotent; POST, PATCH are not).
	Idempotent *bool

	// DisableDecompression, when true, prevents automatic gzip/deflate
	// decompression of the response body. When false (default), the
	// client sends accept-encoding: gzip and transparently decodes
	// content-encoding: gzip/deflate responses. Response.BytesReceived
	// reflects wire bytes; Response.Body contains decompressed bytes.
	DisableDecompression bool

	// Priority embeds an RFC 7540 §5.3 priority hint into the
	// HEADERS frame. When non-nil the HEADERS frame carries the
	// PRIORITY flag and a 5-byte priority payload. The server may
	// use this to weight response delivery (e.g. deliver CSS before
	// images). StreamDep=0 means root stream (no parent). Weight
	// must be 1..256 (RFC 7540 §5.3.4). Use Exclusive=true to make
	// this stream the sole dependent of its parent.
	Priority *frame.Priority
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
	for i := range r.Trailers {
		if len(r.Trailers[i].Name) > 0 && r.Trailers[i].Name[0] == ':' {
			return fmt.Errorf("%w: pseudo-header %q in Trailers slice",
				ErrInvalidRequest, r.Trailers[i].Name)
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
