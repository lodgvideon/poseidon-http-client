package client

import (
	"fmt"
	"io"
	"strings"
	"time"
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

	// Timeout is the per-request deadline. When > 0, the client
	// derives a sub-context from ctx with this timeout. When the
	// timeout fires, the request fails with context.DeadlineExceeded
	// and the in-flight stream is reset with RST_STREAM(CANCEL).
	// Zero uses the parent ctx's deadline (or no deadline if ctx
	// has none). Per-request timeout is independent of ctx.
	Timeout time.Duration
}

// forbiddenRequestHeader reports whether name (already lower-cased ASCII)
// is one of the connection-specific headers that RFC 7540 §8.1.2.3
// forbids endpoints from generating. The TE header is handled
// separately because it is allowed only when its value is exactly
// "trailers".
func forbiddenRequestHeader(name string) bool {
	switch name {
	case "connection", "keep-alive", "proxy-connection",
		"transfer-encoding", "upgrade":
		return true
	}
	return false
}

// isTEHeader reports whether name is "te" (RFC 7540 §8.1.2.3 allows
// TE only with the value "trailers"; any other value is forbidden).
func isTEHeader(name string) bool { return name == "te" }

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
	// RFC 8441 §4: the :protocol pseudo-header MAY appear ONLY when
	// Method is "CONNECT". Sending :protocol on any other method is
	// a protocol violation. The server-side check
	// (SETTINGS_ENABLE_CONNECT_PROTOCOL=1) is the server's
	// responsibility, not the client's.
	if r.Protocol != "" && r.Method != "CONNECT" {
		return fmt.Errorf("%w: :protocol pseudo-header requires Method=CONNECT (RFC 8441 §4), got %q",
			ErrInvalidRequest, r.Method)
	}
	// RFC 7540 §8.1.2.3: connection-specific headers MUST NOT appear
	// in HTTP/2 requests. Sending any of them is a request-smuggling
	// vector through HTTP/1.1 downgrading intermediaries.
	for i := range r.Headers {
		hf := r.Headers[i]
		if len(hf.Name) > 0 && hf.Name[0] == ':' {
			return fmt.Errorf("%w: pseudo-header %q in regular Headers slice",
				ErrInvalidRequest, hf.Name)
		}
		name := strings.ToLower(string(hf.Name))
		if forbiddenRequestHeader(name) {
			return fmt.Errorf("%w: %q header is forbidden in HTTP/2 requests (RFC 7540 §8.1.2.3)",
				ErrInvalidRequest, hf.Name)
		}
		if isTEHeader(name) && !strings.EqualFold(string(hf.Value), "trailers") {
			return fmt.Errorf("%w: TE header value %q forbidden; only %q is allowed (RFC 7540 §8.1.2.3)",
				ErrInvalidRequest, hf.Value, "trailers")
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
