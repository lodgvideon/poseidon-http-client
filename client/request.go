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
	//
	// Ignored entirely when CompressBody is set: it describes the uncompressed
	// body, so it cannot describe what goes on the wire. See CompressBody for
	// what is sent instead.
	ContentLength int64

	// BodyMode controls how the response body is handled. The zero value,
	// BodyDiscard, drops the body (DATA frames are still consumed so
	// flow-control refunds run); BodyBuffer accumulates it into
	// Response.Body; BodyStream returns it via Response.BodyReader for
	// incremental reads (Do returns after the HEADERS frame).
	BodyMode BodyMode
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

	// Idempotency overrides automatic idempotency classification (which
	// governs transport-level retry eligibility). The zero value,
	// IdempotencyAuto, classifies by Method; ForceIdempotent / ForceNotIdempotent
	// override it. Replaces the former *bool, dropping the addr-of-a-local dance.
	Idempotency IdempotencyMode

	// CompressBody compresses the request body with the given coding before
	// sending, and sets the content-encoding header itself. The zero value,
	// EncodingIdentity, sends the body unchanged — the existing behaviour.
	//
	// This is deliberately a separate knob from the content-encoding header.
	// That header is a representation header (RFC 9110 §8.4): it describes what
	// the body already *is*. Callers who compress their own bodies set it and
	// expect those exact bytes on the wire, so it cannot double as a request to
	// compress without silently double-encoding them. Setting both this field
	// and a content-encoding header is refused with
	// [ErrConflictingContentEncoding]; an unrecognised coding is refused with
	// [ErrUnsupportedContentEncoding].
	//
	// content-length is managed for you and any caller-supplied one is replaced:
	// for a Body the compressed length is sent, and for a BodyReader — whose
	// compressed length cannot be known before it is read — no content-length is
	// sent at all (HTTP/1.1 then uses chunked transfer-coding). A request with no
	// body is left alone: there is no representation to encode.
	CompressBody ContentEncoding

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

// BodyMode controls how Do handles the response body.
type BodyMode uint8

const (
	// BodyDiscard (the zero value) drops the response body — DATA frames are
	// still consumed so flow-control refunds run, but the payload is not
	// retained. The default; fits a load generator that does not need bodies.
	BodyDiscard BodyMode = iota
	// BodyBuffer accumulates the full response body into Response.Body.
	BodyBuffer
	// BodyStream makes Do return after the response HEADERS frame, with the
	// body available via Response.BodyReader (an io.ReadCloser) for incremental
	// reads. The caller MUST Close it (or call Response.Reset) before the next
	// Do. Not supported on HTTP/1.1 connections.
	BodyStream
)

// IdempotencyMode overrides how a request is classified for transport-level
// retry. The zero value classifies by HTTP method.
type IdempotencyMode uint8

const (
	// IdempotencyAuto (zero value) classifies by Method: GET, HEAD, OPTIONS,
	// PUT, DELETE, TRACE are idempotent (retriable); POST, PATCH and others are
	// not.
	IdempotencyAuto IdempotencyMode = iota
	// ForceIdempotent marks the request retriable regardless of Method — e.g. a
	// POST guarded by an idempotency key.
	ForceIdempotent
	// ForceNotIdempotent marks the request non-retriable regardless of Method.
	ForceNotIdempotent
)

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
	// A synthesized pseudo-header value carrying CR, LF or NUL is a request-
	// splitting vector once an HTTP/2->HTTP/1.1 downgrading intermediary
	// translates it verbatim (RFC 7540 §10.3). http1 refuses these on its own
	// wire; the HTTP/2 and HTTP/3 send paths, which share this validator, had no
	// equivalent, so an Authority or Scheme like "a\r\nX-Injected: 1" reached the
	// encoder untouched.
	if bad := firstInjectionPseudoField(r); bad != "" {
		return fmt.Errorf("%w: %s value carries CR, LF or NUL (request-splitting vector, RFC 7540 §10.3)",
			ErrInvalidRequest, bad)
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
		if !isTokenName(hf.Name) {
			return fmt.Errorf("%w: header name %q is not a token (RFC 9110 §5.6.2); a name "+
				"carrying CR, LF, NUL, a space or a colon is a request-splitting vector",
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
		// The value the same §10.3 vector rides on. HPACK cannot split the H2
		// wire with these bytes, but a downgrading intermediary can.
		if hasFieldInjectionByte(hf.Value) {
			return fmt.Errorf("%w: header %q value carries CR, LF or NUL (request-splitting vector, RFC 7540 §10.3)",
				ErrInvalidRequest, hf.Name)
		}
	}
	for i := range r.Trailers {
		if len(r.Trailers[i].Name) > 0 && r.Trailers[i].Name[0] == ':' {
			return fmt.Errorf("%w: pseudo-header %q in Trailers slice",
				ErrInvalidRequest, r.Trailers[i].Name)
		}
		if !isTokenName(r.Trailers[i].Name) {
			return fmt.Errorf("%w: trailer name %q is not a token (RFC 9110 §5.6.2)",
				ErrInvalidRequest, r.Trailers[i].Name)
		}
		if hasFieldInjectionByte(r.Trailers[i].Value) {
			return fmt.Errorf("%w: trailer %q value carries CR, LF or NUL (request-splitting vector, RFC 7540 §10.3)",
				ErrInvalidRequest, r.Trailers[i].Name)
		}
	}
	// Checked here as well as at the point of use so Do rejects a
	// self-contradictory request before opening a connection.
	return validateCompressBody(r)
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

// hasFieldInjectionByte reports whether v carries CR, LF or NUL — the three
// bytes RFC 7540 §10.3 names as the ones that "might be exploited by an attacker
// if they are translated verbatim". HPACK length-prefixes a field value, so these
// cannot split the HTTP/2 wire itself; the danger is an HTTP/2->HTTP/1.1
// downgrading intermediary, where CR and LF ARE the field and line delimiters, so
// a value carrying them becomes several fields and, past a blank line, an injected
// request (§8.1.2.6 makes such a message malformed). NUL is in the set for the
// same reason one step removed: it terminates a C string, so a value carrying it
// can mean one thing here and another to a proxy written in C.
func hasFieldInjectionByte(v []byte) bool {
	for _, c := range v {
		if c == '\r' || c == '\n' || c == 0 {
			return true
		}
	}
	return false
}

// tokenChar is the RFC 9110 §5.6.2 token character set: DIGIT / ALPHA / one of
// "!#$%&'*+-.^_`|~". Upper case is included — a header field name is a token and
// tokens are case-insensitive, so refusing upper case here would reject a
// perfectly legal caller name.
var tokenChar = func() (t [256]bool) {
	for _, c := range []byte("!#$%&'*+-.^_`|~") {
		t[c] = true
	}
	for c := byte('0'); c <= '9'; c++ {
		t[c] = true
	}
	for c := byte('a'); c <= 'z'; c++ {
		t[c] = true
	}
	for c := byte('A'); c <= 'Z'; c++ {
		t[c] = true
	}
	return t
}()

// isTokenName reports whether name is a non-empty RFC 9110 §5.6.2 token — the
// grammar every HTTP header field name obeys. A name outside it (a space, a
// colon, CR/LF/NUL, any control byte) cannot be put on the wire without changing
// the message: on HTTP/1.1 those bytes ARE the delimiters, and an HTTP/2->HTTP/1.1
// downgrading intermediary re-serialising "X\r\nInjected: 1" produces a second
// header line. http1.WriteRequest (validToken) and http3 (validFieldName) both
// refuse such a name; validateRequest checked field VALUES for the same bytes but
// never the NAME, so the HTTP/2 send path emitted it verbatim.
func isTokenName(name []byte) bool {
	if len(name) == 0 {
		return false
	}
	for _, c := range name {
		if !tokenChar[c] {
			return false
		}
	}
	return true
}

// firstInjectionPseudoField returns the name of the first synthesized
// pseudo-header value that carries an injection byte, or "" if none does. Method
// and Path are already refused for CR and LF by the whitespace checks in
// validateRequest, but not for NUL; Scheme, Authority and Protocol are checked
// nowhere else. http1.WriteRequest refuses these bytes on its own wire — this is
// the guard for the HTTP/2 and HTTP/3 send paths, which share validateRequest and
// had no equivalent.
func firstInjectionPseudoField(r *Request) string {
	switch {
	case hasFieldInjectionByte(unsafeStringToBytes(r.Method)):
		return "method"
	case hasFieldInjectionByte(unsafeStringToBytes(r.Path)):
		return "path"
	case hasFieldInjectionByte(unsafeStringToBytes(r.Scheme)):
		return "scheme"
	case hasFieldInjectionByte(unsafeStringToBytes(r.Authority)):
		return "authority"
	case hasFieldInjectionByte(unsafeStringToBytes(r.Protocol)):
		return ":protocol"
	}
	return ""
}
