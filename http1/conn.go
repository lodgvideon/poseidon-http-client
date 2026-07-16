package http1

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ErrResponseTooLarge reports that the server sent more than the client will
// buffer for one response: a single protocol line past readBufSize, a header
// or trailer block past maxHeaderListBytes, or interim responses past
// maxInterimResponses. It mirrors http3.ErrResponseTooLarge so a caller can
// classify "the peer is hostile or broken" the same way across transports —
// the pool discards a connection on any exchange error, so nothing depends on
// telling this apart, but a load generator counting failure modes wants to.
//
// The connection is always left un-poolable when this is returned: the bytes
// needed to resynchronise the stream are exactly the bytes being refused.
var ErrResponseTooLarge = errors.New("http1: response exceeds client buffering limit")

// ErrInvalidRequest reports that the caller asked this client to send a request
// that cannot be encoded on an HTTP/1.1 wire without changing its meaning: a
// header value carrying CR, LF or NUL, a header name that is not a token, or a
// method/target carrying a space or control character.
//
// It is refused rather than sanitised. Silently stripping the bytes would send
// a request the caller did not write; escaping them has no defined encoding in
// HTTP/1.1, where the framing *is* the delimiter bytes. Refusing is the only
// option that keeps "what the caller asked for" and "what goes on the wire" the
// same message.
//
// Nothing is written to the connection when this is returned: the request is
// validated in full before the first byte leaves. A half-written poisoned
// request would be exactly the split being prevented.
var ErrInvalidRequest = errors.New("http1: invalid request")

const (
	// readBufSize is the bufio.Reader buffer and therefore, by construction,
	// the hard ceiling on one CRLF-terminated protocol line: readLine uses
	// ReadSlice, which never grows the buffer and reports ErrBufferFull
	// instead of accumulating. The bound has to be structural rather than a
	// post-hoc length check — by the time a ReadString has returned a line to
	// measure, the memory it took is already committed, which is the whole
	// bug being fixed here.
	//
	// 16 KiB is what this reader already allocated, so the cap is free, and it
	// is roughly twice what mainstream servers will even emit or accept on one
	// header line (nginx large_client_header_buffers 8k; Apache
	// LimitRequestFieldSize 8190). A legitimate response line does not reach
	// it.
	readBufSize = 16 * 1024

	// maxHeaderListBytes bounds a whole header or trailer block. A per-line
	// cap cannot catch a server that sends endlessly many short, perfectly
	// well-formed header lines, so the block needs its own ceiling on the
	// accumulated total.
	//
	// It is deliberately the same 8 MiB as conn.defaultMaxHeaderListSize, the
	// HTTP/2 SETTINGS_MAX_HEADER_LIST_SIZE this client advertises, rather than
	// a second invented number: the amount of response header a caller should
	// expect this library to buffer does not depend on which protocol version
	// carried it.
	maxHeaderListBytes = 8 << 20 // 8 MiB

	// hpackFieldOverhead is the per-field accounting overhead from RFC 7541
	// §4.1, matching hpack.HeaderField.Size(). Charging it per line is what
	// makes a flood of tiny lines cost something: without it, "a: b\r\n"
	// repeated forever would be charged only its wire bytes and a server could
	// buy an unbounded field count very cheaply.
	hpackFieldOverhead = 32

	// maxInterimResponses bounds the 1xx responses drained before the final
	// one, matching http3's identical cap. This vector is a livelock rather
	// than a leak — each interim response is parsed and discarded, so memory is
	// flat — but ReadResponse otherwise never returns while a server keeps
	// sending them.
	maxInterimResponses = 100
)

// Conn is a persistent HTTP/1.1 connection. At most one Exchange at a time
// (no pipelining). The caller serializes exchanges via an external mutex or
// by using Conn only from one goroutine at a time.
type Conn struct {
	nc     net.Conn
	br     *bufio.Reader
	closed atomic.Bool
}

// NewConn wraps nc in a persistent HTTP/1.1 Conn.
// nc must already be connected (TCP + optional TLS handshake complete).
func NewConn(nc net.Conn) *Conn {
	return &Conn{
		nc: nc,
		br: bufio.NewReaderSize(nc, readBufSize),
	}
}

// IsAlive reports whether the connection is open and usable.
func (c *Conn) IsAlive() bool {
	return !c.closed.Load()
}

// Close closes the underlying network connection.
func (c *Conn) Close() error {
	c.closed.Store(true)
	return c.nc.Close()
}

// NewExchange allocates and returns a new Exchange for the next HTTP/1.1
// request/response pair. The previous exchange must be fully drained before
// calling NewExchange again.
func (c *Conn) NewExchange() *Exchange {
	return &Exchange{c: c}
}

// crlf and finalChunk are shared immutable slices for writev payloads.
var (
	crlf       = []byte("\r\n")
	finalChunk = []byte("0\r\n\r\n")
)

// Exchange is one HTTP/1.1 request/response pair.
//
// Lifecycle:
//  1. WriteRequest — send request line + headers
//  2. WriteBody (zero or more) — send request body chunks; omit if endStream=true in WriteRequest
//  3. ReadResponse — receive response status + headers
//  4. ReadBodyChunk (zero or more) — receive response body until done=true
type Exchange struct {
	c      *Conn
	method string // request method (from :method pseudo-header)

	// request side
	reqChunked bool // sending chunked request body

	// response side
	statusCode  int
	keepAlive   bool
	respChunked bool
	// respTE and respCL record mere presence of Transfer-Encoding and
	// Content-Length in the response head, independent of their values and of
	// the order they arrived in. RFC 9112 §6.3 rule 3 keys on both being
	// present, and either can be parsed first.
	respTE         bool
	respCL         bool
	contentLen     int64 // -1 = read until connection close
	bodyRead       int64
	chunkRemaining int64
	chunkFinal     bool // terminal 0-chunk received
}

// validFieldValue reports whether v is a legal HTTP field value.
//
// RFC 9110 §5.5: "Field values ... MUST NOT contain CR, LF, or NUL". Those
// three bytes and no others: this is the literal rule, not a stricter
// invention. The field-vchar grammar and obsolete line folding (RFC 9112 §5.2)
// bound what a sender should emit more tightly, but the security property here
// is exactly the §5.5 one. CR and LF are the HTTP/1.1 field delimiters, so a
// value containing them is not one field value: it is several fields and,
// given a blank line, a whole second request (RFC 9112 §11.2).
//
// NUL is in the list for the same reason one step removed. It is not a
// delimiter to this client, but it terminates a C string, so a value carrying
// it can mean one thing here and another to a proxy written in C.
func validFieldValue(v []byte) bool {
	for _, c := range v {
		if c == '\r' || c == '\n' || c == 0 {
			return false
		}
	}
	return true
}

// tchar is the RFC 9110 §5.6.2 token character set:
//
//	"!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" / "-" / "." /
//	"^" / "_" / "`" / "|" / "~" / DIGIT / ALPHA
//
// A table rather than a switch so the check costs one indexed load per byte:
// this runs per header per request on a load generator's send path.
var tchar = func() (t [256]bool) {
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

// validToken reports whether s is a non-empty RFC 9110 §5.6.2 token.
//
// Header names are tokens. (§5.1 is what makes them case-insensitive, which is
// why WriteRequest may lower-case them; §5.6.2 is what constrains the bytes.)
// The injection subset is CR, LF, NUL and ':' — a name carrying ':' invents a
// field boundary the same way a CR invents a line boundary — but checking the
// whole token rule is no more code than that denylist and is what the grammar
// actually says, so it holds against the next vector too rather than only the
// one that prompted it.
func validToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !tchar[s[i]] {
			return false
		}
	}
	return true
}

// validRequestTarget reports whether s is safe to place in a request line.
//
// RFC 9112 §3: request-line = method SP request-target SP HTTP-version. The
// line is delimited by SP and terminated by CRLF, so a target containing SP or
// a control character re-cuts that line into different fields, or ends it
// early: the same class as a header-value CRLF, one line up. Rejecting every
// byte at or below SP plus DEL covers SP, HTAB, CR, LF and NUL in one bound
// without enumerating them.
//
// Deliberately looser than "a target is a URI reference": this layer owns the
// framing, not the URI grammar, and re-litigating RFC 3986 here would reject
// targets a server would have accepted.
func validRequestTarget(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= 0x20 || s[i] == 0x7f {
			return false
		}
	}
	return true
}

// validateFields checks everything WriteRequest is about to encode, before it
// encodes any of it.
//
// Up front rather than field-by-field during the build because a partial write
// is itself the failure being prevented: bailing out halfway through the
// net.Buffers would put the good prefix of a poisoned request on the wire and
// leave the stream mid-message, which is a worse outcome than either sending it
// or refusing it.
func validateFields(method, path, authority string, fields []hpack.HeaderField) error {
	if !validToken(method) {
		return fmt.Errorf("%w: method %q is not a token (RFC 9110 §9.1)", ErrInvalidRequest, method)
	}
	if !validRequestTarget(path) {
		return fmt.Errorf("%w: target %q contains a space or control character (RFC 9112 §3)",
			ErrInvalidRequest, path)
	}
	// The authority becomes the Host field value, so it answers to §5.5 like any
	// other value. Emptiness is deliberately NOT an error: RFC 9110 §7.2 says a
	// client whose target URI has no authority MUST send Host with an empty
	// value, so "Host: \r\n" is the conformant output here, not a rejection.
	if !validFieldValue([]byte(authority)) {
		return fmt.Errorf("%w: :authority contains CR, LF or NUL (RFC 9110 §5.5)", ErrInvalidRequest)
	}
	for _, f := range fields {
		name := string(f.Name)
		if len(name) == 0 || name[0] == ':' {
			continue // pseudo-headers are consumed above, never emitted as fields
		}
		if !validToken(name) {
			return fmt.Errorf("%w: header name %q is not a token (RFC 9110 §5.6.2)",
				ErrInvalidRequest, name)
		}
		if !validFieldValue(f.Value) {
			return fmt.Errorf("%w: value of header %q contains CR, LF or NUL (RFC 9110 §5.5)",
				ErrInvalidRequest, name)
		}
	}
	return nil
}

// WriteRequest sends the HTTP/1.1 request line and headers.
// fields must contain H2-style pseudo-headers (:method, :path, :authority,
// :scheme) followed by regular headers. :scheme and :protocol are silently
// dropped. Host is derived from :authority.
//
// When endStream is true no body will follow — the request is fully sent by
// WriteRequest. When endStream is false, WriteBody must be called to send the
// body and signal completion. If no Content-Length header is present and
// endStream is false, WriteRequest adds "Transfer-Encoding: chunked" and
// WriteBody writes RFC 7230 chunk framing.
//
// Uses net.Buffers (writev) to avoid copying all header bytes into one buffer.
func (ex *Exchange) WriteRequest(ctx context.Context, fields []hpack.HeaderField, endStream bool) error {
	var method, path, authority string
	var hasContentLength bool

	for _, f := range fields {
		switch string(f.Name) {
		case ":method":
			method = string(f.Value)
		case ":path":
			path = string(f.Value)
		case ":authority":
			authority = string(f.Value)
		case "content-length":
			hasContentLength = true
		}
	}
	// Validate before building or writing anything. This layer owns the wire, so
	// it is the last place a caller-supplied CR can be stopped from becoming a
	// request boundary, and http1 is a public package: a caller who uses it
	// directly has nothing else between them and the socket.
	// RFC 9110 §5.5, RFC 9112 §11.2.
	//
	// There is no HTTP/2 equivalent of this check by construction. There the same
	// value is length-prefixed by HPACK into a frame payload, so a CR is just a
	// byte and cannot invent a frame boundary. HTTP/1.1 is the only transport
	// here whose framing is in-band, which is why this lives in this package
	// rather than somewhere shared.
	if err := validateFields(method, path, authority, fields); err != nil {
		return err
	}
	ex.method = method

	// Determine how to frame the request body.
	if !endStream && !hasContentLength {
		ex.reqChunked = true
	}

	// Build request using net.Buffers for scatter-gather write (writev on Linux).
	var bufs net.Buffers
	bufs = append(bufs,
		[]byte(method+" "+path+" HTTP/1.1\r\n"),
		[]byte("Host: "+authority+"\r\n"),
	)

	for _, f := range fields {
		name := string(f.Name)
		if len(name) == 0 || name[0] == ':' {
			continue // skip pseudo-headers
		}
		lower := strings.ToLower(name)
		switch lower {
		case "host", "connection", "transfer-encoding", "te",
			"proxy-connection", "keep-alive", "upgrade":
			// H2 forbidden / hop-by-hop headers; we manage them ourselves.
			continue
		}
		bufs = append(bufs, []byte(lower+": "+string(f.Value)+"\r\n"))
	}

	// Body framing signals.
	if endStream {
		// No body follows. Add Content-Length: 0 for methods that could carry a
		// body so strict servers don't reject the request.
		switch method {
		case "POST", "PUT", "PATCH":
			bufs = append(bufs, []byte("Content-Length: 0\r\n"))
		}
	} else if ex.reqChunked {
		bufs = append(bufs, []byte("Transfer-Encoding: chunked\r\n"))
	}
	// else: Content-Length already in user-supplied headers.

	bufs = append(bufs, crlf) // blank line ending headers

	if dl, ok := ctx.Deadline(); ok {
		_ = ex.c.nc.SetWriteDeadline(dl)
		defer func() { _ = ex.c.nc.SetWriteDeadline(time.Time{}) }()
	}
	_, err := bufs.WriteTo(ex.c.nc)
	return err
}

// WriteBody writes a body chunk to the wire.
// When fin is true this is the last chunk; WriteBody must not be called again.
// Omit WriteBody entirely when endStream was true in WriteRequest.
func (ex *Exchange) WriteBody(ctx context.Context, p []byte, fin bool) error {
	if dl, ok := ctx.Deadline(); ok {
		_ = ex.c.nc.SetWriteDeadline(dl)
		defer func() { _ = ex.c.nc.SetWriteDeadline(time.Time{}) }()
	}

	if ex.reqChunked {
		var bufs net.Buffers
		if len(p) > 0 {
			// Chunk: hex_len\r\n data \r\n
			bufs = append(bufs,
				[]byte(strconv.FormatInt(int64(len(p)), 16)+"\r\n"),
				p,
				crlf,
			)
		}
		if fin {
			bufs = append(bufs, finalChunk)
		}
		if len(bufs) == 0 {
			return nil
		}
		_, err := bufs.WriteTo(ex.c.nc)
		return err
	}

	// Non-chunked: write data directly (Content-Length governs framing).
	if len(p) == 0 {
		return nil
	}
	_, err := ex.c.nc.Write(p)
	return err
}

// readLine reads one CRLF-terminated protocol line and returns it with the
// trailing CRLF stripped. what names the line for the error message ("status
// line", "header line", ...).
//
// The read is bounded by construction. bufio.Reader.ReadSlice returns a slice
// into the fixed 16 KiB buffer and reports ErrBufferFull once that buffer is
// full without a delimiter, so a server that never sends '\n' costs a bounded
// amount of memory. This is the difference that matters: the ReadString this
// replaces would append each full buffer to a growing fragment list and keep
// reading, so the client's allocation tracked whatever the server chose to
// send. A length check after the fact could not have helped — the memory is
// spent before there is anything to check.
//
// The returned slice aliases the reader's buffer and is invalidated by the
// next read, so it is copied into a string before returning.
func (ex *Exchange) readLine(what string) (string, error) {
	line, err := ex.c.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			// Refusing the line leaves the stream mid-line and its position
			// indeterminate: resynchronising would mean reading exactly the
			// bytes being refused. The connection must not be pooled.
			ex.keepAlive = false
			return "", fmt.Errorf("http1: %s exceeds %d bytes: %w", what, readBufSize, ErrResponseTooLarge)
		}
		return "", fmt.Errorf("http1: read %s: %w", what, err)
	}
	return strings.TrimRight(string(line), "\r\n"), nil
}

// ReadResponse reads the HTTP/1.1 response status line and headers.
// It skips 1xx informational responses automatically and blocks until a
// final (≥200) status is received.
// Returns the response headers as []hpack.HeaderField. The first element is
// always the ":status" pseudo-header for compatibility with the client layer.
func (ex *Exchange) ReadResponse(ctx context.Context) (statusCode int, headers []hpack.HeaderField, err error) {
	if dl, ok := ctx.Deadline(); ok {
		_ = ex.c.nc.SetReadDeadline(dl)
	}

	var proto string
	var interim int
	for {
		// Status line: "HTTP/1.x NNN Reason\r\n"
		line, rerr := ex.readLine("status line")
		if rerr != nil {
			return 0, nil, rerr
		}

		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 || !strings.HasPrefix(parts[0], "HTTP/1") {
			return 0, nil, fmt.Errorf("http1: malformed status line: %q", line)
		}
		proto = parts[0]
		code, perr := strconv.Atoi(parts[1])
		if perr != nil {
			return 0, nil, fmt.Errorf("http1: invalid status code: %q", parts[1])
		}

		if code >= 200 {
			statusCode = code
			break
		}
		// 1xx informational: drain its headers and loop back for the real response.
		interim++
		if interim > maxInterimResponses {
			ex.keepAlive = false
			return 0, nil, fmt.Errorf("http1: more than %d interim responses: %w",
				maxInterimResponses, ErrResponseTooLarge)
		}
		if err = ex.consumeHeaders(nil, false); err != nil {
			return 0, nil, err
		}
	}

	ex.statusCode = statusCode
	// RFC 2616 §8.1: HTTP/1.1 defaults to persistent; HTTP/1.0 defaults to close.
	ex.keepAlive = proto == "HTTP/1.1"
	ex.contentLen = -1

	headers = make([]hpack.HeaderField, 0, 12)
	// Prepend :status for compatibility with the H2-style client layer.
	headers = append(headers, hpack.HeaderField{
		Name:  []byte(":status"),
		Value: []byte(strconv.Itoa(statusCode)),
	})

	// Drop the partial block on error rather than handing it back beside the
	// error. Nothing reads it (the sole caller discards the response on any
	// error), and on the too-large path it is precisely the accumulation being
	// complained about — returning it would keep alive the memory the cap
	// exists to release.
	if err = ex.consumeHeaders(&headers, true); err != nil {
		return 0, nil, err
	}

	// RFC 9112 §6.3 rule 3: a message carrying both Transfer-Encoding and
	// Content-Length "might indicate an attempt to perform request smuggling
	// (§11.2) or response splitting (§11.1) and ought to be handled as an
	// error"; the sender MUST close the connection after responding. For a
	// client, "close" means the socket must not be reused: the two headers
	// disagree about where this response ends, so whatever is on the wire next
	// cannot be trusted to be the next response. keepAlive=false is what
	// carries that — client/h1_pool.go's handleRelease evicts any conn released
	// with it rather than returning it to the idle set.
	if ex.respTE && ex.respCL {
		ex.keepAlive = false
	}
	return statusCode, headers, nil
}

// asciiLowerHeaderName lowercases a response header name over ASCII only,
// returning s unchanged when it already has no upper-case letter.
//
// strings.ToLower is the wrong primitive for peer-controlled bytes: it is
// Unicode-aware, so it re-encodes every byte that is not valid UTF-8 into the
// three-byte replacement rune. A header name is an ASCII token (RFC 7230
// §3.2.6), so that can only corrupt a name that was already invalid — and it
// inflates it 3x while doing so, which would let a server retain three bytes
// for every byte maxHeaderListBytes charges it. Lowering ASCII only keeps
// retained bytes <= bytes received, which is what makes the header cap mean
// what it says. Found by FuzzReadResponse.
func asciiLowerHeaderName(s string) string {
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s // the common case: no allocation
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

// lastTransferCoding returns the final transfer-coding token of one
// Transfer-Encoding field value, lowercased, or "" when the list is empty.
//
// RFC 9112 §7 defines the field as a comma-separated list of transfer-coding,
// each optionally carrying ";"-delimited parameters, with optional whitespace
// around the commas. Only the last element decides the framing (§6.3 rules 3
// and 4), so that is all this reports. Splitting on "," without honouring
// quoted-string parameters is safe for that question: a quoted comma can only
// appear inside a parameter of some coding, and the token that follows the
// final unquoted comma is still the final coding.
//
// Lowering is ASCII-only for the same reason asciiLowerHeaderName is (RFC 9110
// §5.1 makes the comparison case-insensitive; the token itself is ASCII per
// §5.6.2, so strings.ToLower could only re-encode bytes that were already
// invalid).
func lastTransferCoding(value string) string {
	last := ""
	for _, tok := range strings.Split(value, ",") {
		if semi := strings.IndexByte(tok, ';'); semi >= 0 {
			tok = tok[:semi]
		}
		tok = strings.Trim(tok, " \t")
		if tok != "" {
			last = tok
		}
	}
	return asciiLowerHeaderName(last)
}

// consumeHeaders reads HTTP/1.1 headers until a blank line.
// When out is non-nil, parsed headers are appended to *out.
// When parseBody is true, it also updates ex.contentLen, ex.respChunked,
// and ex.keepAlive from the header values.
//
// The block as a whole is bounded by maxHeaderListBytes. Each line is charged
// its wire length plus the RFC 7541 §4.1 per-field overhead, which is what
// stops a server that sends endlessly many short, individually legal header
// lines — the one vector here that no per-line cap can see.
func (ex *Exchange) consumeHeaders(out *[]hpack.HeaderField, parseBody bool) error {
	var listSize uint64
	for {
		line, err := ex.readLine("header line")
		if err != nil {
			return err
		}
		if line == "" {
			return nil // blank line = end of headers
		}

		// Charge the line before parsing it, so that lines skipped as
		// malformed below still count: otherwise a flood of colon-less lines
		// would spin here forever for free.
		listSize += uint64(len(line)) + hpackFieldOverhead
		if listSize > maxHeaderListBytes {
			ex.keepAlive = false
			return fmt.Errorf("http1: header list exceeds %d bytes: %w",
				maxHeaderListBytes, ErrResponseTooLarge)
		}

		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue // skip malformed header lines
		}
		name := asciiLowerHeaderName(strings.TrimSpace(line[:colon]))
		value := strings.TrimSpace(line[colon+1:])

		if parseBody {
			switch name {
			case "content-length":
				ex.respCL = true
				// RFC 9112 §6.3 rule 6: a *valid* Content-Length with no
				// Transfer-Encoding defines the body length. The !respTE guard
				// is rule 3 seen from the other header order — Content-Length
				// may legally arrive after Transfer-Encoding, and it must not
				// reinstate length framing that the Transfer-Encoding branch
				// already overrode.
				if n, perr := strconv.ParseInt(value, 10, 64); perr == nil && !ex.respTE && ex.contentLen < 0 {
					ex.contentLen = n
				}
			case "transfer-encoding":
				ex.respTE = true
				// RFC 9112 §7: the field value is an ordered, comma-separated
				// list of transfer-coding tokens. Only "chunked" as the *final*
				// coding gives chunked framing; a substring match instead reads
				// "not-chunked" and "chunked, gzip" as chunked and then desyncs
				// on the first body byte.
				//
				// Either branch overrides any Content-Length parsed so far
				// (§6.3 rule 3), which is why contentLen is assigned
				// unconditionally here: this is the only place that can undo a
				// Content-Length that arrived first.
				if lastTransferCoding(value) == "chunked" {
					ex.respChunked = true
					ex.contentLen = -2 // sentinel: chunked overrides content-length
				} else {
					// §6.3 rule 4: chunked is not the final encoding, so the
					// body length is determined by reading until the server
					// closes the connection.
					ex.respChunked = false
					ex.contentLen = -1
				}
			case "connection":
				lower := strings.ToLower(value)
				if strings.Contains(lower, "close") {
					ex.keepAlive = false
				} else if strings.Contains(lower, "keep-alive") {
					ex.keepAlive = true
				}
			}
		}

		if out != nil {
			*out = append(*out, hpack.HeaderField{
				Name:  []byte(name),
				Value: []byte(value),
			})
		}
	}
}

// ReadBodyChunk reads up to len(buf) bytes of the response body.
// Returns (n, done, err). done=true when the response body is fully received.
// ReadBodyChunk must not be called after done=true is returned.
//
// For HEAD responses ReadBodyChunk returns (0, true, nil) immediately without
// reading any bytes (the server must not send a body for HEAD per RFC 7230 §3.3).
func (ex *Exchange) ReadBodyChunk(buf []byte) (n int, done bool, err error) {
	// HEAD responses carry no body regardless of Content-Length.
	if ex.method == "HEAD" {
		return 0, true, nil
	}

	// 204 No Content and 304 Not Modified also have no body.
	if ex.statusCode == 204 || ex.statusCode == 304 {
		return 0, true, nil
	}

	if ex.respChunked {
		return ex.readChunkedChunk(buf)
	}

	// Content-Length known.
	if ex.contentLen >= 0 {
		if ex.contentLen == 0 || ex.bodyRead >= ex.contentLen {
			return 0, true, nil
		}
		remaining := ex.contentLen - ex.bodyRead
		if int64(len(buf)) > remaining {
			buf = buf[:remaining]
		}
		n, err = ex.c.br.Read(buf)
		ex.bodyRead += int64(n)
		done = ex.bodyRead >= ex.contentLen
		if err == io.EOF {
			if !done {
				// Premature EOF before Content-Length satisfied.
				return n, true, fmt.Errorf("http1: premature EOF: got %d of %d bytes", ex.bodyRead, ex.contentLen)
			}
			// Final body bytes arrived coalesced with io.EOF in a single
			// Read (bufio passes through the underlying (n, io.EOF) when
			// the caller buffer is >= bufio's buffer). The body is now
			// complete, so surface the bytes with a nil error instead of
			// discarding n. The EOF means the peer closed the socket, so the
			// connection is no longer reusable — do not let it be pooled.
			ex.keepAlive = false
			err = nil
		}
		return n, done, err
	}

	// contentLen == -1: read until connection close.
	n, err = ex.c.br.Read(buf)
	if err == io.EOF {
		ex.keepAlive = false
		return n, true, nil
	}
	return n, false, err
}

// readChunkedChunk reads the next chunk worth of data from a chunked body.
func (ex *Exchange) readChunkedChunk(buf []byte) (n int, done bool, err error) {
	if ex.chunkFinal {
		return 0, true, nil
	}

	// Need to start a new chunk?
	for ex.chunkRemaining == 0 {
		// Read chunk-size line: "hex[;extension]\r\n"
		line, lerr := ex.readLine("chunk size")
		if lerr != nil {
			return 0, false, lerr
		}
		if semi := strings.IndexByte(line, ';'); semi >= 0 {
			line = line[:semi] // strip chunk extensions
		}
		line = strings.TrimSpace(line)
		size, perr := strconv.ParseInt(line, 16, 64)
		if perr != nil {
			return 0, false, fmt.Errorf("http1: invalid chunk size %q: %w", line, perr)
		}
		if size < 0 {
			// chunk-size is 1*HEXDIG (unsigned) per RFC 7230 §4.1;
			// ParseInt accepts a leading '-', so reject it explicitly
			// before it becomes a negative slice bound below. The chunked
			// framing is now corrupt and the stream position indeterminate,
			// so the connection must not be pooled.
			ex.keepAlive = false
			return 0, false, fmt.Errorf("http1: invalid chunk size %q: negative", line)
		}
		if size == 0 {
			// Terminal chunk. Consume optional trailers, bounded exactly as the
			// header block is — a trailer section is a header block, and a
			// server can stream one forever just as easily.
			//
			// Any other error (typically EOF from a server that closes straight
			// after the terminal chunk) stays tolerated as it was before: the
			// body is already complete, so the response is good even when the
			// trailers are not.
			if terr := ex.consumeHeaders(nil, false); terr != nil && errors.Is(terr, ErrResponseTooLarge) {
				return 0, false, terr
			}
			ex.chunkFinal = true
			return 0, true, nil
		}
		ex.chunkRemaining = size
	}

	// Read up to min(len(buf), chunkRemaining) bytes from this chunk.
	toRead := ex.chunkRemaining
	if int64(len(buf)) < toRead {
		toRead = int64(len(buf))
	}
	n, err = ex.c.br.Read(buf[:toRead])
	ex.chunkRemaining -= int64(n)
	if err != nil {
		return n, false, err
	}

	// After exhausting a chunk, consume its trailing CRLF.
	if ex.chunkRemaining == 0 {
		if _, lerr := ex.readLine("chunk CRLF"); lerr != nil {
			return n, false, lerr
		}
	}

	return n, false, nil
}

// KeepAlive reports whether the underlying connection should be returned to
// a pool after this exchange completes. Returns false when the server sent
// "Connection: close" or used HTTP/1.0 without "Connection: keep-alive".
func (ex *Exchange) KeepAlive() bool {
	return ex.keepAlive
}
