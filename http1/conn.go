package http1

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
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

// ErrInvalidContentLength reports that the response carried a Content-Length
// this client will not frame a body with: a value that is not RFC 9110 §8.6's
// ABNF `Content-Length = 1*DIGIT`, a value that overflows int64, or several
// Content-Length values that disagree.
//
// RFC 9112 §6.3 rule 5 makes this unrecoverable rather than ignorable — "If a
// message is received without Transfer-Encoding and with an invalid
// Content-Length header field, then the message framing is invalid and the
// recipient MUST treat it as an unrecoverable error ... If it is in a response
// message received by a user agent, the user agent MUST close the connection to
// the server and discard the received response." Disagreeing values are the
// same case and not a separate one: RFC 9110 §5.3 lets a recipient combine
// repeated field lines into one comma-separated value, so "Content-Length: 5"
// followed by "Content-Length: 10" is "Content-Length: 5, 10", which is not
// 1*DIGIT.
//
// Picking a winner instead of rejecting is the CL.CL request-smuggling desync
// (RFC 9112 §11.2): the bytes the losing value would have covered stay unread
// on the socket, and a pooled connection then begins its next response
// mid-stream — parsing peer-chosen body bytes as a status line. The connection
// is therefore always left un-poolable when this is returned, matching the other
// paths here whose stream position is indeterminate.
var ErrInvalidContentLength = errors.New("http1: invalid Content-Length")

// ErrInvalidHeaderBlock reports that a response header or trailer block could
// not be interpreted as a sequence of field lines — currently, an obs-fold
// continuation (RFC 9112 §5.2) arriving with no field line to continue.
//
// The connection is always left un-poolable when this is returned. A block whose
// structure is not understood cannot be trusted to have ended where this client
// thinks it did, so the stream position is indeterminate.
var ErrInvalidHeaderBlock = errors.New("http1: invalid header block")

// ErrInvalidChunkSize reports that a chunk-size line was not RFC 9112 §7.1's
// ABNF `chunk-size = 1*HEXDIG`: empty, non-hex, signed, or past int64.
//
// The connection is always left un-poolable when this is returned. A chunk-size
// this client cannot parse is a chunk boundary it cannot find, so it no longer
// knows where this response ends — and a pooled connection would begin its next
// response somewhere inside the previous one's body, parsing peer-chosen bytes
// as a status line.
var ErrInvalidChunkSize = errors.New("http1: invalid chunk size")

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
	// clSeen, clValue and clErr accumulate the Content-Length decision across
	// the header block instead of committing to it line by line. RFC 9112 §6.3
	// rule 5 only makes an invalid Content-Length fatal "without
	// Transfer-Encoding", and rule 3 lets Transfer-Encoding override it — but
	// either header may arrive first, so neither question can be answered until
	// the blank line. Resolving early is what would make the verdict depend on
	// the order the server chose to send its headers in.
	clSeen         bool  // a Content-Length field was present
	clValue        int64 // its value, when clSeen && clErr == nil
	clErr          error // first Content-Length defect seen, if any
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
// RFC 9110 §5.5 names exactly these three bytes: "Field values containing CR,
// LF, or NUL characters are invalid and dangerous, due to the varying ways that
// implementations might parse and interpret those characters; a recipient of CR,
// LF, or NUL within a field value MUST either reject the message or replace each
// of those characters with SP before further processing or forwarding of that
// message."
//
// Note what that MUST binds: a RECIPIENT, and it offers a choice — reject, or
// replace with SP. §5.5 states no sender-side prohibition at all, so refusing to
// emit these bytes is this client's policy, not a quotation of one. The policy
// is the right one anyway, and for a reason §5.5 only gestures at: CR and LF are
// the HTTP/1.1 field delimiters, so a value containing them is not one field
// value — it is several fields and, given a blank line, a whole second request
// (RFC 9112 §11.2). We would be the implementation doing the "varying" parsing.
//
// Three bytes and no others: the tighter bound is the field-vchar grammar in the
// same section, but it is the delimiter property that carries the security
// weight, and that property belongs to exactly CR, LF and NUL.
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
	// other value. Emptiness is deliberately NOT an error: RFC 9112 §3.2 says a
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
		default:
			// Content-Length must be recognised case-insensitively (RFC 9110
			// §5.1: "Field names are case-insensitive"). An exact match here
			// against the lower-case spelling missed the canonical
			// "Content-Length" while the emit loop below lower-cased and wrote
			// it anyway, so hasContentLength stayed false, reqChunked was set,
			// and the request went out carrying BOTH a content-length field and
			// Transfer-Encoding: chunked. RFC 9112 §6.1: "A sender MUST NOT send
			// a Content-Length header field in any message that contains a
			// Transfer-Encoding header field." That pair is the request-smuggling
			// primitive (§11.2) — a front end honouring one and a back end the
			// other disagree about where the request ends — and this client was
			// generating it, unprompted, for every caller that spelled the header
			// the way the RFC does.
			//
			// The fold is byte-wise rather than strings.ToLower so that probing a
			// name costs no allocation on the request hot path.
			if asciiEqualFold(f.Name, "content-length") {
				hasContentLength = true
			}
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
		// body so strict servers don't reject the request — but only when the
		// caller supplied none. The field loop above has already written any
		// caller-supplied Content-Length to the wire, so appending
		// unconditionally emitted two disagreeing Content-Length field lines,
		// which is the CL.CL desync (RFC 9112 §11.2) in the request direction:
		// the same construct this client rejects when a server sends it.
		if !hasContentLength {
			switch method {
			case "POST", "PUT", "PATCH":
				bufs = append(bufs, []byte("Content-Length: 0\r\n"))
			}
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
	// Install this exchange's read deadline unconditionally — a deadline when
	// ctx has one, the zero value when it does not.
	//
	// The write path installs its deadline and clears it with a defer, because
	// writing is over when WriteRequest returns. Reading is not: ReadBodyChunk
	// runs after this function returns and must stay under the same deadline, so
	// there is nothing here to defer to. That left the deadline installed on the
	// socket after the exchange, and the connection is pooled — so the NEXT
	// request inherited the PREVIOUS one's deadline and failed with i/o timeout
	// at an instant that had nothing to do with it, even with no deadline of its
	// own.
	//
	// Setting it on every entry rather than clearing it on every exit is what
	// makes that unrepresentable: an exchange's read deadline is exactly its own
	// ctx's, and no exit path has to remember anything.
	if dl, ok := ctx.Deadline(); ok {
		_ = ex.c.nc.SetReadDeadline(dl)
	} else {
		_ = ex.c.nc.SetReadDeadline(time.Time{})
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
	// (Section 11.2) or response splitting (Section 11.1) and ought to be
	// handled as an error".
	//
	// "Ought to be handled as an error" is all rule 3 says; refusing to reuse
	// the socket is this client's reading of it, not a quotation. RFC 9112 does
	// state a hard MUST-close for this shape, but §6.1 scopes it to the other
	// side of the exchange — "the server MUST close the connection after
	// responding to such a request" — a server's duty on an inbound request, not
	// a client's on an inbound response. (The client-side MUST-close in this spec
	// belongs to §6.3 rule 5, the invalid-Content-Length case, which
	// resolveContentLength handles.)
	//
	// The reading is still forced by the wire: the two headers disagree about
	// where this response ends, so whatever follows cannot be trusted to be the
	// next response. keepAlive=false carries that — client/h1_pool.go's
	// handleRelease evicts any conn released with it rather than returning it to
	// the idle set.
	if ex.respTE && ex.respCL {
		ex.keepAlive = false
	}

	// A status that RFC 9112 §6.3 rule 1 makes bodyless, whose head nevertheless
	// declared a body.
	//
	// Rule 1 is honoured correctly by ReadBodyChunk — a 204/304 ends at the blank
	// line whatever the fields say — and that is exactly what makes this
	// dangerous. The declared bytes are never read, so they stay on the socket,
	// and the connection is pooled. The next request then parses attacker-chosen
	// bytes as its status line, and because the attacker chose them they can be a
	// complete well-formed response: request N+1 gets a response the server never
	// sent for it, with err=nil.
	//
	// §6.3 forbids exactly that outcome: "A client MUST NOT process, cache, or
	// forward such extra data as a separate response, since such behavior would
	// be vulnerable to cache poisoning."
	//
	// This is not #251's CL.CL desync — there is one Content-Length and it is
	// valid, so resolveContentLength never objects — and not the TE+CL shape
	// above. It is the head declaring a body the status forbids.
	switch ex.statusCode {
	case 204:
		// RFC 9110 §8.6: "A server MUST NOT send a Content-Length header field in
		// any response with a status code of 1xx (Informational) or 204 (No
		// Content)". RFC 9112 §6.1 says the same of Transfer-Encoding. Either
		// field on a 204 means the peer is broken or hostile, and body-shaped
		// bytes may be sitting on the socket.
		if ex.respTE || ex.respCL {
			ex.keepAlive = false
		}
	case 304:
		// Content-Length on a 304 is explicitly permitted and must NOT cost the
		// connection — §8.6: "A server MAY send a Content-Length header field in
		// a 304 (Not Modified) response to a conditional GET request". It
		// describes the selected representation, not a message body, so no bytes
		// follow it. Origin servers send it routinely; evicting on it would cost
		// a connection per conditional GET.
		//
		// Transfer-Encoding gets no such licence. Honesty about the grounds: no
		// RFC forbids it on a 304 either — §6.1's MUST NOT lists 1xx, 204 and a
		// 2xx to CONNECT, and 304 is not among them. This is therefore this
		// client's reading, not a quoted rule: unlike Content-Length,
		// Transfer-Encoding has no representation-describing meaning to fall back
		// on. It is a message-framing field, and rule 1 says this message has no
		// body to frame — so its presence says the server intends bytes we are
		// required not to read.
		if ex.respTE {
			ex.keepAlive = false
		}
	}
	// HEAD is deliberately absent. Rule 1 makes a HEAD response bodyless too, but
	// RFC 9110 §9.3.2 — "The server SHOULD send the same header fields in response
	// to a HEAD request as it would have sent if the request method had been GET"
	// — makes Content-Length normal there, describing the body a GET would have
	// returned. Evicting on it would discard a pooled connection after every HEAD.
	return statusCode, headers, nil
}

// asciiEqualFold reports whether name matches lower case-insensitively, where
// lower is an all-lower-case ASCII literal. It allocates nothing, so a caller's
// header names can be probed on the request hot path.
//
// ASCII-only folding is not a shortcut here, it is the correct rule: RFC 9110
// §5.6.2 confines a field name to `token`, which is ASCII, so a name carrying a
// non-ASCII byte is malformed and cannot equal any token this client looks for.
// strings.EqualFold would additionally apply Unicode case rules to those bytes.
func asciiEqualFold(name []byte, lower string) bool {
	if len(name) != len(lower) {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
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

// parseDecimalOctets parses one RFC 9110 §8.6 `1*DIGIT` field value, reporting
// !ok for anything else.
//
// It exists because strconv.ParseInt is a superset of that ABNF in exactly the
// direction that matters: ParseInt accepts a leading sign, so "+5" would be
// taken as 5 (a value the spec says is invalid framing) and "-5" as -5, which
// then silently fails the contentLen >= 0 test in ReadBodyChunk and degrades the
// response to read-until-close. Both are peer-chosen framing this client must
// refuse, not reinterpret.
//
// The accumulator is checked before each multiply rather than after, so a
// numeral longer than int64 is rejected instead of wrapping — RFC 9110 §8.6:
// "a recipient MUST anticipate potentially large decimal numerals and prevent
// parsing errors due to integer conversion overflows".
func parseDecimalOctets(s string) (int64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := int64(c - '0')
		if n > (math.MaxInt64-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

// parseContentLength parses one Content-Length field value, which is normally a
// bare 1*DIGIT but is also allowed to be a comma-separated list whose values are
// all valid and all identical — RFC 9112 §6.3 rule 5 makes that list the one
// exception to "invalid Content-Length is unrecoverable", to be "processed with
// that single value used as the Content-Length field value". It arises when an
// upstream processor combines duplicate fields per RFC 9110 §5.3, so refusing it
// would break responses that are merely relayed, not hostile.
//
// A list whose values differ ("5, 10") is not the exception and is rejected: it
// is the CL.CL desync with the duplication already folded into one line.
func parseContentLength(value string) (int64, bool) {
	var first int64
	seen := false
	for {
		part := value
		comma := strings.IndexByte(value, ',')
		if comma >= 0 {
			part = value[:comma]
		}
		n, ok := parseDecimalOctets(strings.Trim(part, " \t"))
		if !ok {
			return 0, false
		}
		if !seen {
			first, seen = n, true
		} else if n != first {
			return 0, false
		}
		if comma < 0 {
			return first, true
		}
		value = value[comma+1:]
	}
}

// noteContentLength records one Content-Length field line. The verdict is
// deferred to resolveContentLength; see the clErr field comment.
//
// The first defect is the one kept: later lines cannot clear it, and a response
// with several bad Content-Length lines is no more or less unrecoverable than
// one with a single bad line.
func (ex *Exchange) noteContentLength(value string) {
	if ex.clErr != nil {
		return
	}
	n, ok := parseContentLength(value)
	if !ok {
		ex.clErr = fmt.Errorf("http1: Content-Length %q is not 1*DIGIT: %w",
			truncateForError(value), ErrInvalidContentLength)
		return
	}
	if ex.clSeen && n != ex.clValue {
		// Two field lines that disagree. Per RFC 9110 §5.3 this is the same
		// message as a single "Content-Length: a, b" line, and rule 5 rejects it
		// for the same reason.
		ex.clErr = fmt.Errorf("http1: conflicting Content-Length %d and %d: %w",
			ex.clValue, n, ErrInvalidContentLength)
		return
	}
	ex.clSeen, ex.clValue = true, n
}

// resolveContentLength applies the deferred Content-Length verdict once the
// whole header block has been read, and is the only place ex.contentLen is set
// from a Content-Length field.
//
// The respTE gate is RFC 9112 §6.3 rules 3 and 5 acting together: rule 3 says
// Transfer-Encoding overrides Content-Length, and rule 5's fatal case is scoped
// to a message "received without Transfer-Encoding". So whenever the field is
// PRESENT the Content-Length is not consulted at all — not even to reject it —
// because it no longer decides anything about where this response ends.
//
// The gate is presence of the field, not chunked framing. Those coincide only
// while a substring match makes respChunked true for every Transfer-Encoding
// carrying "chunked" anywhere. Once the final-coding parse lands, "TE: gzip"
// leaves respChunked false while the field is still present, and gating on
// respChunked would let a Content-Length re-frame a body that rule 4 says must
// be read until the server closes — which is the TE.CL desync this file exists
// to close.
func (ex *Exchange) resolveContentLength() error {
	if ex.respTE {
		return nil
	}
	if ex.clErr != nil {
		// Rule 5: "the user agent MUST close the connection to the server and
		// discard the received response." The error discards it; keepAlive=false
		// is what closes the connection rather than pooling it, since the body
		// boundary this response claimed cannot be believed and the stream
		// position is therefore indeterminate.
		ex.keepAlive = false
		return ex.clErr
	}
	if ex.clSeen {
		ex.contentLen = ex.clValue
	}
	return nil
}

// truncateForError bounds a peer-controlled value spliced into an error string.
// A header line may be up to readBufSize (16 KiB) and an error tends to be
// logged, so the full value is not worth carrying; the head of it is enough to
// identify the offender.
func truncateForError(s string) string {
	const max = 64
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// lastTransferCoding returns the final transfer-coding token of one
// Transfer-Encoding field value, lowercased, or "" when the list is empty.
//
// The field is a list (RFC 9112 §6.1), whose element grammar RFC 9112 imports
// rather than defines — it lives in RFC 9110 §10.1.4:
//
//	Transfer-Encoding  = #transfer-coding                    (RFC 9112 §6.1)
//	transfer-coding    = token *( OWS ";" OWS transfer-parameter )
//	transfer-parameter = token BWS "=" BWS ( token / quoted-string )
//	                                                        (RFC 9110 §10.1.4)
//
// Only the last element decides the framing (RFC 9112 §6.3 rules 3 and 4), so
// that is all this reports. Empty elements are skipped rather than counted,
// because RFC 9110 §5.6.1 requires a recipient to "parse and ignore a reasonable
// number of empty list elements" — so "gzip, chunked," still ends in chunked.
//
// The scan tracks quoted-string state, and that is load-bearing rather than
// pedantic. A parameter value may be a quoted-string, which may contain a comma,
// which is then data and not a list delimiter. Splitting the raw value on ","
// cannot tell the two apart, so `gzip;a=", chunked;x=1"` — ONE coding, gzip,
// carrying a parameter — was read as a list whose final element was "chunked".
// The body was then chunk-framed, the connection stayed poolable, and the bytes
// the server chose were left on the socket for the next response to parse as its
// status line: response splitting (§11.1), the exact primitive this function
// exists to deny. An escaped quote inside the string (`a="\", chunked"`) hid it
// the same way, so quoted-pair is honoured too.
//
// Lowering is ASCII-only: RFC 9112 §7 makes transfer-coding names
// case-insensitive, and §5.6.2 confines a token to ASCII, so strings.ToLower
// could only re-encode bytes that were already invalid — and would inflate them
// 3x doing it (see asciiLowerHeaderName).
//
// No allocation on the common path: the scan slices the value in place, and
// asciiLowerHeaderName returns its argument unchanged for an already-lower-case
// token. http1 is a pooled transport on a load generator's hot path and sits
// outside the bench gate's scope, so nothing else would catch a regression here.
func lastTransferCoding(value string) string {

	last := ""

	start := 0

	inQuotes := false

	for i := 0; i <= len(value); i++ {

		if i == len(value) || (value[i] == ',' && !inQuotes) {

			if name := transferCodingName(value[start:i]); name != "" {

				last = name

			}

			start = i + 1

			continue

		}

		switch value[i] {

		case '\\':

			// quoted-pair: inside a quoted-string the next octet is data,

			// including a '"' that would otherwise close it.

			if inQuotes && i+1 < len(value) {

				i++

			}

		case '"':

			inQuotes = !inQuotes

		}

	}

	return asciiLowerHeaderName(last)

}


// transferCodingName returns the bare coding name of one Transfer-Encoding list
// element: OWS-trimmed, with any ";"-delimited parameters removed.
//
// The first ';' can be found without tracking quotes because the name precedes
// every parameter and a token cannot contain a quote — so any quoted-string in
// the element lies after that ';', never before it.
func transferCodingName(el string) string {
	if semi := strings.IndexByte(el, ';'); semi >= 0 {
		el = el[:semi]
	}
	return strings.Trim(el, " \t")
}

// parseChunkSize parses one RFC 9112 §7.1 `chunk-size = 1*HEXDIG`, reporting
// !ok for anything else.
//
// It exists for the same reason parseDecimalOctets does: strconv.ParseInt is a
// superset of the ABNF in the direction that matters. ParseInt(s, 16, 64)
// accepts a leading sign, so "+5" was taken as a five-octet chunk — peer-chosen
// framing spelled in a way the grammar does not admit. "-5" was caught only by a
// separate `size < 0` check downstream, which is the shape of a guard written
// per-symptom rather than per-grammar; parsing to the grammar removes the need
// for either.
//
// The accumulator is checked before each shift rather than after, so a numeral
// longer than int64 is refused instead of wrapping into a small or negative
// size.
func parseChunkSize(s string) (int64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	var n int64
	for i := 0; i < len(s); i++ {
		var d int64
		switch c := s[i]; {
		case c >= '0' && c <= '9':
			d = int64(c - '0')
		case c >= 'a' && c <= 'f':
			d = int64(c-'a') + 10
		case c >= 'A' && c <= 'F':
			d = int64(c-'A') + 10
		default:
			return 0, false
		}
		if n > (math.MaxInt64-d)/16 {
			return 0, false
		}
		n = n*16 + d
	}
	return n, true
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

	// field holds the current logical field line. A line cannot be interpreted
	// when it is read, because the NEXT line may be a continuation of it
	// (RFC 9112 §5.2) — so each line is held until something proves it complete:
	// a non-folded line, or the blank line ending the block.
	//
	// folded is nil until this field actually carries a fold, and only then does
	// anything get copied. That split is not tidiness, it is the whole cost
	// model:
	//
	//   - The fold-free path — every line of every ordinary response — assigns a
	//     string header and allocates nothing.
	//   - The folded path appends into one growing buffer, so joining N
	//     continuations costs O(total bytes) amortised.
	//
	// The first cut of this used `field += " " + rest`, which reallocates and
	// copies everything accumulated so far on EVERY fold. That is O(n²) in the
	// bytes a server chooses to send, and maxHeaderListBytes (8 MiB) bounds the
	// bytes, not the work they buy:
	//
	//	wire  86 KB ->    83 MiB allocated,   8ms
	//	wire 344 KB ->  1281 MiB allocated, 130ms
	//	wire 688 KB ->  5067 MiB allocated, 500ms
	//
	// Doubling the wire quadrupled the memory. A cap that charges what arrives
	// says nothing about what processing it costs.
	var field string
	var folded []byte
	var haveField bool

	for {
		line, err := ex.readLine("header line")
		if err != nil {
			return err
		}
		if line == "" {
			// Blank line = end of headers. Commit whatever field was still
			// being folded into.
			if err := ex.commitHeaderLine(logicalLine(field, folded), haveField, out, parseBody); err != nil {
				return err
			}
			// The Content-Length verdict can only be reached here, once it is
			// known whether a Transfer-Encoding also arrived.
			if parseBody {
				return ex.resolveContentLength()
			}
			return nil
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

		// RFC 9112 §5.2 obs-fold: a line beginning with SP or HTAB continues the
		// PREVIOUS field's value. "A user agent that receives an obs-fold in a
		// response message that is not within a 'message/http' container MUST
		// replace each received obs-fold with one or more SP octets prior to
		// interpreting the field value."
		//
		// Joining rather than rejecting is that MUST. Rejecting is the duty of a
		// server on a request (400) or a proxy on a response (502) — a user
		// agent is given no such option, so the fold is unfolded and the message
		// continues.
		//
		// The joined bytes are deliberately NOT re-split on ':'. Treating a
		// continuation as its own field line is how "X-Junk: a\r\n\tContent-Length:
		// 5" became a real Content-Length that the sender never sent: the client
		// framed the body at 5, left the rest on the socket, and pooled it for
		// the next response to parse as a status line — header smuggling.
		if line[0] == ' ' || line[0] == '\t' {
			if !haveField {
				// A fold with nothing to fold into: obs-fold is OWS CRLF RWS,
				// which only exists after a field line. The block is malformed
				// and its remainder cannot be trusted to be fields at all.
				ex.keepAlive = false
				return fmt.Errorf("http1: obs-fold with no preceding field line: %w", ErrInvalidHeaderBlock)
			}
			if folded == nil {
				// First fold on this field: take one copy of what we have, sized
				// for it plus this continuation, and append from here on.
				folded = make([]byte, 0, len(field)+len(line)+16)
				folded = append(folded, field...)
			}
			// §5.2: "replace each received obs-fold with one or more SP octets".
			folded = append(folded, ' ')
			folded = append(folded, strings.TrimLeft(line, " 	")...)
			continue
		}

		// A non-folded line proves the previous one complete.
		if err := ex.commitHeaderLine(logicalLine(field, folded), haveField, out, parseBody); err != nil {
			return err
		}
		field, folded, haveField = line, nil, true
	}
}

// commitHeaderLine interprets one complete logical field line — obs-folds
// already joined — appending it to out and, when parseBody, feeding the framing
// switch.
func (ex *Exchange) commitHeaderLine(line string, have bool, out *[]hpack.HeaderField, parseBody bool) error {
	if !have {
		return nil
	}
	{
		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			return nil // skip malformed header lines
		}
		name := asciiLowerHeaderName(strings.TrimSpace(line[:colon]))
		value := strings.TrimSpace(line[colon+1:])

		if parseBody {
			switch name {
			case "content-length":
				ex.respCL = true
				ex.noteContentLength(value)
			case "transfer-encoding":
				ex.respTE = true
				// RFC 9112 §6.1: the field value is an ordered, comma-separated
				// list of transfer-coding (element grammar in RFC 9110 §10.1.4).
				// Only "chunked" as the *final* coding gives chunked framing; a
				// substring match instead reads "not-chunked" and "chunked, gzip"
				// as chunked and then desyncs on the first body byte.
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
	return nil
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
		size, ok := parseChunkSize(line)
		if !ok {
			// Any unparseable chunk-size means the chunked framing is corrupt and
			// the stream position indeterminate — the next bytes might be chunk
			// data, a size line, or a whole response the server never sent. The
			// connection must not be pooled, whatever shape the defect took.
			//
			// Only the negative case used to clear keepAlive; a non-hex or empty
			// size returned the error with the socket still marked reusable, so a
			// caller honouring KeepAlive()'s documented contract pooled a
			// mid-stream connection and read an attacker-chosen response on it.
			// Same corrupt framing, same indeterminate position, opposite verdict.
			ex.keepAlive = false
			return 0, false, fmt.Errorf("http1: invalid chunk size %q: %w", truncateForError(line), ErrInvalidChunkSize)
		}
		if size == 0 {
			// Terminal chunk. Consume optional trailers, bounded exactly as the
			// header block is — a trailer section is a header block, and a
			// server can stream one forever just as easily.
			if terr := ex.consumeHeaders(nil, false); terr != nil {
				if errors.Is(terr, io.EOF) || errors.Is(terr, io.ErrUnexpectedEOF) {
					// The server closed straight after the terminal chunk. The
					// body is already complete, so the response is good even
					// though the trailer section never arrived — but the socket
					// is gone, so it must not be pooled.
					ex.keepAlive = false
				} else {
					// Anything else — a read deadline, a too-large block, a
					// malformed fold — means the trailer section is
					// unterminated, so the stream position is indeterminate and
					// the response cannot be called complete.
					//
					// This used to tolerate EVERY error except ErrResponseTooLarge
					// and report done=true, err=nil. A stalled trailer section
					// therefore swallowed the caller's deadline whole and left
					// KeepAlive() true with the server's next bytes unread: the
					// pool handed that socket to the next request, which parsed
					// them as its status line. The comment justifying the
					// tolerance said "typically EOF" — and for EOF it was sound,
					// because a dead socket merely fails on next use. The
					// predicate was "any error"; a deadline is not EOF.
					ex.keepAlive = false
					return 0, false, terr
				}
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

// logicalLine returns the complete field line: the raw line when it carried no
// obs-fold, or the joined buffer when it did.
//
// The string conversion happens once per folded field, not once per fold, which
// is what keeps joining N continuations linear rather than quadratic.
func logicalLine(field string, folded []byte) string {
	if folded == nil {
		return field
	}
	return string(folded)
}
