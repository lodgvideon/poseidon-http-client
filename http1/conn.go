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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
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
// not be interpreted as a well-formed sequence of field lines: an obs-fold
// continuation (RFC 9112 §5.2) with no field line to continue, or a field whose
// name is not a token or whose value carries CR, LF or NUL (RFC 9110 §5.6.2,
// §5.5).
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

// ErrInvalidStatusLine reports that a response's status line was not RFC 9112
// §4's ABNF `status-line = HTTP-version SP status-code SP [ reason-phrase ]`:
// too few fields, an HTTP-version that is not two digits, or a status-code that
// is not exactly 3DIGIT.
//
// The connection is always left un-poolable when this is returned. A status line
// this client cannot parse is a message boundary it cannot find, so it no longer
// knows where this response begins.
//
// This existed only as untyped fmt.Errorf text until the grammar tests that name
// it were found to be asserting `err != nil` — there was no sentinel for them to
// match, so a rejection for an unrelated reason satisfied them.
var ErrInvalidStatusLine = errors.New("http1: invalid status line")

// ErrUnsolicitedUpgrade reports a 101 (Switching Protocols) response to a request
// that offered no Upgrade.
//
// This client never emits an Upgrade header — WriteRequest strips it from the
// wire — so every 101 it can possibly receive names a protocol it never offered,
// which RFC 9110 §7.8 forbids a server from switching to. It was previously
// treated as an ordinary 1xx: consumeHeaders drained it and the loop kept reading
// the SAME socket for a "final" status line, so a server answering any request
// with a 101 followed by a synthetic response had that fabricated response
// returned to the caller with err == nil, on a connection that stayed poolable.
//
// The connection is always left un-poolable when this is returned: after a 101
// the peer considers the connection to be speaking another protocol entirely.
var ErrUnsolicitedUpgrade = errors.New("http1: unsolicited 101 Switching Protocols")

// ErrServerClosedIdle reports that the server closed the connection without
// sending any part of a response — the first read of the status line returned
// EOF with zero bytes.
//
// It is the one HTTP/1.1 failure carrying the guarantee the HTTP/2 and HTTP/3
// retry signals carry (REFUSED_STREAM, GOAWAY, H3_REQUEST_REJECTED): the request
// produced no response at all, so replaying it cannot duplicate an effect the
// server already applied and observed. It is the ordinary end of a pooled
// keep-alive connection, where the server's idle timeout fires between the
// checkout probe and the write.
//
// Deliberately narrow. An EOF after ANY response byte has arrived means the
// server was answering and stopped, which says nothing about whether it
// processed the request, and is NOT this error.
var ErrServerClosedIdle = errors.New("http1: server closed the connection without responding")

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

	// headBufRetainMax caps how large the reusable head-assembly buffer may grow
	// before it is dropped rather than kept for the next exchange, so one request
	// with an enormous header block does not pin that memory for the connection's
	// life. Matches readBufSize: an ordinary head is a few hundred bytes.
	headBufRetainMax = 16 * 1024

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
	// §4.1, matching header.Field.Size(). Charging it per line is what
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
// by using Conn only from one goroutine at a time. That serialization is what
// the connection's single context watchdog rests on — see ctxWatchdog.
type Conn struct {
	nc     net.Conn
	br     *bufio.Reader
	closed atomic.Bool

	// peerMinor records the HTTP/1.x minor version the peer last answered with,
	// and peerMinorKnown whether any response has been read yet. RFC 9112 §6.1
	// forbids sending Transfer-Encoding to a peer that does not handle HTTP/1.1,
	// and this is the only evidence of that the client has. Plain fields, not
	// atomics: a Conn carries one Exchange at a time (no pipelining) and the
	// caller serialises exchanges, so the write that records it happens-before
	// the read that consults it.
	peerMinor      int
	peerMinorKnown bool

	// Cached by initResidue so HasResidue allocates nothing. rawCtl is the real
	// socket's syscall.RawConn (nil when the transport is not a syscall.Conn),
	// ctlFn a control func pre-bound to pendN/pendOK, and layered records that
	// c.nc sits above another net.Conn — i.e. TLS, where octets on the socket are
	// not necessarily application data. Plain fields for the same reason as
	// peerMinor: one exchange at a time, and HasResidue runs only between them.
	rawCtl  syscall.RawConn
	ctlFn   func(uintptr)
	pendN   int
	pendOK  bool
	layered bool

	// headBuf is the reusable scratch the request line + header block is
	// assembled into, so the whole head goes out as ONE Write. net.Buffers used
	// to carry each field line as its own segment for a writev, but writev is
	// void through TLS — crypto/tls has no vectored write, so net.Buffers falls
	// back to one tls.Conn.Write per segment, each its own record and syscall
	// (seven for an ordinary head). conn/ solved the same problem for HTTP/2
	// with a bufio.Writer; here one coalescing buffer does it without pinning
	// the persistent 4-16 KiB a bufio.Writer would. Reused across exchanges
	// under the one-exchange-at-a-time contract, same discipline as peerMinor;
	// dropped when it grows past headBufRetainMax.
	headBuf []byte

	// wd is the connection's single context watchdog, re-armed around each
	// blocking read or write instead of respawned. Its channels are made here
	// once; its goroutine starts on the first arming that needs it.
	wd ctxWatchdog
}

// NewConn wraps nc in a persistent HTTP/1.1 Conn.
// nc must already be connected (TCP + optional TLS handshake complete).
func NewConn(nc net.Conn) *Conn {
	c := &Conn{
		nc: nc,
		br: bufio.NewReaderSize(nc, readBufSize),
	}
	c.wd.armCh = make(chan watchReq)
	c.wd.disarmCh = make(chan struct{})
	c.wd.quit = make(chan struct{})
	c.wd.gone = make(chan struct{})
	c.initResidue()
	return c
}

// IsAlive reports whether the connection is open and usable.
func (c *Conn) IsAlive() bool {
	return !c.closed.Load()
}

// Close closes the underlying network connection.
func (c *Conn) Close() error {
	c.closed.Store(true)
	// Retires the watchdog goroutine, which otherwise lives as long as the
	// connection. It exits from its idle state, so an arming in flight is
	// unaffected: that call's disarm still completes, and the goroutine leaves
	// on the next trip round its loop. sync.Once because Close is idempotent
	// and closing a closed channel panics.
	c.wd.stop.Do(func() { close(c.wd.quit) })
	return c.nc.Close()
}

// ProbeIdle reports whether an idle connection is still safe to reuse. It is a
// near-non-blocking readability check (bounded by a brief read deadline): an
// empty, still-open socket returns true; a peer FIN, or any unsolicited byte,
// returns false.
//
// RFC 9112 §9.6 asks implementations to monitor idle connections for a closure
// signal, and RFC 9110 makes data arriving on a connection with no outstanding
// request not a valid response — so an idle connection with anything readable
// must be evicted rather than handed to the next request, which would otherwise
// consume those bytes as its own status line. IsAlive only reflects a local
// Close; this actually looks at the socket.
//
// It MUST only be called when no exchange is in flight — the caller owns the
// connection and no ReadResponse/ReadBodyChunk is running (e.g. a pool's
// maintenance sweep on a checked-in conn). It reads through the same bufio
// reader an exchange uses, so a concurrent read would corrupt the stream. The
// read deadline is restored to none before returning, so a later real read is
// unaffected.
func (c *Conn) ProbeIdle() bool {
	if c.closed.Load() {
		return false
	}
	// Anything already buffered on an idle conn is unsolicited by definition.
	if c.br.Buffered() > 0 {
		return false
	}
	// A brief FUTURE deadline, not a past one: a past deadline makes the read
	// return a timeout immediately without ever looking at the socket, so an
	// already-arrived byte or FIN would be missed. With a short future deadline a
	// peer FIN or unsolicited byte that is already present returns immediately,
	// while an empty, still-open socket blocks until the deadline.
	//
	// That last case is the common one and it is not cheap: a HEALTHY idle conn
	// costs the full deadline every time, measured at ~1.5ms here including the
	// two SetReadDeadline calls. This comment used to say the wait "costs nothing
	// under load" and that the sweep "only ticks while the pool is idle" — neither
	// is true. The health-check ticker is unconditional, and a pool that is busy
	// overall still has idle conns at the tick instant, one probe each. A caller
	// must therefore not run this where a stall is visible: the HTTP/1.1 pool
	// learned that the hard way and now probes off its actor goroutine (see
	// client.h1Pool.startHealthSweep).
	//
	// A timeout means healthy; a byte means unsolicited data; any other error
	// (EOF, reset) means the peer is gone.
	//
	// FUTURE deadline: this asks the socket, and it has to.
	err := c.peekUnder(time.Now().Add(probeWindow))
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return false
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
	// statusName is the :status field name ReadResponse synthesises for the
	// H2-style client layer. It is fixed, so it is built once here rather than
	// converted from its literal on every response — the same treatment
	// client/client.go and grpc/conn.go already give their pseudo-header names.
	statusName = []byte(":status")
)

// statusDigits holds the three ASCII digits of every status code 100-999, so the
// synthesised :status VALUE costs no allocation either. A package-level array is
// zero-initialised data, not a heap object, so the whole table is free.
var statusDigits = buildStatusDigits()

func buildStatusDigits() [3000]byte {
	var d [3000]byte
	for code := 100; code <= 999; code++ {
		i := code * 3
		d[i] = byte('0' + code/100)
		d[i+1] = byte('0' + (code/10)%10)
		d[i+2] = byte('0' + code%10)
	}
	return d
}

// statusValue returns code rendered as three ASCII digits, without allocating.
//
// The result is shared and immutable: it aliases statusDigits, which nothing
// writes to after construction. That is safe here only because nothing mutates a
// header field's value in place — the returned fields reach the caller through
// StreamEvent, and every consumer reads them (client/request.go copies via
// strings.ToLower(string(...)), conn/handler.go compares bytes). The capacity is
// capped to the three digits so that if a consumer ever DOES append to a value,
// it reallocates instead of scribbling over the next code's digits.
//
// A code outside 100-999 cannot come from readStatusLine, which rejects anything
// that is not three digits, but the fallback keeps this total rather than making
// the caller prove that.
func statusValue(code int) []byte {
	if code < 100 || code > 999 {
		return []byte(strconv.Itoa(code))
	}
	i := code * 3
	return statusDigits[i : i+3 : i+3]
}

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
	// reqContentLen is the Content-Length this request declared (-1 when none
	// was sent, including the chunked case), and reqBodyWritten counts the body
	// octets actually handed to the socket. RFC 9110 §8.6 forbids a sender from
	// emitting a Content-Length it knows to be incorrect: a body that over- or
	// under-runs its declaration desyncs the peer, which then reads the next
	// request's bytes as this body's tail (or waits for octets that never come).
	// Nothing reconciled the two, so a caller streaming more or fewer bytes than
	// it declared put a smuggling primitive on the wire and the connection was
	// still pooled afterwards.
	reqContentLen  int64
	reqBodyWritten int64
	// condemned latches, one way, that this connection must not be reused.
	//
	// One latch, because it used to be three — condemned for the request side,
	// closeSeen for an explicit Connection: close, noReuse for an indeterminate
	// body boundary — plus keepAlive itself, and every new condemnation site had
	// to remember which subset to set. The comments on the old fields narrated
	// that: each was added to stop a LATER header line from undoing an earlier
	// verdict, which is a property that belongs to the decision, not to the
	// reason for it.
	//
	// Order-safety is now structural. Several condemnations are safe today only
	// by PLACEMENT — the Content-Length ones run after the last field line, so
	// the keep-alive resurrection below cannot reach them — and moving any of
	// them per-line, as the Transfer-Encoding check already is, would have
	// reopened the hole the three latches were patching.
	//
	// What must NOT route through here is the HTTP/1.0 default at the top of
	// ReadResponse: that false is reversible by design, by a Connection:
	// keep-alive on the same response (RFC 2616 §8.1.2.1), and latching it would
	// cost a connection per HTTP/1.0 response.
	condemned bool

	// readCtx is the context of the ReadResponse that opened this response, kept
	// so ReadBodyChunk — whose signature predates ctx and is public API — can
	// honour cancellation too. Without it a caller with a cancellable,
	// deadline-less context hung forever mid-body against a silent peer, and the
	// pool slot that exchange held was never released.
	readCtx context.Context
	// readConsumedNothing records that the last failing read consumed zero
	// bytes, which is what separates "the server never answered" from "the
	// server answered and stopped". Only ReadResponse's first status-line read
	// acts on it; see ErrServerClosedIdle.
	readConsumedNothing bool

	// response side
	statusCode int
	// keepAlive is the POSITIVE persistence signal, derived from the response
	// version and then possibly raised by a Connection: keep-alive. It is written
	// at exactly two places and is not where condemnations live — those latch
	// condemned. RFC 9110 §5.3 combines repeated Connection field lines into one
	// value, so "close" then "keep-alive" on separate lines means "close,
	// keep-alive" and close wins; the latch is what makes that order-independent.
	keepAlive   bool
	respChunked bool
	// clSeen, clValue and clErr accumulate the Content-Length decision across
	// the header block instead of committing to it line by line. RFC 9112 §6.3
	// rule 5 only makes an invalid Content-Length fatal "without
	// Transfer-Encoding", and rule 3 lets Transfer-Encoding override it — but
	// either header may arrive first, so neither question can be answered until
	// the blank line. Resolving early is what would make the verdict depend on
	// the order the server chose to send its headers in.
	clSeen  bool  // a Content-Length field was present
	clValue int64 // its value, when clSeen && clErr == nil
	clErr   error // first Content-Length defect seen, if any
	// respTE and respCL record mere presence of Transfer-Encoding and
	// Content-Length in the response head, independent of their values and of
	// the order they arrived in. RFC 9112 §6.3 rule 3 keys on both being
	// present, and either can be parsed first.
	respTE bool
	respCL bool
	// contentLen is the response body's byte count, or contentLenUnknown when it
	// is not a byte count at all.
	contentLen     int64
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

// contentLenUnknown marks a response body whose length is not a byte count.
//
// Two cases share the value, and deliberately: chunked framing, and §6.3 rule
// 4's read-until-close. ReadBodyChunk asks ex.respChunked first and only then
// whether contentLen >= 0, so the two never need telling apart by this field —
// and nothing else reads it. A second sentinel (-2, "chunked overrides
// content-length") sat here for exactly one write and no read, contradicting the
// field's own documented -1 while no code could observe the difference.
const contentLenUnknown = -1

// probeWindow is how long a future-deadline probe waits for the socket to say
// something. A healthy idle connection costs the whole window, so it is the
// price of every such probe, not a timeout that rarely fires.
const probeWindow = time.Millisecond

// peekUnder runs the one-byte probe under deadline t and clears the read
// deadline again afterwards, whatever the outcome. It returns Peek's error: nil
// means a byte is available, a net.Error with Timeout() means none was, and
// anything else (EOF, reset) means the peer is gone.
//
// Four call sites ran these three lines with the deadline as their only
// difference, and that difference is the whole decision — which is why it stays
// at the call site with its own reason, and only the mechanism lives here:
//
//   - A deadline in the FUTURE asks the SOCKET. A past one would return a
//     timeout immediately without ever issuing a recv, so an already-arrived
//     byte or FIN would be missed — a probe that fails open.
//   - A deadline in the PAST asks crypto/tls's own input buffer instead.
//     crypto/tls checks no deadline itself, so the read decrypts a record it has
//     already buffered and returns the plaintext without touching the socket,
//     and reports a timeout the moment it would have to.
//
// The restore is defence in depth rather than the thing that prevents a stale
// deadline, and it is worth being exact about which: every read path arms its
// own deadline on entry — ReadResponse from its ctx, ReadBodyChunk through
// armCancel, both setting the zero time when there is no ctx deadline — so a
// deadline left behind here is overwritten before anything could read under it.
// Removing the restore leaves the whole http1 suite green, verified twice; it is
// kept because that argument holds only as long as every future read path keeps
// arming on entry, and the line costs one syscall on a path that already made
// two.
func (c *Conn) peekUnder(t time.Time) error {
	_ = c.nc.SetReadDeadline(t)
	_, err := c.br.Peek(1)
	_ = c.nc.SetReadDeadline(time.Time{})
	return err
}

// deadlineLongPast is the "release any blocked I/O now" deadline: unambiguously
// in the past on every clock, unlike time.Now() on a coarse timer.
var deadlineLongPast = time.Unix(1, 0)

// deadlineKind selects which of the socket's two deadlines an arming trips.
//
// It replaces the `set func(time.Time) error` these helpers used to take. Every
// call site spelled that argument as a method value (`c.nc.SetReadDeadline`),
// and a method value is a heap-allocated closure — one of the per-call
// allocations the per-connection watchdog exists to remove, so the selector has
// to be a value rather than a func.
type deadlineKind bool

const (
	readDeadline  deadlineKind = false
	writeDeadline deadlineKind = true
)

// setDeadline applies t to the deadline named by k.
func (c *Conn) setDeadline(k deadlineKind, t time.Time) error {
	if k == writeDeadline {
		return c.nc.SetWriteDeadline(t)
	}
	return c.nc.SetReadDeadline(t)
}

// watchReq is one arming. It travels by value over an unbuffered channel, so
// arming the watchdog puts nothing on the heap.
type watchReq struct {
	ctx  context.Context
	kind deadlineKind
}

// ctxWatchdog is the connection's context watchdog: ONE goroutine, started on
// the first arming that needs one and re-armed for the duration of each
// blocking read or write, in place of a goroutine + two channels + two closures
// per call. A single request/response arms it several times — the head write,
// each body write, the response read, every body chunk — which is why the
// per-call form was the second and third HTTP/1.1 allocation site by bytes.
//
// It rests on the serialization Conn already documents: at most one exchange at
// a time, and within an exchange the calls are ordered (write the request, then
// read the response). Every arming is also paired with a deferred disarm in the
// SAME call, so no arming outlives the function that made it, and within that
// contract exactly one arming is live at any instant. A caller that breaks the
// contract — writing the body from one goroutine while another reads — blocks
// its second arming here until the first disarms, but it is already racing on
// the Exchange's own plain fields, so this is not a new hazard.
//
// The state machine is two rendezvous on unbuffered channels:
//
//	idle --armCh--> watching --disarmCh--> idle
//
// disarmCh is what keeps "the release waits for the watchdog to exit" true of a
// goroutine that never exits. An unbuffered send completes only once the
// receiver has committed to one branch of its select, so by the time disarm
// returns the watchdog has either already tripped the deadline or can no longer
// trip it for this arming — exactly what the per-call `<-exited` guaranteed.
// That is what lets releaseDeadline clear the deadline with no cancellation
// racing in behind the clear to latch a past deadline on a connection about to
// be pooled and reused.
//
// The goroutine lives until Conn.Close, which every discard path already owes
// the connection anyway (it holds an fd), so this adds no lifetime obligation
// that was not already there.
type ctxWatchdog struct {
	// started gates the lazy goroutine: a connection only ever handed
	// non-cancellable contexts (ctx.Done() == nil) never starts one. An atomic
	// CAS rather than a sync.Once because Once.Do takes a func, and the closure
	// handed to it escapes into doSlow — a heap allocation on EVERY arming,
	// which is exactly the cost this type exists to remove.
	started atomic.Bool
	// stop guards close(quit) so a second Conn.Close cannot panic. Close is not
	// on the hot path, so a Once costs nothing here.
	stop     sync.Once
	armCh    chan watchReq
	disarmCh chan struct{}
	// quit is closed by Conn.Close, gone by the goroutine as it leaves. gone is
	// what stops an arming that races Close from blocking forever on a channel
	// no one will ever receive from.
	quit chan struct{}
	gone chan struct{}
}

// watch is the watchdog goroutine: idle until armed, bound to one context until
// disarmed, for the life of the connection.
func (c *Conn) watch() {
	defer close(c.wd.gone)
	for {
		select {
		case <-c.wd.quit:
			return
		case req := <-c.wd.armCh:
			select {
			case <-req.ctx.Done():
				// A fixed instant in the past, not time.Now(): "now" can land a
				// hair in the future on a coarse clock and leave the call
				// blocked. Nor a short future deadline — that leaves the call
				// blocked until it elapses, which is the opposite of the job.
				_ = c.setDeadline(req.kind, deadlineLongPast)
				// The disarm always arrives: the deadline just tripped is what
				// returns the blocked call, and that call's deferred disarm is
				// the other half of this arming.
				<-c.wd.disarmCh
			case <-c.wd.disarmCh:
			}
		}
	}
}

// armWatch binds ctx to the watchdog for the duration of one blocking call and
// reports whether it took. The result MUST be handed to disarmWatch, in the
// same call, on every exit path.
func (c *Conn) armWatch(ctx context.Context, k deadlineKind) bool {
	if ctx == nil || ctx.Done() == nil {
		return false
	}
	if !c.wd.started.Load() && c.wd.started.CompareAndSwap(false, true) {
		go c.watch()
	}
	select {
	case c.wd.armCh <- watchReq{ctx: ctx, kind: k}:
		return true
	case <-c.wd.gone:
		// Conn.Close won the race. The socket is already closed, so a blocking
		// call returns on its own and there is nothing left for a watchdog to
		// release.
		return false
	}
}

// disarmWatch returns the watchdog to idle. It blocks until the watchdog has
// committed to leaving this connection's deadlines alone — see ctxWatchdog.
func (c *Conn) disarmWatch(armed bool) {
	if !armed {
		return
	}
	c.wd.disarmCh <- struct{}{}
}

// armCancel releases a blocked call when ctx is cancelled, WITHOUT otherwise
// touching the deadline. Pair it with disarmWatch.
//
// The read path cannot use armDeadline: ReadResponse deliberately installs its
// deadline on every entry and never clears it on exit, so that a pooled
// connection can never inherit the previous exchange's deadline. A release that
// cleared it would reintroduce exactly that. Cancellation still has to be
// honoured though — a blocking Read cannot be selected on, and http1 is
// single-goroutine — so this arms only the "make the deadline elapse now" half.
//
// If the watchdog fired, the past deadline is left in place: that exchange has
// been cancelled and its connection is discarded, never pooled.
func (c *Conn) armCancel(ctx context.Context, k deadlineKind) bool {
	return c.armWatch(ctx, k)
}

// armDeadline binds ctx to a net.Conn deadline for the duration of one blocking
// I/O call. Pair it with releaseDeadline, passing back what it returned.
//
// When ctx carries a deadline it is applied directly — the cheap path, and the
// shape callers are told to use. When ctx is cancellable but carries NO deadline
// there is nothing to apply, and a blocking Write cannot be selected on: a peer
// that simply stops reading then hangs the call forever, because this exchange is
// single-goroutine and has no other thread to notice the cancellation. The
// watchdog covers exactly that gap by making the deadline elapse when ctx is
// done, which is what unblocks the syscall.
func (c *Conn) armDeadline(ctx context.Context, k deadlineKind) bool {
	// The deadline and the cancellation are NOT alternatives. Treating them as
	// either/or meant a context that carries both — which is exactly what
	// client.Do builds from Request.Timeout — got no watchdog, so cancelling such
	// a request did not release a blocked call until its deadline expired. Hence
	// an unconditional arm below, not an else-branch of this one.
	if dl, ok := ctx.Deadline(); ok {
		_ = c.setDeadline(k, dl)
	} else {
		_ = c.setDeadline(k, time.Time{})
	}
	return c.armWatch(ctx, k)
}

// releaseDeadline disarms the watchdog and clears the deadline armDeadline
// installed.
//
// The disarm happens first and blocks until the watchdog can no longer fire, so
// a cancellation racing the return cannot land its past deadline after the
// clear and leave it latched on a connection that is about to be reused.
func (c *Conn) releaseDeadline(k deadlineKind, armed bool) {
	c.disarmWatch(armed)
	_ = c.setDeadline(k, time.Time{})
}

// parseRequestContentLength parses a caller-supplied Content-Length value as RFC
// 9110 §8.6's `Content-Length = 1*DIGIT`.
//
// Surrounding OWS is tolerated — a field value's edges are not part of it — but
// nothing else is: a sign, or a comma-folded list like "5, 10" (which RFC 9110
// §5.3 makes equivalent to two field lines, i.e. the CL.CL smuggling primitive
// on one line), is refused rather than best-effort parsed. That matches how this
// client's own receive side treats the same bytes; emitting a shape it would
// reject on arrival is exactly the asymmetry that lets a request smuggle.
func parseRequestContentLength(v string) (int64, bool) {
	// OWS is SP / HTAB (RFC 9110 §5.6.3), not Unicode whitespace: TrimSpace also
	// ate VT/FF/NEL, so a value this client's own receive side rejects was
	// emitted as valid.
	s := strings.Trim(v, " \t")
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// parseHTTP1Version parses an HTTP/1.x version token and returns its minor digit.
//
// RFC 9112 §2.3: HTTP-version = "HTTP" "/" DIGIT "." DIGIT — one digit each side,
// so a well-formed HTTP/1.x token is exactly 8 bytes. The previous loose
// HasPrefix(s, "HTTP/1") accept let "HTTP/1.00" through, which then compared
// unequal to the literal "HTTP/1.0" and slipped past every version-keyed rule
// while still being an HTTP/1.0 message — it seeded persistent and bypassed the
// §6.1 Transfer-Encoding close. Parsing once, and keying the rules on the number,
// makes that unrepresentable.
func parseHTTP1Version(s string) (minor int, ok bool) {
	if len(s) != 8 || s[:5] != "HTTP/" || s[5] != '1' || s[6] != '.' {
		return 0, false
	}
	if s[7] < '0' || s[7] > '9' {
		return 0, false
	}
	return int(s[7] - '0'), true
}

// parseStatusCode parses RFC 9112 §4's `status-code = 3DIGIT`.
//
// strconv.Atoi is a superset of that ABNF in the direction that decides control
// flow: it accepts a sign, any digit count, and leading zeros, so "-5", "+99",
// "99" and "0000200" all produced a number. Every one of those below 200 entered
// ReadResponse's 1xx interim-drain loop — which discards a header block and
// parses the NEXT line off the socket as another status line. That is a
// server-triggered "read me another response" primitive built out of a status
// line that is not a status line, and §4 warns about exactly this outcome:
// "lenient parsing can result in response splitting security vulnerabilities if
// there are multiple recipients of the message and each has its own unique
// interpretation of robustness".
//
// 3DIGIT is the WHOLE rule, and deliberately so. An earlier version of this
// also required the first digit to be 1-5, reasoning that §15 defines no other
// classes. That was this client's own addition, it contradicted RFC 9110 §15.1
// — which tells a recipient to process an unrecognised status as the x00 of its
// class rather than fail — and it bought nothing, because the interim-drain
// loop is gated on the 1xx range directly. What it cost was every request
// against a host that answers "HTTP/1.1 999 Request denied", a shape deployed
// in the wild; net/http accepts it, and so does this client's own HTTP/2 status
// parser. A parse that hard-fails a well-formed status line is an outage, not
// strictness.
func parseStatusCode(s string) (int, bool) {
	if len(s) != 3 {
		return 0, false
	}
	for i := 0; i < 3; i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	return int(s[0]-'0')*100 + int(s[1]-'0')*10 + int(s[2]-'0'), true
}

// hasConnectionOption reports whether a Connection field value carries opt as a
// list member.
//
// RFC 9110 §7.6.1 defines Connection as a #token list, so a substring test is
// wrong in both directions and unsafe in one: "x-keep-alive-probe" contains
// "keep-alive" without being it, which let a peer flip an otherwise
// close-by-default response back to poolable with a field this client does not
// otherwise honour. Matching whole comma-separated members closes that, and
// EqualFold avoids lower-casing the peer's bytes into a new allocation.
func hasConnectionOption(value, opt string) bool {
	for value != "" {
		tok := value
		if i := strings.IndexByte(value, ','); i >= 0 {
			tok, value = value[:i], value[i+1:]
		} else {
			value = ""
		}
		// OWS, not Unicode whitespace — see the note in commitHeaderLine.
		if strings.EqualFold(strings.Trim(tok, " \t"), opt) {
			return true
		}
	}
	return false
}

// validateFields checks everything WriteRequest is about to encode, before it
// encodes any of it.
//
// Up front rather than field-by-field during the build because a partial write
// is itself the failure being prevented: bailing out halfway through the
// net.Buffers would put the good prefix of a poisoned request on the wire and
// leave the stream mid-message, which is a worse outcome than either sending it
// or refusing it.
func validateFields(method, path, authority string, fields []header.Field) error {
	if !validToken(method) {
		return fmt.Errorf("%w: method %q is not a token (RFC 9110 §9.1)", ErrInvalidRequest, method)
	}
	if !validRequestTarget(path) {
		return fmt.Errorf("%w: target %q contains a space or control character (RFC 9112 §3)",
			ErrInvalidRequest, path)
	}
	// CONNECT is refused rather than framed. RFC 9112 §6.3 rule 2: "Any 2xx
	// (Successful) response to a CONNECT request implies that the connection will
	// become a tunnel immediately after the empty line that concludes the header
	// fields. A client MUST ignore any Content-Length or Transfer-Encoding header
	// fields received in such a message."
	//
	// This Exchange has no way to honour that. Its response path frames every
	// message by the fields the peer sent, so a 2xx to CONNECT read the tunnel's
	// first octets back as a message body — and with no Content-Length at all it
	// fell to rule 4's read-until-close and blocked until the socket died. There
	// is also no API here to hand the caller the tunnelled socket afterwards, so
	// implementing rule 2's framing would produce a "successful" CONNECT whose
	// tunnel nobody can reach: a conformant desync instead of an obvious one.
	//
	// Refusing at the send gate is the honest boundary — the request never
	// reaches the wire, so no tunnel is ever half-established. Callers wanting a
	// CONNECT proxy tunnel use conn.ProxyDialer, which speaks it directly.
	if method == "CONNECT" {
		return fmt.Errorf("%w: CONNECT is not supported on this exchange; a 2xx to CONNECT "+
			"makes the connection a tunnel (RFC 9112 §6.3 rule 2) and this client has no "+
			"tunnel API — use conn.ProxyDialer", ErrInvalidRequest)
	}
	// The authority becomes the Host field value, so it answers to §5.5 like any
	// other value. Emptiness is deliberately NOT an error: RFC 9112 §3.2 says a
	// client whose target URI has no authority MUST send Host with an empty
	// value, so "Host: \r\n" is the conformant output here, not a rejection.
	if !validFieldValue([]byte(authority)) {
		return fmt.Errorf("%w: :authority contains CR, LF or NUL (RFC 9110 §5.5)", ErrInvalidRequest)
	}
	// RFC 9110 §4.2.4 / RFC 9112 §3.2: the Host field value MUST exclude any
	// userinfo subcomponent and its "@" delimiter. '@' cannot appear in a bare
	// host[:port], so its presence is a caller-supplied userinfo that must not be
	// emitted verbatim into Host. (The client/ layer rejects this earlier for
	// Do callers; this guards direct http1 users.)
	if strings.IndexByte(authority, '@') >= 0 {
		return fmt.Errorf("%w: :authority %q carries userinfo '@' (RFC 9110 §4.2.4, RFC 9112 §3.2)",
			ErrInvalidRequest, authority)
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
// Assembles the whole head into the connection's reusable buffer and writes it
// once — see Conn.headBuf for why not net.Buffers.
func (ex *Exchange) WriteRequest(ctx context.Context, fields []header.Field, endStream bool) error {
	// Checked synchronously: armDeadline's watchdog is asynchronous, so a short
	// write can complete before it observes an already-dead context.
	if err := ctx.Err(); err != nil {
		return err
	}
	var method, path, authority string
	var hasContentLength bool
	var clCount int
	var clValue string

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
				clCount++
				clValue = string(f.Value)
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

	// RFC 9110 §5.3 makes Content-Length a singleton field; two field lines are
	// the CL.CL request-smuggling primitive (RFC 9112 §11.2) when they disagree
	// and are never legitimate for a sender to emit. Refuse rather than pick one.
	if clCount > 1 {
		return fmt.Errorf("%w: %d Content-Length header fields; a request must carry at most one (RFC 9110 §5.3, RFC 9112 §11.2)",
			ErrInvalidRequest, clCount)
	}
	// Parse the caller's Content-Length whenever one is present, not only on the
	// bodyless path.
	//
	// RFC 9110 §8.6's ABNF is `Content-Length = 1*DIGIT`, so a folded list like
	// "5, 10" — which §5.3 makes equivalent to two field lines — is the CL.CL
	// smuggling primitive on one line, and this client's own receive side already
	// refuses that shape. Parsing here also hands WriteBody the declared length to
	// reconcile the body against: §8.6 forbids a sender from emitting a
	// Content-Length it knows to be incorrect, and a declared length that does not
	// match the octets written desyncs the peer.
	ex.reqContentLen = -1
	if hasContentLength {
		n, ok := parseRequestContentLength(clValue)
		if !ok {
			return fmt.Errorf("%w: Content-Length %q is not 1*DIGIT (RFC 9110 §8.6)",
				ErrInvalidRequest, clValue)
		}
		// endStream means no body follows, so a non-zero declaration is octets
		// that will never be written — a CL.0 desync on a reused connection,
		// where the peer reads the next request's bytes as this phantom body. A
		// caller "0" is fine (declares and sends nothing).
		if endStream && n != 0 {
			return fmt.Errorf("%w: Content-Length %q on a request with no body (RFC 9110 §8.6)",
				ErrInvalidRequest, clValue)
		}
		ex.reqContentLen = n
	}

	// Determine how to frame the request body.
	if !endStream && !hasContentLength {
		// RFC 9112 §6.1: a client MUST NOT send Transfer-Encoding to a peer it does
		// not know handles HTTP/1.1. Framing was decided with no reference to the
		// peer at all, so once a 1.0 response was pooled — which "Connection:
		// keep-alive" makes possible — the next request chunked itself to a peer
		// the client had itself observed as 1.0. A caller who wants a body on such
		// a connection must declare a Content-Length.
		if ex.c.peerMinorKnown && ex.c.peerMinor == 0 {
			return fmt.Errorf("%w: cannot chunk a request body to a peer that answered HTTP/1.0; "+
				"send a Content-Length instead (RFC 9112 §6.1)", ErrInvalidRequest)
		}
		ex.reqChunked = true
	}

	// Assemble the whole head into the connection's reusable buffer and send it
	// as one Write — see Conn.headBuf for why not net.Buffers.
	h := ex.c.headBuf[:0]
	h = append(h, method...)
	h = append(h, ' ')
	h = append(h, path...)
	h = append(h, " HTTP/1.1\r\n"...)
	h = append(h, "Host: "...)
	h = append(h, authority...)
	h = append(h, "\r\n"...)

	for _, f := range fields {
		if len(f.Name) == 0 || f.Name[0] == ':' {
			continue // skip pseudo-headers
		}
		// H2 forbidden / hop-by-hop headers; we manage them ourselves.
		if isConnectionManagedName(f.Name) {
			continue
		}
		// Byte-wise, for the reason stated at the Content-Length probe above:
		// the name is peer- or caller-supplied and this runs per header per
		// request. string(f.Name) plus strings.ToLower cost one allocation for
		// every name spelled with a capital — which is how RFC 9110 spells them,
		// so the canonical shape was the expensive one. Measured before the
		// change on eight fields, five of them capitalised: 16 allocs/op against
		// 11 for the same request with the names already lower-case.
		h = appendASCIILower(h, f.Name)
		h = append(h, ": "...)
		h = append(h, f.Value...)
		h = append(h, "\r\n"...)
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
				h = append(h, "Content-Length: 0\r\n"...)
				// Recorded, not merely written: leaving reqContentLen at -1 meant
				// the body reconciliation skipped this request, so a caller that
				// went on to call WriteBody wrote a whole second request after a
				// head declaring zero octets — a CL.0 desync built from the
				// library's own header and reported as success.
				ex.reqContentLen = 0
			}
		}
	} else if ex.reqChunked {
		h = append(h, "Transfer-Encoding: chunked\r\n"...)
	}
	// else: Content-Length already in user-supplied headers.

	h = append(h, "\r\n"...) // blank line ending headers

	return ex.writeHead(ctx, h)
}

// writeHead puts the assembled head on the wire, condemning the exchange if the
// socket failed part-way through.
//
// A failed head write is a PARTIAL head: a short Write reports the error after
// some prefix has already gone, so the peer is mid-request-line or mid-field
// and cannot resynchronise. Condemning is what stops ReadResponse's
// `keepAlive = respMinor >= 1 && !ex.condemned` from handing the socket back —
// the read side has had this invariant as a blanket defer since #310, and the
// write side simply never did.
//
// Split out from WriteRequest only because that function is at the gocyclo
// ceiling; the split is along the seam where the head stops being assembled and
// starts being sent.
func (ex *Exchange) writeHead(ctx context.Context, head []byte) error {
	// Keep the assembled buffer for the next exchange on this connection, but
	// drop an outsized one rather than pin it for the connection's life. The
	// bytes have been read into `head`, so retaining it is safe regardless of
	// how the Write below goes. Done here rather than in WriteRequest to keep
	// that function under the gocyclo ceiling.
	if cap(head) <= headBufRetainMax {
		ex.c.headBuf = head
	} else {
		ex.c.headBuf = nil
	}
	armed := ex.c.armDeadline(ctx, writeDeadline)
	defer ex.c.releaseDeadline(writeDeadline, armed)
	_, err := ex.c.nc.Write(head)
	if err != nil {
		ex.condemn()
	}
	return err
}

// WriteBody writes a body chunk to the wire.
// When fin is true this is the last chunk; WriteBody must not be called again.
// Omit WriteBody entirely when endStream was true in WriteRequest.
func (ex *Exchange) WriteBody(ctx context.Context, p []byte, fin bool) error {
	// Synchronous, for the same reason as WriteRequest: the watchdog below only
	// covers a cancellation that arrives while a write is already blocked.
	if err := ctx.Err(); err != nil {
		return err
	}
	armed := ex.c.armDeadline(ctx, writeDeadline)
	defer ex.c.releaseDeadline(writeDeadline, armed)

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
		if err != nil {
			// Same as the head: a partial chunk leaves the peer's chunked decoder
			// mid-frame, so the octet boundary is no longer agreed.
			ex.condemn()
		}
		return err
	}

	// Non-chunked: write data directly (Content-Length governs framing), keeping
	// the octets written reconciled against what the head declared.
	//
	// Over-run is refused BEFORE the write: once the excess is on the wire the
	// desync has already happened and cannot be taken back — the peer reads those
	// octets as the start of the next request. Under-run can only be judged at
	// fin, where the peer would instead block waiting for octets that never come.
	// Either way the connection is no longer safe to reuse.
	// A head that declared no framing at all — no Content-Length, not chunked —
	// promised the peer a message ending at the blank line. Octets written after
	// it are not a body: they are whatever the peer's parser makes of them, and
	// what it makes of them is the next request-line. That is request smuggling
	// (§11.2) with the caller's own bytes, so it is refused before the write for
	// the same reason over-run is — once the octets are gone the desync is done.
	//
	// Reachable for exactly the methods that get no synthesized "Content-Length:
	// 0" on a bodyless head: GET, HEAD, DELETE, OPTIONS. POST/PUT/PATCH were
	// already covered because their declared 0 made the over-run check below fire
	// — the guard existed, the value it keyed on just wasn't always there.
	if len(p) > 0 && ex.reqContentLen < 0 {
		ex.condemn()
		return fmt.Errorf("%w: request body written after a head that declared no "+
			"Content-Length and no chunked framing (RFC 9112 §6)", ErrInvalidRequest)
	}
	if ex.reqContentLen >= 0 && ex.reqBodyWritten+int64(len(p)) > ex.reqContentLen {
		ex.condemn()
		return fmt.Errorf("%w: request body exceeds its declared Content-Length %d (RFC 9110 §8.6)",
			ErrInvalidRequest, ex.reqContentLen)
	}
	if len(p) > 0 {
		n, err := ex.c.nc.Write(p)
		ex.reqBodyWritten += int64(n)
		if err != nil {
			// A short write under a declared Content-Length owes the peer octets
			// it will block waiting for.
			ex.condemn()
			return err
		}
	}
	if fin && ex.reqContentLen >= 0 && ex.reqBodyWritten != ex.reqContentLen {
		ex.condemn()
		return fmt.Errorf("%w: request body is %d octets but declared Content-Length %d (RFC 9110 §8.6)",
			ErrInvalidRequest, ex.reqBodyWritten, ex.reqContentLen)
	}
	return nil
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
//
// Exactly one CRLF is stripped — or one bare LF, since §2.2 lets a recipient
// "recognize a single LF as a line terminator and ignore any preceding CR". A CR left at the
// end after that is a bare CR, and §2.2 requires a recipient of one to "consider
// that element to be invalid or replace each bare CR with SP". The TrimRight
// this replaces did neither: it DELETED the run, so "Content-Length: 5\r\r\n"
// became a clean Content-Length here while a parser honouring §2.2 saw an
// invalid field — two recipients, two framings, which is the disagreement §11.1
// names as the response-splitting primitive. Rejecting is the option that cannot
// desync.
func (ex *Exchange) readLine(what string) (string, error) {
	line, err := ex.c.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			// Refusing the line leaves the stream mid-line and its position
			// indeterminate: resynchronising would mean reading exactly the
			// bytes being refused. The connection must not be pooled.
			ex.condemn()
			return "", fmt.Errorf("http1: %s exceeds %d bytes: %w", what, readBufSize, ErrResponseTooLarge)
		}
		// Record whether this failure arrived with nothing consumed. ReadResponse
		// turns that — on the FIRST status-line read only — into ErrServerClosedIdle;
		// nowhere else can tell "no response at all" from "a response that stopped".
		ex.readConsumedNothing = len(line) == 0
		return "", fmt.Errorf("http1: read %s: %w", what, err)
	}
	s := string(line[:len(line)-1]) // the delimiting LF
	s = strings.TrimSuffix(s, "\r")
	if strings.HasSuffix(s, "\r") {
		ex.condemn()
		return "", fmt.Errorf("http1: %s ends with a bare CR: %w", what, ErrInvalidHeaderBlock)
	}
	return s, nil
}

// ReadResponse reads the HTTP/1.1 response status line and headers.
// It skips 1xx informational responses automatically and blocks until a
// final (≥200) status is received.
// Returns the response headers as []header.Field. The first element is
// always the ":status" pseudo-header for compatibility with the client layer.
func (ex *Exchange) ReadResponse(ctx context.Context) (statusCode int, headers []header.Field, err error) {
	// Any failure reading the head leaves the stream position unknown, so the
	// connection must not be pooled. Several exits (a truncated or stalled header
	// block, a cancelled read) returned with keepAlive still true; deciding it
	// once here is the same "no exit path has to remember" the body reader uses.
	defer func() {
		if err != nil {
			ex.condemn()
		}
	}()
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
	// Kept for the body reads, which have no ctx of their own.
	ex.readCtx = ctx
	armed := ex.c.armCancel(ctx, readDeadline)
	defer ex.c.disarmWatch(armed)

	var respMinor int
	var interim int
	firstRead := true
	for {
		// Status line: "HTTP/1.x NNN Reason\r\n"
		ex.readConsumedNothing = false
		line, rerr := ex.readLine("status line")
		if rerr != nil {
			// Nothing of a response ever arrived on the first read: the server closed
			// an idle keep-alive rather than answering. That is the one H1 failure a
			// caller may safely replay, so it gets a type the retry classifier can
			// match instead of an opaque wrapped EOF.
			if firstRead && ex.readConsumedNothing && errors.Is(rerr, io.EOF) {
				return 0, nil, fmt.Errorf("%w: %w", ErrServerClosedIdle, rerr)
			}
			return 0, nil, rerr
		}
		firstRead = false

		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			return 0, nil, fmt.Errorf("%w: %q", ErrInvalidStatusLine, line)
		}
		minor, vok := parseHTTP1Version(parts[0])
		if !vok {
			return 0, nil, fmt.Errorf("%w: %q", ErrInvalidStatusLine, line)
		}
		respMinor = minor
		code, cok := parseStatusCode(parts[1])
		if !cok {
			return 0, nil, fmt.Errorf("%w: bad status code %q", ErrInvalidStatusLine, truncateForError(parts[1]))
		}

		// A 101 is not an interim status to drain. See ErrUnsolicitedUpgrade:
		// this client offers no Upgrade, so a 101 can only be a protocol it never
		// asked for, and continuing to read the socket for a "final" status line
		// is what let a server fabricate a response for it.
		if code == 101 {
			ex.condemn()
			return 0, nil, fmt.Errorf("%w (the client sends no Upgrade)", ErrUnsolicitedUpgrade)
		}
		// Gated on the 1xx range itself, not on "not final". This is the loop the
		// status-code grammar has to keep malformed input out of — it discards a
		// header block and reads the next line off the socket as another status
		// line — so the condition that admits a message to it is stated
		// positively. Anything outside 1xx is a final response, including the
		// unrecognised classes §15.1 says to process rather than refuse.
		if code < 100 || code > 199 {
			statusCode = code
			break
		}
		// 1xx informational: drain its headers and loop back for the real response.
		interim++
		if interim > maxInterimResponses {
			ex.condemn()
			return 0, nil, fmt.Errorf("http1: more than %d interim responses: %w",
				maxInterimResponses, ErrResponseTooLarge)
		}
		if err = ex.consumeHeaders(nil, false); err != nil {
			return 0, nil, err
		}
	}

	ex.statusCode = statusCode
	// RFC 9110 §6: HTTP/1.1 defaults to persistent, HTTP/1.0 to close. A response
	// with a higher minor version of major 1 (e.g. HTTP/1.2) is processed as the
	// highest minor this client conforms to — HTTP/1.1 — so it too defaults to
	// persistent rather than being closed as if it were unknown. Only HTTP/1.0
	// (and a version string with no 1.x minor) closes by default.
	// respMinor decides persistence, but must not RESURRECT a connection the
	// request side already condemned (a body that disagreed with its declared
	// Content-Length, say) — that write has already desynced the peer.
	ex.keepAlive = respMinor >= 1 && !ex.condemned
	// The only evidence the client has of what the peer speaks; WriteRequest
	// consults it before framing a request body as chunked (RFC 9112 §6.1).
	ex.c.peerMinor, ex.c.peerMinorKnown = respMinor, true
	ex.contentLen = contentLenUnknown

	headers = make([]header.Field, 0, 12)
	// Prepend :status for compatibility with the H2-style client layer.
	headers = append(headers, header.Field{
		Name:  statusName,
		Value: statusValue(statusCode),
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
		ex.condemn()
	}

	// RFC 9112 §6.1: a recipient of an HTTP/1.0 message carrying Transfer-Encoding
	// must treat the framing as faulty and close the connection after processing
	// the message — a 1.0 hop is not required to understand chunked, so whatever
	// re-framed the message may have got it wrong. The version seed above already
	// defaults HTTP/1.0 to close, but a "Connection: keep-alive" field line flips
	// it back and nothing re-consulted the version afterwards, so a
	// 1.0 + keep-alive + chunked response was decoded and returned to the pool.
	// The body is still chunk-decoded (chunked is self-delimiting, so the caller
	// gets its bytes); only reuse is refused.
	if ex.respTE && respMinor == 0 {
		ex.condemn()
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
	ex.checkBodylessStatusFraming()
	// HEAD is deliberately absent. Rule 1 makes a HEAD response bodyless too, but
	// RFC 9110 §9.3.2 — "The server SHOULD send the same header fields in response
	// to a HEAD request as it would have sent if the request method had been GET"
	// — makes Content-Length normal there, describing the body a GET would have
	// returned. Evicting on it would discard a pooled connection after every HEAD.
	return statusCode, headers, nil
}

// checkBodylessStatusFraming condemns the connection when a status that RFC 9112
// §6.3 rule 1 makes bodyless nevertheless declared a body.
//
// Split out of ReadResponse only because that function is at the gocyclo
// ceiling; the seam is where header parsing ends and the framing verdict begins.
func (ex *Exchange) checkBodylessStatusFraming() {
	switch ex.statusCode {
	case 204:
		// RFC 9110 §8.6: "A server MUST NOT send a Content-Length header field in
		// any response with a status code of 1xx (Informational) or 204 (No
		// Content)". RFC 9112 §6.1 says the same of Transfer-Encoding. Either
		// field on a 204 means the peer is broken or hostile, and body-shaped
		// bytes may be sitting on the socket.
		// Keyed on the VALUE, not on presence. §8.6's MUST NOT is what makes the
		// field illegal here, but the danger this branch exists for is body-shaped
		// octets left on the socket — and "Content-Length: 0" describes none.
		// Presence alone cost a connection per request against the many endpoints
		// that answer 204 with an explicit zero (generate_204 and friends), which
		// is a self-inflicted outage in exchange for no safety.
		//
		// An UNPARSEABLE Content-Length (clValue left 0) never reaches here: with
		// no Transfer-Encoding, resolveContentLength returns its clErr and
		// ReadResponse aborts before this runs; with Transfer-Encoding present the
		// `ex.respTE` term below evicts regardless. So clValue is only inspected
		// when the parse succeeded, and testing the value is sound.
		if ex.respTE || (ex.respCL && ex.clValue != 0) {
			ex.condemn()
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
			ex.condemn()
		}
	}
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

// appendASCIILower appends name to dst, folding A-Z on the way. RFC 9110 §5.1
// makes field names case-insensitive, so the wire spelling is ours to choose and
// lower-case is what this client has always emitted.
//
// ASCII-only, like asciiEqualFold and asciiLowerHeaderName and for the same
// reason: strings.ToLower is Unicode-aware, so it re-encodes bytes that are not
// valid UTF-8 rather than leaving them alone, and RFC 9112 §5.6.2 confines a
// token to ASCII anyway.
func appendASCIILower(dst, name []byte) []byte {
	for _, c := range name {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		dst = append(dst, c)
	}
	return dst
}

// isConnectionManagedName reports whether a caller-supplied field name is one
// this client manages itself: the hop-by-hop set plus Host, which WriteRequest
// writes from the authority.
//
// Switching on the length first means the common case — a name that is none of
// these — costs one integer comparison rather than seven folds.
func isConnectionManagedName(name []byte) bool {
	switch len(name) {
	case 2:
		return asciiEqualFold(name, "te")
	case 4:
		return asciiEqualFold(name, "host")
	case 7:
		return asciiEqualFold(name, "upgrade")
	case 10:
		return asciiEqualFold(name, "connection") || asciiEqualFold(name, "keep-alive")
	case 16:
		return asciiEqualFold(name, "proxy-connection")
	case 17:
		return asciiEqualFold(name, "transfer-encoding")
	}
	return false
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
		ex.condemn()
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
func lastTransferCoding(value string) (name string, wellFormed bool) {
	last := ""
	start := 0
	inQuotes := false
	for i := 0; i <= len(value); i++ {
		if i == len(value) || (value[i] == ',' && !inQuotes) {
			if n := transferCodingName(value[start:i]); n != "" {
				last = n
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
	// A quoted-string still open at end-of-value is unterminated: the field is
	// malformed (RFC 9110 §10.1.4 admits no such value), and the scan may have
	// swallowed a real final coding into the runaway quote — chunked;x=", gzip
	// otherwise resolves to "chunked" because the unclosed quote eats the comma.
	// The caller must treat !wellFormed as malformed, not as a coding verdict.
	return asciiLowerHeaderName(last), !inQuotes
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
func (ex *Exchange) consumeHeaders(out *[]header.Field, parseBody bool) error {
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
			ex.condemn()
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
				ex.condemn()
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
func (ex *Exchange) commitHeaderLine(line string, have bool, out *[]header.Field, parseBody bool) error {
	if !have {
		return nil
	}
	colon := strings.IndexByte(line, ':')
	if colon < 0 {
		return nil // skip malformed header lines
	}
	// The field name is NOT trimmed. §5.1: "No whitespace is allowed between
	// the field name and colon. In the past, differences in the handling of
	// such whitespace have led to security vulnerabilities in request routing
	// and response handling." Trimming it silently normalised
	// "Content-Length : 5" into a Content-Length this client then framed the
	// body by, while §5.1 obliges a proxy to "remove any such whitespace from
	// a response message before forwarding" — so the hop in front of us may
	// well have seen no Content-Length at all. Leaving the space in the name
	// makes validToken below reject the line, which is the outcome that cannot
	// disagree with anyone. (A LEADING space cannot reach here: consumeHeaders
	// has already routed such a line to the obs-fold branch.)
	name := asciiLowerHeaderName(line[:colon])
	// OWS is SP / HTAB (RFC 9110 §5.6.3). strings.TrimSpace is Unicode-aware
	// and also ate VT, FF and NEL, so "Content-Length: 5\v" — which §2.2
	// requires be parsed as octets, not characters — became a valid 5 here and
	// an invalid field elsewhere. Same divergence, same class of bug as the
	// name above; parseRequestContentLength already made this call on the send
	// side.
	value := strings.Trim(line[colon+1:], " \t")

	// A response field must be a token name with a value free of CR, LF and
	// NUL (RFC 9110 §5.6.2, §5.5). This mirrors what WriteRequest enforces on
	// the SEND side and what conn/http3 enforce on their receive sides — http1
	// validated outgoing request fields but handed an incoming response field
	// to the caller verbatim, so a NUL or a bare CR the server put in a value
	// reached whatever copied it into an HTTP/1.1 message, a log, or a header
	// of its own. §5.5: a recipient of such a byte "MUST either reject the
	// message or replace each of those characters with SP". We reject and
	// condemn the connection: the block is not a well-formed field sequence,
	// so the stream position cannot be trusted.
	if !validToken(name) || !validFieldValue([]byte(value)) {
		ex.condemn()
		return fmt.Errorf("http1: response header %q has an invalid name or a value "+
			"containing CR, LF or NUL: %w", truncateForError(name), ErrInvalidHeaderBlock)
	}

	// Connection is hop-by-hop and scoped to the CONNECTION, not to the message
	// carrying it (RFC 9110 §7.6.1), and "close" means the sender will close
	// after this response (RFC 9112 §9.6). So it is interpreted on EVERY head,
	// including an interim 1xx — unlike the framing fields below, which a 1xx
	// cannot carry at all (RFC 9110 §15.2) and which stay gated.
	//
	// It used to sit inside that gate, so a "close" on a 1xx was read off the
	// wire and discarded, and the socket went back to the pool. Not a desync —
	// the 1xx is fully drained — but the next request went out on a connection
	// the server had said it was closing, racing the FIN.
	if name == "connection" {
		if hasConnectionOption(value, "close") {
			ex.condemn()
		} else if hasConnectionOption(value, "keep-alive") && !ex.condemned {
			ex.keepAlive = true
		}
	}

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
			coding, wellFormed := lastTransferCoding(value)
			if !wellFormed {
				// An unterminated quoted-string makes the Transfer-Encoding
				// malformed (RFC 9110 §10.1.4), and the runaway quote may have
				// swallowed the real final coding — `chunked;x=", gzip` reads
				// as final coding "chunked" only because the unclosed quote ate
				// the comma. The framing verdict cannot be trusted, so fall to
				// §6.3 rule 4's read-until-close AND condemn the connection: the
				// body boundary is indeterminate, so the socket must not be
				// pooled for the next response to resynchronise on.
				ex.respChunked = false
				ex.contentLen = contentLenUnknown
				// condemn, not a bare clear: the "connection" case below re-sets
				// keepAlive on a keep-alive option, and header lines arrive in
				// whatever order the peer chose — so without the latch a server
				// could undo a framing condemnation just by putting
				// Connection: keep-alive after its malformed Transfer-Encoding.
				ex.condemn()
				break
			}
			if coding == "" {
				// This field line contributes no codings, so it cannot move
				// the verdict — earlier lines still decide.
				//
				// Repeated field lines are ONE list: RFC 9110 §5.3 defines
				// them by appending each value "to the initial field line
				// value in order, separated by a comma", so
				//
				//	Transfer-Encoding: chunked
				//	Transfer-Encoding:
				//
				// is the message "Transfer-Encoding: chunked, " — whose empty
				// final element §5.6.1.2 requires a recipient to ignore,
				// leaving chunked as the final coding. Letting each line
				// overwrite the verdict made the second line win and turned a
				// chunked body into read-until-close: the same message spelled
				// on one line ("chunked, ") parsed correctly, which is what
				// isolates this to the multi-line path rather than to
				// lastTransferCoding, whose own contract already skips empty
				// elements.
				break
			}
			// Either branch overrides any Content-Length parsed so far
			// (§6.3 rule 3), which is why contentLen is assigned
			// unconditionally here: this is the only place that can undo a
			// Content-Length that arrived first.
			if coding == "chunked" {
				ex.respChunked = true
				ex.contentLen = contentLenUnknown
			} else {
				// §6.3 rule 4: chunked is not the final encoding, so the
				// body length is determined by reading until the server
				// closes the connection.
				ex.respChunked = false
				ex.contentLen = contentLenUnknown
			}
		}
	}

	if out != nil {
		*out = append(*out, header.Field{
			Name:  []byte(name),
			Value: []byte(value),
		})
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
	// Whatever is still buffered when this message ends is unsolicited, so the
	// connection must not be pooled.
	//
	// This client does not pipeline (one exchange per connection at a time), so at
	// message completion there is by construction no outstanding request left for
	// those octets to belong to. RFC 9112 §6.3: "A client MUST NOT process, cache,
	// or forward such extra data as a separate response, since such behavior would
	// be vulnerable to cache poisoning." Without this the socket goes back to the
	// idle set with a peer-chosen response sitting in the reader, and request N+1
	// parses those bytes as its own status line — a response the server never sent
	// for it, returned with err == nil.
	//
	// A defer rather than a check at each site: every framing mode has its own
	// done=true return (HEAD, 204/304, Content-Length satisfied, terminal chunk
	// after its trailer section, read-until-close), and one of them missing the
	// check is exactly the bug. It is a pure memory read on the bufio reader — no
	// syscall — so it costs nothing on the hot path. Only a still-poolable
	// connection is downgraded; error paths have already cleared keepAlive.
	// Any error at all condemns the connection. Most framing-defect paths cleared
	// keepAlive individually, but the ones that did not — the Content-Length tail
	// return, the chunk-data read error, both readLine failures inside
	// readChunkedChunk — left a connection whose stream position is unknown
	// marked reusable, which is the same pooled-desync this defer's other half
	// prevents. Deciding it once here is what makes "no exit path has to
	// remember" true.
	defer func() {
		if err != nil {
			ex.condemn()
			return
		}
		if done && ex.keepAlive && ex.c.br.Buffered() > 0 {
			ex.condemn()
		}
	}()
	// Body reads honour the ReadResponse ctx: a blocking Read cannot be selected
	// on, and this exchange has no second goroutine to notice a cancellation.
	armed := ex.c.armCancel(ex.readCtx, readDeadline)
	defer ex.c.disarmWatch(armed)
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
		// bufio.Read short-circuits when its buffer is empty AND the caller's
		// slice is at least buffer-sized: it reads straight into that slice and
		// the buffer stays empty. Anything the peer appended after this response
		// then never enters the reader, so the completion guard below — which asks
		// Buffered() — sees nothing, the connection is pooled, and the next
		// request parses the peer's appended response as its own (RFC 9112 §6.3
		// MUST NOT). Note it before the read; the guard cannot infer it after.
		bypassed := len(buf) >= readBufSize && ex.c.br.Buffered() == 0
		n, err = ex.c.br.Read(buf)
		ex.bodyRead += int64(n)
		done = ex.bodyRead >= ex.contentLen
		// Only on the bypass path, and only once the body is complete: ask the
		// socket what the reader could not see. A peer that closed is already
		// reported through the io.EOF branch below (the direct read surfaces it
		// coalesced), so this is specifically about octets it appended.
		//
		// HasResidue, not ProbeIdle. This runs inside the armCancel window opened
		// at the top of ReadBodyChunk, and that watchdog releases a blocked read by
		// installing a deadline in the PAST — which ProbeIdle, whose whole method
		// is a bounded future deadline, cannot distinguish from a healthy quiet
		// socket. It answered "clean" for a cancelled read and left keepAlive true.
		// HasResidue's decisive layer is FIONREAD, which reads the kernel queue
		// whatever the deadline says, so the verdict no longer depends on winning
		// a race with the watchdog. It is also ~0.5µs rather than ~1ms.
		if bypassed && done && err == nil && ex.keepAlive && ex.c.HasResidue() {
			ex.condemn()
		}
		if err == io.EOF {
			if !done {
				// Premature EOF before Content-Length satisfied. RFC 9112 §6.3
				// rule 6 makes the message incomplete, and this connection's
				// stream position is now indeterminate, so it must not be reused —
				// every other framing-defect path in this file clears keepAlive,
				// and KeepAlive()'s documented contract says so. client/ happened
				// to be safe because h1Exchange.Close forces release(false), but
				// http1 is a public package and a direct caller trusting
				// KeepAlive() would have pooled a truncated stream.
				ex.condemn()
				return n, true, fmt.Errorf("http1: premature EOF: got %d of %d bytes", ex.bodyRead, ex.contentLen)
			}
			// Final body bytes arrived coalesced with io.EOF in a single
			// Read (bufio passes through the underlying (n, io.EOF) when
			// the caller buffer is >= bufio's buffer). The body is now
			// complete, so surface the bytes with a nil error instead of
			// discarding n. The EOF means the peer closed the socket, so the
			// connection is no longer reusable — do not let it be pooled.
			ex.condemn()
			err = nil
		}
		return n, done, err
	}

	// contentLen is contentLenUnknown and the body is not chunked: §6.3 rule 4,
	// read until the connection closes.
	n, err = ex.c.br.Read(buf)
	if err == io.EOF {
		ex.condemn()
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
		// `chunk = chunk-size [ chunk-ext ] CRLF` with `chunk-size = 1*HEXDIG`
		// (§7.1), so no whitespace may surround the size — EXCEPT the BWS that
		// §7.1.1's `chunk-ext = *( BWS ";" BWS ... )` puts before the semicolon,
		// which only exists when there IS a semicolon. Blanket-TrimSpace instead
		// accepted " 5", "5 " and "5\v" as sizes, and a chunk-size line is where
		// a framing disagreement costs the most: every byte after it is data or
		// not data depending on who parsed it.
		var (
			size int64
			ok   bool
		)
		if semi := strings.IndexByte(line, ';'); semi >= 0 {
			size, ok = parseChunkSize(strings.TrimRight(line[:semi], " \t"))
		} else {
			size, ok = parseChunkSize(line)
		}
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
			ex.condemn()
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
					ex.condemn()
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
					ex.condemn()
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

	// After exhausting a chunk, consume its trailing CRLF — and verify that is
	// all it was. §7.1: `chunk = chunk-size [ chunk-ext ] CRLF chunk-data CRLF`,
	// so the octets between the last data byte and the next chunk-size line are
	// exactly one CRLF. This used to read the line and discard it whatever it
	// held, which made the delimiter "everything up to the next LF": a server
	// could park arbitrary octets there, and a recipient that measured chunk-data
	// by chunk-size alone — as the grammar says to — would read them as the next
	// chunk-size line instead. Two framings of one body again.
	if ex.chunkRemaining == 0 {
		term, lerr := ex.readLine("chunk CRLF")
		if lerr != nil {
			return n, false, lerr
		}
		if term != "" {
			ex.condemn()
			return n, false, fmt.Errorf("http1: chunk-data not followed by CRLF, got %q: %w",
				truncateForError(term), ErrInvalidChunkSize)
		}
	}

	return n, false, nil
}

// condemn latches that this connection must not be reused. One way: nothing
// clears it for the life of the Exchange, and an Exchange is one request/response
// pair.
//
// keepAlive is cleared alongside so the two cheap guards that read it — the
// leftover-octet check and the residue probe — keep short-circuiting on a
// condemned exchange rather than paying a syscall to re-learn it.
func (ex *Exchange) condemn() {
	ex.keepAlive = false
	ex.condemned = true
}

// KeepAlive reports whether the underlying connection should be returned to
// a pool after this exchange completes. Returns false when the server sent
// "Connection: close" or used HTTP/1.0 without "Connection: keep-alive".
func (ex *Exchange) KeepAlive() bool {
	// An abandoned upload is not reusable, whether or not anyone called WriteBody
	// with fin. The under-run check inside WriteBody only fires on the final
	// chunk, so a caller that declared a Content-Length, wrote part of it, and
	// then gave up — a cancelled request, a BodyReader that errored — left this
	// reporting true with the peer still counting octets. The pool then handed
	// that socket to the next request, whose request-line the peer consumed as
	// the tail of the previous body.
	//
	// Asked here rather than latched at some earlier point because this is the
	// question's natural home: it is the caller's decision to stop that makes the
	// body short, and this is the moment the caller asks whether the connection
	// survived. reqContentLen is -1 unless a length was declared, and is 0 for a
	// bodyless head, so neither case can trip it.
	if ex.reqContentLen > 0 && ex.reqBodyWritten < ex.reqContentLen {
		return false
	}
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
