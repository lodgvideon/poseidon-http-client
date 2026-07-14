package http3

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/qpack"
	"github.com/lodgvideon/poseidon-http-client/quic"
)

// quicStream is the QUIC stream surface the client uses. *quic.Stream satisfies
// it; tests supply a fake. RecvState / WaitReadable / WaitSendable are the
// reader-goroutine wake vocabulary (docs/HTTP3_DESIGN.md §3.3): a Do reads its
// response via Recv, waits for more with WaitReadable, and parks on WaitSendable
// when a Send is flow-control blocked; the reader signals progress.
type quicStream interface {
	ID() uint64
	Send(data []byte, fin bool) (int, error)
	Recv() []byte
	RecvState() (finished, reset bool, code uint64)
	WaitReadable(ctx context.Context) error
	WaitSendable(ctx context.Context) error
	Finished() bool
	Reset(errCode uint64) error
	StopSending(errCode uint64) error
}

// quicConn is the QUIC connection surface the client uses. A connAdapter over
// *quic.Conn satisfies it; tests supply a fake.
type quicConn interface {
	OpenStream(ctx context.Context) (quicStream, error)
	OpenUniStream() (quicStream, error)
	AcceptUniStream() quicStream // next accepted server-initiated uni stream, or nil
	Poll(ctx context.Context) error
	CloseWithError(app bool, code uint64, reason string) error
}

// ErrGoAway is returned by Do when the server has sent GOAWAY and the new request
// would use a stream the server will not process (RFC 9114 §5.2).
var ErrGoAway = errors.New("http3: server is going away")

// ErrH3Control reports a fatal HTTP/3 connection error — a control-stream
// violation (a missing or duplicate SETTINGS, a forbidden push, an oversized
// frame, a closed critical stream) or a frame that may not appear on a request
// stream (§7.2); a CONNECTION_CLOSE with the specific HTTP/3 error code has
// already been sent.
var ErrH3Control = errors.New("http3: connection error")

// HTTP/3 error codes (RFC 9114 §8.1), carried in the QUIC application
// CONNECTION_CLOSE frame.
const (
	H3NoError              uint64 = 0x0100 // H3_NO_ERROR
	H3InternalError        uint64 = 0x0102 // H3_INTERNAL_ERROR
	H3StreamCreationError  uint64 = 0x0103 // H3_STREAM_CREATION_ERROR
	H3ClosedCriticalStream uint64 = 0x0104 // H3_CLOSED_CRITICAL_STREAM
	H3FrameUnexpected      uint64 = 0x0105 // H3_FRAME_UNEXPECTED
	H3FrameError           uint64 = 0x0106 // H3_FRAME_ERROR
	H3ExcessiveLoad        uint64 = 0x0107 // H3_EXCESSIVE_LOAD
	H3IDError              uint64 = 0x0108 // H3_ID_ERROR
	H3SettingsErrorCode    uint64 = 0x0109 // H3_SETTINGS_ERROR
	H3MissingSettings      uint64 = 0x010a // H3_MISSING_SETTINGS
	H3RequestRejected      uint64 = 0x010b // H3_REQUEST_REJECTED
	H3RequestCancelled     uint64 = 0x010c // H3_REQUEST_CANCELLED
	H3MessageError         uint64 = 0x010e // H3_MESSAGE_ERROR

	// QPACK error codes (RFC 9204 §6), carried in the same HTTP/3 CONNECTION_CLOSE.
	H3QpackDecompressionFailed uint64 = 0x0200 // QPACK_DECOMPRESSION_FAILED
)

// maxInterimResponses bounds the 1xx informational responses buffered before the
// final response (RFC 9114 §4.1), so a server streaming them endlessly — including
// as empty frames that add no bytes — cannot exhaust memory.
const maxInterimResponses = 100

// maxResponseBytes bounds a whole response the client buffers in memory: it is
// both the per-frame declared-length cap (a single HEADERS, trailer, or DATA frame
// past it is refused before its payload is buffered — RFC 9114 places no per-frame
// size limit, so the request stream otherwise had none) and the cumulative cap on
// the header, body, trailer, and 1xx payloads retained together. One limit keeps
// the two consistent: a single DATA frame up to the whole budget is accepted, but
// the retained total cannot exceed it. A var, not a const, so a test can exercise
// the limit without buffering hundreds of megabytes.
var maxResponseBytes uint64 = 1 << 27 // 128 MiB

// ErrResponseTooLarge is returned by Do when a response exceeds a client buffering
// limit — a single frame or the retained total past maxResponseBytes, or the
// interim (1xx) responses past maxInterimResponses. The request stream is aborted;
// it is not a connection error.
var ErrResponseTooLarge = errors.New("http3: response exceeds client buffering limit")

// StreamResetError reports that the server abruptly aborted the request stream
// with RESET_STREAM (RFC 9000 §3.5) before the response finished. Code is the
// HTTP/3 application error code the server signalled (RFC 9114 §8.1).
type StreamResetError struct{ Code uint64 }

// Error implements error.
func (e *StreamResetError) Error() string {
	return fmt.Sprintf("http3: server reset the request stream (error %#x)", e.Code)
}

// Retryable reports whether the reset means the request received no application
// processing and is safe to retry on a new connection — the server signalled
// H3_REQUEST_REJECTED (RFC 9114 §4.1.1).
func (e *StreamResetError) Retryable() bool { return e.Code == H3RequestRejected }

// connAdapter lets a concrete *quic.Conn satisfy quicConn — the interface methods
// return quicStream where *quic.Conn returns the concrete *quic.Stream.
type connAdapter struct{ *quic.Conn }

func (a connAdapter) OpenStream(ctx context.Context) (quicStream, error) {
	s, err := a.OpenStreamContext(ctx) // waits on stream credit; threads the request ctx (2d)
	if err != nil {
		return nil, err // avoid returning a non-nil interface wrapping a nil *Stream
	}
	return s, nil
}

func (a connAdapter) OpenUniStream() (quicStream, error) {
	s, err := a.Conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (a connAdapter) AcceptUniStream() quicStream {
	if s := a.Conn.AcceptUniStream(); s != nil {
		return s // avoid a non-nil interface wrapping a nil *Stream
	}
	return nil
}

// Client is a minimal HTTP/3 client over an established QUIC connection. It owns
// the connection's control stream; each request carries its own stack-local QPACK
// codecs (docs/HTTP3_DESIGN.md §3.5, PR 2d), so nothing request-scoped is shared. A
// dedicated reader goroutine drives the QUIC engine and services the server control
// stream for the connection's lifetime (§3.1). Do sends its own request frames and
// blocks on per-stream wakeups, so N goroutines may call Do on one Client
// concurrently — OpenStream's id/map mutation and every seal are under the QUIC
// c.mu, streams wake independently, control state is reader-owned, and QPACK is
// per-request (§3.5). Client is safe for concurrent use.
type Client struct {
	conn quicConn

	// Connection lifecycle. The reader goroutine runs Poll + serviceControl on
	// connCtx until the connection terminates; Close cancels connCtx and waits on
	// readerDone (docs/HTTP3_DESIGN.md §3.1, F6).
	connCtx    context.Context
	connCancel context.CancelFunc
	readerDone chan struct{}

	// Server control-stream state (RFC 9114 §6.2.1, §5.2), reader-owned: touched
	// only on the reader goroutine (serviceControl after each Poll), so no lock.
	pendingUni    []*uniStream // accepted server uni streams whose type isn't peeled yet
	control       quicStream   // the server control stream, once identified
	controlReader FrameReader
	qpackEnc      quicStream // the server QPACK encoder stream (RFC 9204 §4.2)
	qpackDec      quicStream // the server QPACK decoder stream (RFC 9204 §4.2)
	settingsRead  bool       // the mandatory first SETTINGS frame has been read

	// maxFieldSection and goaway are published as atomics (docs/HTTP3_DESIGN.md
	// §3.5): the reader writes them from serviceControl, a Do reads them. maxFieldSection
	// is the peer SETTINGS_MAX_FIELD_SECTION_SIZE (init ^uint64(0) = no limit, §4.2.2).
	// goaway is the largest request stream id the server will process (init ^uint64(0)
	// = "none", so stream.ID() >= goaway is false until a real GOAWAY lands, §5.2 —
	// one atomic, no separate haveGoaway bool).
	maxFieldSection atomic.Uint64
	goaway          atomic.Uint64
}

// uniStream is an accepted server unidirectional stream whose leading stream-type
// varint has not yet been fully received (it may span datagrams).
type uniStream struct {
	stream quicStream
	buf    []byte
}

// NewClient wraps an established QUIC connection: it spawns the connection's
// reader goroutine, opens the client's control stream, and sends the mandatory
// first SETTINGS frame (RFC 9114 §6.2.1). The connection's handshake must already
// have completed.
func NewClient(conn *quic.Conn, settings []Setting) (*Client, error) {
	return newClient(connAdapter{conn}, settings)
}

func newClient(conn quicConn, settings []Setting) (*Client, error) {
	c := &Client{conn: conn, readerDone: make(chan struct{})}
	c.maxFieldSection.Store(^uint64(0)) // no limit until the peer's SETTINGS arrive
	c.goaway.Store(^uint64(0))          // "none" until a real GOAWAY lands (§5.2)
	c.connCtx, c.connCancel = context.WithCancel(context.Background())

	control, err := conn.OpenUniStream()
	if err != nil {
		c.connCancel()
		close(c.readerDone) // no reader was started
		return nil, err
	}
	// Start the reader BEFORE sending SETTINGS (docs/HTTP3_DESIGN.md §5, ordering
	// fix): a flow-control-blocked SETTINGS send is unblocked by the peer's MAX_DATA
	// that the reader processes — otherwise a startup deadlock.
	go c.readLoop()
	if err := c.sendAll(c.connCtx, control, AppendClientControlStream(nil, settings), false); err != nil {
		// The reader is running; tear it down so it is not leaked.
		c.connCancel()
		_ = c.conn.CloseWithError(true, H3NoError, "")
		<-c.readerDone
		return nil, err
	}
	return c, nil
}

// readLoop is the connection's reader goroutine (docs/HTTP3_DESIGN.md §3.1): it
// owns Poll (QUIC receive, loss/PTO, idle, key-update, ACK) and the H3 control
// stream servicing for the connection's lifetime. serviceControl runs OUTSIDE
// c.mu — safe because Poll never returns holding it (the §3.2 postcondition). Any
// error is fatal to the connection.
func (c *Client) readLoop() {
	defer close(c.readerDone)
	for {
		if err := c.conn.Poll(c.connCtx); err != nil {
			c.fatal(err)
			return
		}
		if err := c.serviceControl(); err != nil {
			c.fatal(err)
			return
		}
	}
}

// fatal tears the connection down on a reader-goroutine error. The QUIC layer has
// already latched its terminal error (terminateLocked) for its own teardown paths,
// so a blocked Do has usually already woken with the meaningful error; this
// cancels connCtx (stopping the read watchdog) and closes the transport as a
// catch-all. Idempotent: CloseWithError is a no-op once the connection is closed,
// so it never clobbers a close code an earlier teardown already set.
func (c *Client) fatal(_ error) {
	c.connCancel()
	_ = c.conn.CloseWithError(true, H3InternalError, "")
}

// Close terminates the HTTP/3 connection, sending a CONNECTION_CLOSE with
// H3_NO_ERROR (RFC 9114 §8.1) so the server can release the connection
// immediately rather than waiting for its idle timeout, then stopping the reader
// goroutine. It is idempotent across sequential calls.
//
// Close may be called while a Do is in flight: CloseWithError latches the graceful
// terminal error BEFORE connCancel (docs/HTTP3_DESIGN.md §3.1, F6), so the reader's
// fatal(connCtx.Err()) finds the latch taken and the in-flight Do wakes with the
// graceful ErrConnClosed rather than context.Canceled. The reader releases c.mu
// before returning, so waiting on readerDone here never holds a lock (F6).
func (c *Client) Close() error {
	err := c.conn.CloseWithError(true, H3NoError, "") // latch + CONNECTION_CLOSE + pc.Close
	c.connCancel()                                    // wake the parked reader
	<-c.readerDone                                    // never held under a lock (F6)
	return err
}

// sendAll writes the whole of data on stream. Stream.Send consumes only a prefix
// when flow-control / congestion / pacing blocked, so this advances past each
// partial send until every byte — and the FIN, which rides the final byte — is on
// the wire. When a Send makes no progress (returns zero), it parks on WaitSendable
// until the stream may admit more: a MAX_STREAM_DATA / MAX_DATA frame, the cwnd
// broadcast, or the pacing timer, all delivered by the reader (docs/HTTP3_DESIGN.md
// §3.3). A partial (non-zero) send is retried immediately.
func (c *Client) sendAll(ctx context.Context, stream quicStream, data []byte, fin bool) error {
	sent := 0
	for {
		n, err := stream.Send(data[sent:], fin)
		if err != nil {
			return err
		}
		sent += n
		if sent >= len(data) {
			return nil
		}
		if n == 0 {
			if err := stream.WaitSendable(ctx); err != nil {
				return err
			}
		}
	}
}

// Do sends req on a new request stream and reads the response, driving the QUIC
// connection's receive loop until the response stream finishes. It returns the
// response head and the fully-buffered body. The request carries no body: the
// HEADERS frame is sent with FIN.
//
// ctx bounds the whole exchange: a cancel or deadline unblocks the receive loop
// and Do returns ctx.Err(). On any error (a context cancel, or a malformed
// response) the request stream is aborted with STOP_SENDING and RESET_STREAM so
// the server frees it and stops sending (RFC 9000 §3.5, RFC 9114 §4.1).
func (c *Client) Do(ctx context.Context, req *Request) (*Response, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	// OpenStream blocks on stream credit (2d): when at the peer's cumulative
	// initial_max_streams_bidi limit it parks until a MAX_STREAMS grant, ctx fires,
	// or the connection terminates — so a wave of concurrent Do past the limit waits
	// for the peer to raise it instead of racing an immediate error.
	stream, err := c.conn.OpenStream(ctx)
	if err != nil {
		return nil, nil, err
	}
	// GOAWAY gate (RFC 9114 §5.2): the server will not process a request on a stream
	// id at or above the GOAWAY id it published. goaway is ^uint64(0) until a real
	// GOAWAY lands, so this never trips on a healthy connection. The reader publishes
	// goaway from serviceControl; Do reads it (§3.5). Reclaim the just-opened stream
	// with STOP_SENDING + RESET_STREAM so maybeRetire drops its c.streams entry — the
	// post-open abandon path that closes the 2d TOCTOU leak (F4) rather than leaving
	// a dead stream in the routing map.
	if stream.ID() >= c.goaway.Load() {
		_ = stream.StopSending(H3RequestCancelled)
		_ = stream.Reset(H3RequestCancelled)
		return nil, nil, ErrGoAway // the caller should retry on a new connection
	}
	resp, body, err := c.roundTrip(ctx, stream, req)
	if err != nil {
		// Best-effort abort of the abandoned exchange. A malformed response is a
		// stream error of type H3_MESSAGE_ERROR (RFC 9114 §4.1.2); a response that
		// exceeds a buffering cap is signalled H3_EXCESSIVE_LOAD (§8.1); anything else
		// (a context cancel, a decode abort) is signalled H3_REQUEST_CANCELLED.
		code := H3RequestCancelled
		switch {
		case errors.Is(err, ErrH3Message):
			code = H3MessageError
		case errors.Is(err, ErrResponseTooLarge):
			code = H3ExcessiveLoad
		}
		_ = stream.StopSending(code)
		_ = stream.Reset(code)
	}
	return resp, body, err
}

// roundTrip sends the request on stream and reads the response. Its caller aborts
// the stream on a non-nil error.
// sendRequest writes the request's HEADERS — ending the stream immediately when
// there is no body — and then the body in a DATA frame carrying the FIN (RFC 9114
// §4.1). If the server aborts reading the request with STOP_SENDING (surfaced as
// ErrStreamReset), the send stops but is not fatal: the caller still reads the
// response on the stream's independent receive side (§4.1). Any other send error is
// returned.
func (c *Client) sendRequest(ctx context.Context, stream quicStream, req *Request, frame []byte) error {
	hasBody := len(req.Body) > 0
	if err := c.sendAll(ctx, stream, frame, !hasBody); err != nil {
		if !errors.Is(err, quic.ErrStreamReset) {
			return err
		}
		return nil // send aborted by STOP_SENDING; still read the response
	}
	if hasBody {
		if err := c.sendAll(ctx, stream, AppendData(nil, req.Body), true); err != nil && !errors.Is(err, quic.ErrStreamReset) {
			return err
		}
	}
	return nil
}

func (c *Client) roundTrip(ctx context.Context, stream quicStream, req *Request) (*Response, []byte, error) {
	// Per-request QPACK codecs (docs/HTTP3_DESIGN.md §5, PR 2d): stack-local, so N
	// concurrent Do never share them. The encoder is an empty struct (trivially
	// safe); the decoder holds Huffman scratch buffers that the slices it emits
	// alias, so a shared decoder would let one Do's decoded headers be overwritten
	// by another — per-request is mandatory, not an optimization. Static-only QPACK
	// keeps each codec stateless per request, so nothing is lost but scratch reuse.
	var enc qpack.Encoder
	var dec qpack.Decoder
	frame, err := req.EncodeHeaders(&enc, nil, c.maxFieldSection.Load())
	if err != nil {
		return nil, nil, err
	}
	if err := c.sendRequest(ctx, stream, req, frame); err != nil {
		return nil, nil, err
	}

	var fr FrameReader
	fr.SetMaxFrameLen(maxResponseBytes) // refuse a frame larger than the whole budget before buffering it
	rb := respBuilder{dec: &dec}
	for {
		if data := stream.Recv(); len(data) > 0 {
			fr.Feed(data)
		}
		if err := c.consumeFrames(&fr, &rb); err != nil {
			return nil, nil, err
		}
		// One locked snapshot of the receive side (docs/HTTP3_DESIGN.md §3.4, F5).
		finished, reset, code := stream.RecvState()
		if !finished {
			// Park until the reader signals this stream has more response data, the
			// per-request context is cancelled, or the connection terminates (§3.3).
			// Level-triggered: the next iteration re-reads the predicate under c.mu.
			if err := stream.WaitReadable(ctx); err != nil {
				return nil, nil, err
			}
			continue
		}
		// The stream is finished. The reader is asynchronous, so finished can flip
		// between the Recv at the top of this iteration and now; drain once more to
		// feed any bytes delivered in that window before concluding — since finished
		// means the FIN is in, no further bytes can arrive after this drain.
		if data := stream.Recv(); len(data) > 0 {
			fr.Feed(data)
			continue // re-parse the newly drained bytes before concluding
		}
		if reset {
			// The server aborted with RESET_STREAM (RFC 9000 §3.5); surface it so the
			// caller can tell a rejected (retryable) request from a completed one (§4.1.1).
			return nil, nil, &StreamResetError{Code: code}
		}
		if fr.Buffered() > 0 {
			// The stream ended cleanly mid-frame, truncating the last frame (§7.1).
			return nil, nil, c.connError(H3FrameError)
		}
		break
	}
	return finalizeResponse(rb.resp, rb.body, req, rb.interim)
}

// respBuilder accumulates a decoded HTTP/3 response as request-stream frames are
// parsed across successive FrameReader feeds (see roundTrip / consumeFrames).
type respBuilder struct {
	dec          *qpack.Decoder // per-request QPACK decoder (2d): its Huffman scratch is aliased by decoded slices, so it MUST NOT be shared across concurrent Do
	resp         *Response
	interim      []*Response // informational 1xx responses, in receive order
	body         []byte
	total        uint64 // header, body, trailer, and 1xx payload bytes retained so far
	trailersSeen bool
}

// consumeFrames reads and dispatches every complete frame currently buffered in
// fr, accumulating into rb. It returns nil when the reader needs more stream
// bytes (ErrNeedMore) or after the buffer drains, and a non-nil error — already
// scoped to the right connection/stream level — on any protocol violation or an
// oversized frame (mapped to ErrResponseTooLarge).
func (c *Client) consumeFrames(fr *FrameReader, rb *respBuilder) error {
	for {
		typ, payload, rerr := fr.ReadFrame()
		if errors.Is(rerr, ErrNeedMore) {
			return nil // wait for more stream bytes
		}
		if rerr != nil {
			// An oversized frame (ErrH3FrameTooLarge) — abort rather than buffer it.
			return ErrResponseTooLarge
		}
		if err := c.dispatchFrame(rb, typ, payload); err != nil {
			return err
		}
	}
}

// dispatchFrame folds one request-stream frame into rb, enforcing HTTP/3 message
// order and the response-size caps (RFC 9114 §4.1). It returns a scoped error for
// an invalid frame sequence (connection error), an oversized response
// (ErrResponseTooLarge), or a header decode failure; nil otherwise.
func (c *Client) dispatchFrame(rb *respBuilder, typ uint64, payload []byte) error {
	switch typ {
	case FrameHeaders:
		// Message order (RFC 9114 §4.1): (1xx HEADERS)* final HEADERS DATA*
		// trailer-HEADERS?. A HEADERS after the trailers is an invalid frame
		// sequence — a connection error, not a stream error.
		if rb.trailersSeen {
			return c.connError(H3FrameUnexpected)
		}
		rb.total += uint64(len(payload))
		if rb.total > maxResponseBytes {
			return ErrResponseTooLarge // retained header/body/trailer bytes over the cap
		}
		if rb.resp == nil {
			r, derr := DecodeResponseHeaders(rb.dec, payload)
			if derr != nil {
				_, _, e := c.decodeError(derr)
				return e
			}
			if r.Status < 200 {
				if len(rb.interim) >= maxInterimResponses {
					return ErrResponseTooLarge // a 1xx flood (RFC 9114 §4.1)
				}
				rb.interim = append(rb.interim, r) // informational 1xx; keep reading
			} else {
				rb.resp = r
			}
		} else {
			tr, derr := DecodeTrailers(rb.dec, payload)
			if derr != nil {
				_, _, e := c.decodeError(derr)
				return e
			}
			rb.resp.Trailers = tr
			rb.trailersSeen = true
		}
	case FrameData:
		if rb.resp == nil || rb.trailersSeen {
			// DATA before the final response, after a 1xx (which has no body), or
			// after the trailers is an invalid frame sequence (RFC 9114 §4.1).
			return c.connError(H3FrameUnexpected)
		}
		rb.total += uint64(len(payload))
		if rb.total > maxResponseBytes {
			return ErrResponseTooLarge // retained header/body/trailer bytes over the cap
		}
		rb.body = append(rb.body, payload...)
	case FrameSettings, FrameCancelPush, FrameGoaway, FrameMaxPushID, 0x02, 0x06, 0x08, 0x09:
		// Control-stream-only and reserved HTTP/2-carryover frame types MUST NOT
		// appear on a request stream (SETTINGS §7.2.4, CANCEL_PUSH §7.2.3, GOAWAY
		// §7.2.6, MAX_PUSH_ID §7.2.7, reserved §7.2.8): a connection error.
		return c.connError(H3FrameUnexpected)
	case FramePushPromise:
		// Server push is disabled — the client never sends MAX_PUSH_ID — so a
		// PUSH_PROMISE is invalid (RFC 9114 §4.6, §7.2.5): a connection error.
		return c.connError(H3IDError)
	default:
		// GREASE (0x1f·N+0x21) and other genuinely-unknown frame types on a request
		// stream MUST be ignored (§9, §7.2.8).
	}
	return nil
}

// finalizeResponse validates a fully received response and returns it, or
// ErrH3Message if it is malformed: there was no final (non-1xx) response, or a
// response that can carry content declared a Content-Length that does not equal
// the sum of the DATA frame payloads received (RFC 9114 §4.1.2). Responses that
// never have content (204, 304, a HEAD response) may carry an anticipatory
// Content-Length that does not match the absent body.
func finalizeResponse(resp *Response, body []byte, req *Request, interim []*Response) (*Response, []byte, error) {
	if resp == nil {
		return nil, nil, ErrH3Message
	}
	if canHaveContent(resp.Status, req.Method) {
		if n, present, valid := responseContentLength(resp.Headers); present && (!valid || n != int64(len(body))) {
			return nil, nil, ErrH3Message
		}
	}
	resp.Interim = interim
	return resp, body, nil
}

// decodeError maps a header field-section decode failure to the right scope: a
// QPACK decompression failure is a connection error (RFC 9204 §2.2, §6 —
// QPACK_DECOMPRESSION_FAILED), so it closes the connection, while a message-rule
// violation (ErrH3Message) stays a stream error the caller aborts the request
// with.
func (c *Client) decodeError(err error) (*Response, []byte, error) {
	if errors.Is(err, qpack.ErrDecompressionFailed) {
		return nil, nil, c.connError(H3QpackDecompressionFailed)
	}
	return nil, nil, err
}
