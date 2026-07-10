package http3

import (
	"context"
	"errors"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/qpack"
	"github.com/lodgvideon/poseidon-http-client/quic"
)

// quicStream is the QUIC stream surface the client uses. *quic.Stream satisfies
// it; tests supply a fake.
type quicStream interface {
	ID() uint64
	Send(data []byte, fin bool) (int, error)
	Recv() []byte
	Finished() bool
	ResetReceived() bool
	ResetCode() uint64
	Reset(errCode uint64) error
	StopSending(errCode uint64) error
}

// quicConn is the QUIC connection surface the client uses. A connAdapter over
// *quic.Conn satisfies it; tests supply a fake.
type quicConn interface {
	OpenStream() (quicStream, error)
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

func (a connAdapter) OpenStream() (quicStream, error) {
	s, err := a.Conn.OpenStream()
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
// the connection's control stream and QPACK codecs. Not safe for concurrent use.
type Client struct {
	conn quicConn
	enc  qpack.Encoder
	dec  qpack.Decoder

	// Server control-stream state (RFC 9114 §6.2.1, §5.2), serviced after every
	// poll. Single-goroutine; no locking.
	pendingUni      []*uniStream // accepted server uni streams whose type isn't peeled yet
	control         quicStream   // the server control stream, once identified
	controlReader   FrameReader
	qpackEnc        quicStream // the server QPACK encoder stream (RFC 9204 §4.2)
	qpackDec        quicStream // the server QPACK decoder stream (RFC 9204 §4.2)
	settingsRead    bool       // the mandatory first SETTINGS frame has been read
	goawayID        uint64     // the largest request stream id the server will process
	haveGoaway      bool
	maxFieldSection uint64 // peer SETTINGS_MAX_FIELD_SECTION_SIZE; max uint64 until known (§4.2.2)
}

// uniStream is an accepted server unidirectional stream whose leading stream-type
// varint has not yet been fully received (it may span datagrams).
type uniStream struct {
	stream quicStream
	buf    []byte
}

// NewClient wraps an established QUIC connection: it opens the client's control
// stream and sends the mandatory first SETTINGS frame (RFC 9114 §6.2.1). The
// connection's handshake must already have completed.
func NewClient(conn *quic.Conn, settings []Setting) (*Client, error) {
	return newClient(context.Background(), connAdapter{conn}, settings)
}

func newClient(ctx context.Context, conn quicConn, settings []Setting) (*Client, error) {
	c := &Client{conn: conn, maxFieldSection: ^uint64(0)} // no limit until the peer's SETTINGS arrive
	control, err := conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	if err := c.sendAll(ctx, control, AppendClientControlStream(nil, settings), false); err != nil {
		return nil, err
	}
	return c, nil
}

// Close terminates the HTTP/3 connection, sending a CONNECTION_CLOSE with
// H3_NO_ERROR (RFC 9114 §8.1) so the server can release the connection
// immediately rather than waiting for its idle timeout, then closing the
// transport. It is idempotent.
func (c *Client) Close() error {
	return c.conn.CloseWithError(true, H3NoError, "")
}

// sendAll writes the whole of data on stream. Stream.Send consumes only a
// prefix when flow-control-blocked, so this advances past each partial send and
// polls to recover credit (a MAX_STREAM_DATA / MAX_DATA frame) until every byte
// — and the FIN, which rides the final byte — is on the wire.
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
		if err := c.poll(ctx); err != nil {
			return err
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
	// Process any control-stream data buffered by earlier polls before deciding
	// whether the server is going away (RFC 9114 §5.2).
	if err := c.serviceControl(); err != nil {
		return nil, nil, err
	}
	stream, err := c.conn.OpenStream()
	if err != nil {
		return nil, nil, err
	}
	if c.haveGoaway && stream.ID() >= c.goawayID {
		// The server will not process a request on this stream (§5.2); the caller
		// should retry on a new connection.
		return nil, nil, ErrGoAway
	}
	resp, body, err := c.roundTrip(ctx, stream, req)
	if err != nil {
		// Best-effort abort of the abandoned exchange. A malformed response is a
		// stream error of type H3_MESSAGE_ERROR (RFC 9114 §4.1.2); anything else
		// (a context cancel, a decode abort) is signalled H3_REQUEST_CANCELLED.
		code := H3RequestCancelled
		if errors.Is(err, ErrH3Message) {
			code = H3MessageError
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
	frame, err := req.EncodeHeaders(&c.enc, nil, c.maxFieldSection)
	if err != nil {
		return nil, nil, err
	}
	if err := c.sendRequest(ctx, stream, req, frame); err != nil {
		return nil, nil, err
	}

	var fr FrameReader
	var resp *Response
	var interim []*Response
	var body []byte
	var trailersSeen bool
	for {
		if data := stream.Recv(); len(data) > 0 {
			fr.Feed(data)
		}
		for {
			typ, payload, rerr := fr.ReadFrame()
			if rerr != nil {
				break // ErrNeedMore: wait for more stream bytes
			}
			switch typ {
			case FrameHeaders:
				// Message order (RFC 9114 §4.1): (1xx HEADERS)* final HEADERS
				// DATA* trailer-HEADERS?. A HEADERS after the trailers is an invalid
				// frame sequence — a connection error, not a stream error.
				if trailersSeen {
					return nil, nil, c.connError(H3FrameUnexpected)
				}
				if resp == nil {
					r, derr := DecodeResponseHeaders(&c.dec, payload)
					if derr != nil {
						return c.decodeError(derr)
					}
					if r.Status < 200 {
						interim = append(interim, r) // informational 1xx; keep reading
					} else {
						resp = r
					}
				} else {
					tr, derr := DecodeTrailers(&c.dec, payload)
					if derr != nil {
						return c.decodeError(derr)
					}
					resp.Trailers = tr
					trailersSeen = true
				}
			case FrameData:
				if resp == nil || trailersSeen {
					// DATA before the final response, after a 1xx (which has no body),
					// or after the trailers is an invalid frame sequence (RFC 9114
					// §4.1) — a connection error, not a stream error.
					return nil, nil, c.connError(H3FrameUnexpected)
				}
				body = append(body, payload...)
			case FrameSettings, FrameCancelPush, FrameGoaway, FrameMaxPushID, 0x02, 0x06, 0x08, 0x09:
				// Control-stream-only and reserved HTTP/2-carryover frame types
				// MUST NOT appear on a request stream (SETTINGS §7.2.4, CANCEL_PUSH
				// §7.2.3, GOAWAY §7.2.6, MAX_PUSH_ID §7.2.7, reserved §7.2.8): a
				// connection error, so close the connection rather than the stream.
				return nil, nil, c.connError(H3FrameUnexpected)
			case FramePushPromise:
				// Server push is disabled — the client never sends MAX_PUSH_ID — so
				// a PUSH_PROMISE is invalid (RFC 9114 §4.6, §7.2.5): a connection error.
				return nil, nil, c.connError(H3IDError)
			default:
				// GREASE (0x1f·N+0x21) and other genuinely-unknown frame types on a
				// request stream MUST be ignored (§9, §7.2.8).
			}
		}
		if stream.Finished() {
			if stream.ResetReceived() {
				// The server aborted the response with RESET_STREAM (RFC 9000 §3.5).
				// Surface the application error code so the caller can distinguish a
				// rejected (retryable) request from a completed one (RFC 9114 §4.1.1),
				// rather than returning a truncated body as a success. A reset is an
				// abrupt end, so its mid-frame position is not an §7.1 frame error.
				return nil, nil, &StreamResetError{Code: stream.ResetCode()}
			}
			if fr.Buffered() > 0 {
				// The stream ended cleanly in the middle of a frame — the last frame
				// was truncated by the clean end of the stream (RFC 9114 §7.1): a
				// connection error H3_FRAME_ERROR.
				return nil, nil, c.connError(H3FrameError)
			}
			break
		}
		if err := c.poll(ctx); err != nil {
			return nil, nil, err
		}
	}
	return finalizeResponse(resp, body, req, interim)
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
