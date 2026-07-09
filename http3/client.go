package http3

import (
	"context"
	"errors"

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

// ErrH3Control reports a fatal error on the server control stream (a missing or
// duplicate SETTINGS, a forbidden push, or an oversized frame); a CONNECTION_CLOSE
// with the specific HTTP/3 error code has already been sent.
var ErrH3Control = errors.New("http3: control stream error")

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
	H3RequestCancelled     uint64 = 0x010c // H3_REQUEST_CANCELLED
)

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
	pendingUni    []*uniStream // accepted server uni streams whose type isn't peeled yet
	control       quicStream   // the server control stream, once identified
	controlReader FrameReader
	settingsRead  bool   // the mandatory first SETTINGS frame has been read
	goawayID      uint64 // the largest request stream id the server will process
	haveGoaway    bool
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
	c := &Client{conn: conn}
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
		// Best-effort abort of the abandoned exchange.
		_ = stream.StopSending(H3RequestCancelled)
		_ = stream.Reset(H3RequestCancelled)
	}
	return resp, body, err
}

// roundTrip sends the request on stream and reads the response. Its caller aborts
// the stream on a non-nil error.
func (c *Client) roundTrip(ctx context.Context, stream quicStream, req *Request) (*Response, []byte, error) {
	frame, err := req.EncodeHeaders(&c.enc, nil)
	if err != nil {
		return nil, nil, err
	}
	// Send HEADERS, ending the stream now only if there is no body; otherwise
	// the body follows in a DATA frame that carries the FIN (RFC 9114 §4.1).
	hasBody := len(req.Body) > 0
	if err := c.sendAll(ctx, stream, frame, !hasBody); err != nil {
		return nil, nil, err
	}
	if hasBody {
		if err := c.sendAll(ctx, stream, AppendData(nil, req.Body), true); err != nil {
			return nil, nil, err
		}
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
				// DATA* trailer-HEADERS?. Nothing may follow the trailers.
				if trailersSeen {
					return nil, nil, ErrH3FrameUnexpected
				}
				if resp == nil {
					r, derr := DecodeResponseHeaders(&c.dec, payload)
					if derr != nil {
						return nil, nil, derr
					}
					if r.Status < 200 {
						interim = append(interim, r) // informational 1xx; keep reading
					} else {
						resp = r
					}
				} else {
					tr, derr := DecodeTrailers(&c.dec, payload)
					if derr != nil {
						return nil, nil, derr
					}
					resp.Trailers = tr
					trailersSeen = true
				}
			case FrameData:
				if resp == nil || trailersSeen {
					// DATA before the final response, after a 1xx (which has no
					// body), or after the trailers (§4.1).
					return nil, nil, ErrH3FrameUnexpected
				}
				body = append(body, payload...)
			}
			// Illegal frame types on a request stream (SETTINGS, etc., §7.2.8)
			// are validated in a later phase.
		}
		if stream.Finished() {
			break
		}
		if err := c.poll(ctx); err != nil {
			return nil, nil, err
		}
	}
	if resp == nil {
		return nil, nil, ErrH3Message // no final (non-1xx) response
	}
	resp.Interim = interim
	return resp, body, nil
}
