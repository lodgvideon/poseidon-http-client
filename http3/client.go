package http3

import (
	"github.com/lodgvideon/poseidon-http-client/qpack"
	"github.com/lodgvideon/poseidon-http-client/quic"
)

// quicStream is the QUIC stream surface the client uses. *quic.Stream satisfies
// it; tests supply a fake.
type quicStream interface {
	Send(data []byte, fin bool) (int, error)
	Recv() []byte
	Finished() bool
}

// quicConn is the QUIC connection surface the client uses. A connAdapter over
// *quic.Conn satisfies it; tests supply a fake.
type quicConn interface {
	OpenStream() (quicStream, error)
	OpenUniStream() (quicStream, error)
	Poll() error
	CloseWithError(app bool, code uint64, reason string) error
}

// HTTP/3 error codes (RFC 9114 §8.1), carried in the QUIC application
// CONNECTION_CLOSE frame.
const (
	H3NoError       uint64 = 0x0100 // H3_NO_ERROR
	H3InternalError uint64 = 0x0102 // H3_INTERNAL_ERROR
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

// Client is a minimal HTTP/3 client over an established QUIC connection. It owns
// the connection's control stream and QPACK codecs. Not safe for concurrent use.
type Client struct {
	conn quicConn
	enc  qpack.Encoder
	dec  qpack.Decoder
}

// NewClient wraps an established QUIC connection: it opens the client's control
// stream and sends the mandatory first SETTINGS frame (RFC 9114 §6.2.1). The
// connection's handshake must already have completed.
func NewClient(conn *quic.Conn, settings []Setting) (*Client, error) {
	return newClient(connAdapter{conn}, settings)
}

func newClient(conn quicConn, settings []Setting) (*Client, error) {
	control, err := conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	if err := sendAll(conn, control, AppendClientControlStream(nil, settings), false); err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
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
func sendAll(conn quicConn, stream quicStream, data []byte, fin bool) error {
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
		if err := conn.Poll(); err != nil {
			return err
		}
	}
}

// Do sends req on a new request stream and reads the response, driving the QUIC
// connection's receive loop until the response stream finishes. It returns the
// response head and the fully-buffered body. The request carries no body: the
// HEADERS frame is sent with FIN.
func (c *Client) Do(req *Request) (*Response, []byte, error) {
	stream, err := c.conn.OpenStream()
	if err != nil {
		return nil, nil, err
	}
	frame, err := req.EncodeHeaders(&c.enc, nil)
	if err != nil {
		return nil, nil, err
	}
	// Send HEADERS, ending the stream now only if there is no body; otherwise
	// the body follows in a DATA frame that carries the FIN (RFC 9114 §4.1).
	hasBody := len(req.Body) > 0
	if err := sendAll(c.conn, stream, frame, !hasBody); err != nil {
		return nil, nil, err
	}
	if hasBody {
		if err := sendAll(c.conn, stream, AppendData(nil, req.Body), true); err != nil {
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
		if err := c.conn.Poll(); err != nil {
			return nil, nil, err
		}
	}
	if resp == nil {
		return nil, nil, ErrH3Message // no final (non-1xx) response
	}
	resp.Interim = interim
	return resp, body, nil
}
