package client

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ClientOptions tunes a Client. Addr and ConnOpts.Dialer are required.
type ClientOptions struct {
	// Addr is the "host:port" target used both as the dial target and
	// as the default :authority for requests that don't set one.
	Addr string

	// ConnOpts is forwarded verbatim to conn.Dial. ConnOpts.Dialer
	// must be non-nil.
	ConnOpts conn.ConnOptions

	// DialBackoff suppresses repeated dial attempts within this window
	// after a failed dial. Zero disables suppression (immediate retry).
	DialBackoff time.Duration
}

// Client is a high-level HTTP/2 client wrapping a single connection.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	tr        transport
	authority string
}

// NewClient validates opts and constructs a Client. It does NOT dial;
// the first Do or DoStream call triggers a lazy connection establish.
func NewClient(opts ClientOptions) (*Client, error) {
	if strings.TrimSpace(opts.Addr) == "" {
		return nil, fmt.Errorf("client: ClientOptions.Addr is required")
	}
	if opts.ConnOpts.Dialer == nil {
		return nil, fmt.Errorf("client: ClientOptions.ConnOpts.Dialer is required")
	}
	tr := &singleConn{
		addr:     opts.Addr,
		connOpts: opts.ConnOpts,
		backoff:  opts.DialBackoff,
	}
	return &Client{tr: tr, authority: deriveAuthority(opts.Addr)}, nil
}

// Close releases the underlying transport. Subsequent Do/DoStream
// calls return ErrClosed. Idempotent.
func (c *Client) Close() error {
	return c.tr.close()
}

// Do issues a synchronous request and returns a fully-buffered Response.
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	cn, release, err := c.tr.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	s, err := cn.NewStream(ctx)
	if err != nil {
		return nil, err
	}

	hdrs := buildHeaders(req, c.authority)
	endStream := len(req.Body) == 0 && req.BodyReader == nil
	if err := s.SendHeaders(ctx, hdrs, endStream); err != nil {
		_ = s.Close()
		return nil, err
	}

	if !endStream {
		if err := writeRequestBody(ctx, s, req); err != nil {
			_ = s.Close()
			return nil, err
		}
	}

	return drainResponse(ctx, s, req)
}

// buildHeaders assembles the on-wire HEADERS slice with pseudo-headers
// first. The returned slice is a fresh allocation; caller-supplied
// req.Headers entries are referenced by value.
func buildHeaders(req *Request, defaultAuthority string) []hpack.HeaderField {
	scheme := req.Scheme
	if scheme == "" {
		scheme = "https"
	}
	authority := req.Authority
	if authority == "" {
		authority = defaultAuthority
	}
	out := make([]hpack.HeaderField, 0, 4+len(req.Headers))
	out = append(out,
		hpack.HeaderField{Name: []byte(":method"), Value: []byte(req.Method)},
		hpack.HeaderField{Name: []byte(":scheme"), Value: []byte(scheme)},
		hpack.HeaderField{Name: []byte(":authority"), Value: []byte(authority)},
		hpack.HeaderField{Name: []byte(":path"), Value: []byte(req.Path)},
	)
	out = append(out, req.Headers...)
	return out
}

// writeRequestBody writes Body or BodyReader as DATA frames, ending
// the request side on the final write. The caller has already issued
// SendHeaders with endStream=false.
func writeRequestBody(ctx context.Context, s *conn.Stream, req *Request) error {
	if req.BodyReader != nil {
		return writeBodyReader(ctx, s, req.BodyReader)
	}
	return s.SendData(ctx, req.Body, true)
}

// readChunkSize is the per-Read buffer for streaming uploads. The
// underlying conn layer further chunks at the peer's MAX_FRAME_SIZE
// and respects flow control.
const readChunkSize = 16 * 1024

// writeBodyReader streams an io.Reader into DATA frames, half-closing
// the stream at EOF. On read error it sends RST_STREAM(CANCEL) via
// Stream.Close and wraps the error.
func writeBodyReader(ctx context.Context, s *conn.Stream, r io.Reader) error {
	buf := make([]byte, readChunkSize)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			final := rerr == io.EOF
			if werr := s.SendData(ctx, buf[:n], final); werr != nil {
				return werr
			}
			if final {
				return nil
			}
		}
		if rerr == io.EOF {
			return s.SendData(ctx, nil, true)
		}
		if rerr != nil {
			return fmt.Errorf("client: read request body: %w", rerr)
		}
	}
}

// drainResponse pumps stream events until the response side ends or
// the stream resets.
func drainResponse(ctx context.Context, s *conn.Stream, req *Request) (*Response, error) {
	var (
		gotHeaders bool
		resp       Response
	)
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			return nil, err
		}
		switch ev.Type {
		case conn.EventHeaders:
			if !gotHeaders {
				// Copy first to defend against the conn-package
				// scratch buffer being overwritten by the reader
				// goroutine processing the next frame.
				copied := copyHeaderFields(ev.Headers)
				status, regular, perr := parseStatus(copied)
				if perr != nil {
					return nil, perr
				}
				resp.Status = status
				resp.Headers = regular
				gotHeaders = true
			} else if req.WantTrailers {
				// The conn package emits trailers as a second
				// EventHeaders rather than EventTrailers; recognize
				// it here.
				resp.Trailers = copyHeaderFields(ev.Headers)
			}
			if ev.EndStream {
				return &resp, nil
			}
		case conn.EventData:
			resp.BytesReceived += int64(len(ev.Data))
			if req.WantBody && len(ev.Data) > 0 {
				resp.Body = append(resp.Body, ev.Data...)
			}
			if ev.EndStream {
				return &resp, nil
			}
		case conn.EventTrailers:
			if req.WantTrailers {
				resp.Trailers = copyHeaderFields(ev.Headers)
			}
			if ev.EndStream {
				return &resp, nil
			}
		case conn.EventReset:
			return nil, &StreamResetError{Code: ev.RSTCode}
		}
	}
}

// copyHeaderFields deep-copies a header slice (including its byte
// slices) so the result is safe to retain across Recv / Close.
func copyHeaderFields(in []hpack.HeaderField) []hpack.HeaderField {
	if len(in) == 0 {
		return nil
	}
	out := make([]hpack.HeaderField, len(in))
	for i := range in {
		nm := make([]byte, len(in[i].Name))
		copy(nm, in[i].Name)
		vl := make([]byte, len(in[i].Value))
		copy(vl, in[i].Value)
		out[i] = hpack.HeaderField{Name: nm, Value: vl}
	}
	return out
}

// deriveAuthority strips the port if it equals 80 (http) or 443 (https).
func deriveAuthority(addr string) string {
	host, port, ok := strings.Cut(addr, ":")
	if !ok {
		return addr
	}
	if port == "80" || port == "443" {
		return host
	}
	return addr
}
