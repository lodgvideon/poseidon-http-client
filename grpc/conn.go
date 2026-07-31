package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// DefaultUserAgent is sent as the user-agent header when Options leaves it
// empty. gRPC servers log this verbatim.
const DefaultUserAgent = "poseidon-grpc/1.0"

// ErrBadMethod is reported when a method name is not of the form
// "/package.Service/Method".
var ErrBadMethod = errors.New("grpc: method must start with '/'")

// Options configures a ClientConn.
type Options struct {
	// Conn is passed through to the HTTP/2 layer. Set
	// Conn.KeepaliveInterval to enable gRPC-style keepalive PINGs.
	Conn conn.ConnOptions

	// Authority is the :authority pseudo-header. Dial defaults it to the
	// host:port it dialled; NewClientConn requires it.
	Authority string

	// Scheme is the :scheme pseudo-header, "https" when empty. Set it to
	// "http" for h2c (plaintext prior-knowledge) connections.
	Scheme string

	// UserAgent overrides DefaultUserAgent.
	UserAgent string

	// MaxRecvMessageSize caps the size of a single received message.
	// Zero means DefaultMaxMessageSize.
	MaxRecvMessageSize int
}

// defaulted returns a copy of o with empty fields filled in.
func (o Options) defaulted() Options {
	if o.Scheme == "" {
		o.Scheme = "https"
	}
	if o.UserAgent == "" {
		o.UserAgent = DefaultUserAgent
	}
	if o.MaxRecvMessageSize <= 0 {
		o.MaxRecvMessageSize = DefaultMaxMessageSize
	}
	return o
}

// ClientConn is one HTTP/2 connection carrying gRPC calls. It multiplexes as
// many concurrent streams as the peer's SETTINGS_MAX_CONCURRENT_STREAMS
// permits, so a single ClientConn is the normal unit of a gRPC client — there
// is no pool to configure.
//
// A ClientConn is safe for concurrent use.
type ClientConn struct {
	c    *conn.Conn
	opts Options
	// owned reports whether Close should also close the underlying conn.Conn.
	owned bool
}

// Dial establishes an HTTP/2 connection to addr and returns a ClientConn ready
// for NewStream. The returned ClientConn owns the connection: Close closes it.
func Dial(ctx context.Context, addr string, opts Options) (*ClientConn, error) {
	opts = opts.defaulted()
	if opts.Authority == "" {
		opts.Authority = authorityFromAddr(addr)
	}
	c, err := conn.Dial(ctx, addr, opts.Conn)
	if err != nil {
		return nil, err
	}
	return &ClientConn{c: c, opts: opts, owned: true}, nil
}

// NewClientConn wraps an already-established HTTP/2 connection. Options.
// Authority is required, because there is no address to derive it from. The
// returned ClientConn does not own c: Close leaves it open.
func NewClientConn(c *conn.Conn, opts Options) (*ClientConn, error) {
	if c == nil {
		return nil, errors.New("grpc: nil conn.Conn")
	}
	opts = opts.defaulted()
	if opts.Authority == "" {
		return nil, errors.New("grpc: Options.Authority is required when wrapping an existing conn.Conn")
	}
	return &ClientConn{c: c, opts: opts}, nil
}

// authorityFromAddr strips a port-less address to itself and leaves host:port
// intact, which is what :authority wants in both cases.
func authorityFromAddr(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return addr
	}
	return addr
}

// Conn returns the underlying HTTP/2 connection, for callers that need
// connection-level facilities such as Ping or Stats.
func (cc *ClientConn) Conn() *conn.Conn { return cc.c }

// Close closes the connection when this ClientConn owns it (created by Dial),
// and is a no-op otherwise. In-flight streams fail with ErrConnClosed.
func (cc *ClientConn) Close() error {
	if !cc.owned {
		return nil
	}
	return cc.c.Close()
}

// NewStream opens a gRPC call on cc. method is the fully-qualified path,
// "/package.Service/Method". md carries request metadata built with
// AppendMetadata; it may be nil.
//
// The request HEADERS frame is written before NewStream returns, so a deadline
// already on ctx is propagated to the server as grpc-timeout. ctx also governs
// the whole call: cancelling it makes in-flight Send and Recv return.
//
// The caller must Close the returned Stream unless it reads it to completion.
func (cc *ClientConn) NewStream(ctx context.Context, method string, md []conn.HeaderField) (*Stream, error) {
	if !strings.HasPrefix(method, "/") {
		return nil, fmt.Errorf("%w: %q", ErrBadMethod, method)
	}
	for i := range md {
		if err := checkMetadataKey(string(md[i].Name)); err != nil {
			return nil, err
		}
	}
	hdrs := cc.buildHeaders(ctx, method, md)

	s, err := cc.c.NewStream(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.SendHeaders(ctx, hdrs, false); err != nil {
		_ = s.Close()
		return nil, err
	}
	st := &Stream{s: s}
	st.dec.Max = cc.opts.MaxRecvMessageSize
	return st, nil
}

// buildHeaders assembles the request header block: pseudo-headers first (RFC
// 9113 §8.3 requires it), then the fixed gRPC headers, then caller metadata.
func (cc *ClientConn) buildHeaders(ctx context.Context, method string, md []conn.HeaderField) []conn.HeaderField {
	hdrs := make([]conn.HeaderField, 0, 8+len(md))
	hdrs = append(hdrs,
		conn.HeaderField{Name: []byte(":method"), Value: []byte("POST")},
		conn.HeaderField{Name: []byte(":scheme"), Value: []byte(cc.opts.Scheme)},
		conn.HeaderField{Name: []byte(":path"), Value: []byte(method)},
		conn.HeaderField{Name: []byte(":authority"), Value: []byte(cc.opts.Authority)},
		conn.HeaderField{Name: []byte("content-type"), Value: []byte("application/grpc")},
		conn.HeaderField{Name: []byte("user-agent"), Value: []byte(cc.opts.UserAgent)},
		// te: trailers is mandatory: it tells the server this client
		// understands the trailers that carry grpc-status. RFC 9113 §8.2.2
		// permits te only with this exact value.
		conn.HeaderField{Name: []byte("te"), Value: []byte("trailers")},
		conn.HeaderField{Name: []byte("grpc-accept-encoding"), Value: []byte("identity")},
	)
	if dl, ok := ctx.Deadline(); ok {
		hdrs = append(hdrs, conn.HeaderField{
			Name:  []byte("grpc-timeout"),
			Value: []byte(encodeTimeout(time.Until(dl))),
		})
	}
	return append(hdrs, md...)
}

// Invoke performs a unary RPC: one request message, one response message. It
// is the common case expressed in terms of Stream.
func (cc *ClientConn) Invoke(ctx context.Context, method string, req []byte, md []conn.HeaderField) ([]byte, error) {
	s, err := cc.NewStream(ctx, method, md)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()

	if err := s.Send(ctx, req); err != nil {
		return nil, err
	}
	if err := s.CloseSend(ctx); err != nil {
		return nil, err
	}
	resp, err := s.Recv(ctx)
	if err != nil {
		return nil, err
	}
	// Drain to the terminal event so a non-OK grpc-status that follows the
	// message is reported rather than swallowed, and so a server that sends
	// two messages to a unary method is caught instead of silently truncated.
	if _, err := s.Recv(ctx); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, &Status{Code: Internal, Message: "unary method returned more than one message"}
		}
		return nil, err
	}
	return resp, nil
}
