package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	//
	// It is also the per-stream memory budget, not only a limit: Dial sizes
	// conn's event channel from it (see eventBufferFor), so a stream can hold
	// roughly this much queued DATA plus the same again in the reassembly
	// buffer while the application is behind. Raise it per call with the
	// MaxRecvMessageSize CallOption rather than connection-wide.
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
	if o.Conn.StreamEventBuffer <= 0 {
		o.Conn.StreamEventBuffer = eventBufferFor(o.MaxRecvMessageSize, o.Conn.Settings.MaxFrameSize)
	}
	if o.Conn.Settings.MaxHeaderListSize == 0 {
		o.Conn.Settings.MaxHeaderListSize = DefaultMaxHeaderListSize
	}
	// gRPC has no server push, and a pushed stream this package never reads
	// would sit in conn's registry until the connection dies. Refusing it at
	// the SETTINGS level beats discarding an event we asked for.
	o.Conn.EnablePush = false
	return o
}

// DefaultMaxHeaderListSize caps the uncompressed size of a response header or
// trailer block. conn's own default is 8 MiB, which is a sane ceiling for a web
// response but two orders of magnitude past anything gRPC metadata carries —
// and this package copies each block into caller-owned memory that lives as
// long as the Stream, so what the cap admits is what a peer can pin.
const DefaultMaxHeaderListSize = 1 << 20

// eventBufferSlackBytes is the headroom added to the per-stream event budget on
// top of one maximum-size message: the HEADERS event, the trailer event, and a
// few frames of whatever follows. It is a byte figure, not a slot count —
// expressed in slots it would silently multiply by the advertised frame size,
// so a caller who raised MaxFrameSize for throughput would get hundreds of
// megabytes of headroom they never asked for.
const eventBufferSlackBytes = 256 << 10

// maxEventBufferBytes caps the DATA a single stream's event channel may hold
// while the application is behind. Past this, conn's RST_STREAM is the correct
// answer: a peer that can outrun the consumer by more than this is not going to
// be rescued by a larger queue.
const maxEventBufferBytes = 8 << 20

// minEventBuffer keeps at least conn's own default number of slots, so a caller
// who advertises an enormous MaxFrameSize is never worse off than the default.
const minEventBuffer = 8

// maxEventBuffer caps the slot count itself, independently of the byte budget,
// so a tiny advertised frame size cannot turn the budget into a huge channel.
const maxEventBuffer = 512

// eventBufferFor sizes conn's per-stream event channel from a byte budget.
//
// This is load-bearing, not a tuning knob. conn refunds flow-control window
// from the reader goroutine as each DATA frame arrives (conn.onDataReceived),
// not as the application consumes it, so HTTP/2 backpressure never throttles
// the peer to the consumer's pace. The only thing between a fast server and a
// busy client is the event channel — and when it fills, conn drops the event
// and resets its own stream with REFUSED_STREAM (conn.Stream.push). conn's
// default of 8 slots is 128 KiB at the default frame size, so an ordinary gRPC
// response of a few hundred KiB loses frames partway through and surfaces to
// the caller as a truncated message.
//
// The budget is bytes rather than frames because every queued EventData pins a
// pooled DATA buffer of up to one frame: slots x frameSize is the memory a peer
// can hold per stream, so that product is what has to be bounded.
func eventBufferFor(maxMessage int, maxFrameSize uint32) int {
	if maxFrameSize == 0 {
		maxFrameSize = 16384 // conn's advertised default
	}
	budget := maxMessage + eventBufferSlackBytes
	if budget > maxEventBufferBytes {
		budget = maxEventBufferBytes
	}
	n := budget / int(maxFrameSize)
	if n < minEventBuffer {
		return minEventBuffer
	}
	if n > maxEventBuffer {
		return maxEventBuffer
	}
	return n
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
		// The dialled address doubles as :authority. Right for a name, wrong
		// for a literal: dialling "10.0.0.7:443" sends ":authority:
		// 10.0.0.7:443", which servers that route on it will reject. Set
		// Options.Authority explicitly when dialling an IP.
		opts.Authority = addr
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
//
// Options.Conn is ignored here — c is already built. That includes the event
// buffer Dial would have sized: c must have been created with a
// ConnOptions.StreamEventBuffer large enough to hold one MaxRecvMessageSize
// worth of DATA frames, or large messages will be truncated. See
// eventBufferFor for why. Prefer Dial unless the connection has to be shared
// with something else.
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

// CallOption customises a single RPC. The interface is closed — only this
// package can implement it — so options can be added without breaking callers.
type CallOption interface {
	apply(*callOptions)
}

// callOptions is the resolved per-call configuration.
type callOptions struct {
	maxRecvMessageSize int
}

// maxRecvCallOption implements CallOption for MaxRecvMessageSize.
type maxRecvCallOption int

func (o maxRecvCallOption) apply(c *callOptions) { c.maxRecvMessageSize = int(o) }

// MaxRecvMessageSize overrides Options.MaxRecvMessageSize for one call — for
// the single method that returns a large payload, without paying its memory
// cost on every other call sharing the connection.
func MaxRecvMessageSize(n int) CallOption { return maxRecvCallOption(n) }

// NewStream opens a gRPC call on cc. method is the fully-qualified path,
// "/package.Service/Method". md carries request metadata built with
// AppendMetadata; it may be nil.
//
// The request HEADERS frame is written before NewStream returns, so a deadline
// already on ctx is propagated to the server as grpc-timeout.
//
// ctx governs this call's setup only. It is NOT retained: Send, CloseSend and
// Recv each take their own context and are cancelled by that one. Pass the same
// context throughout unless you intend the deadline the server was told and the
// deadline this client enforces to differ.
//
// The caller must Close the returned Stream unless it reads it to completion,
// and after any Recv error — an abandoned stream holds a conn.Stream slot and
// leaves the server generating a response nobody reads.
func (cc *ClientConn) NewStream(ctx context.Context, method string, md []conn.HeaderField, opts ...CallOption) (*Stream, error) {
	if !strings.HasPrefix(method, "/") {
		return nil, fmt.Errorf("%w: %q", ErrBadMethod, method)
	}
	// Caller-built md never went through AppendMetadata, so it is validated in
	// full here. Neither conn nor hpack checks field syntax on the send path,
	// which makes this the last gate before the wire.
	for i := range md {
		if err := validMetadata(md[i].Name, md[i].Value); err != nil {
			return nil, err
		}
	}
	co := callOptions{maxRecvMessageSize: cc.opts.MaxRecvMessageSize}
	for _, o := range opts {
		o.apply(&co)
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
	st.dec.max = co.maxRecvMessageSize
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
			// The remaining time, at the finest unit that fits 8 digits — so a
			// different value on essentially every RPC. Indexing it would insert
			// an entry that can never be matched again and evict one that could,
			// churning the connection's dynamic table for the life of the conn.
			// Not IndexNever: a deadline is not a credential, and §7.1.3 binds
			// intermediaries to preserve what that mode marks.
			Indexing: conn.IndexWithout,
		})
	}
	for i := range md {
		f := md[i]
		// The floor, not the ceiling: a caller who never asks for never-indexed
		// would otherwise have their credential added to the connection's HPACK
		// dynamic table, where it outlives the RPC and is emitted as a
		// one-byte index on every later call. Mirrors http3/request.go.
		if defaultSensitiveField(f.Name) {
			f.Indexing = conn.IndexNever
		}
		hdrs = append(hdrs, f)
	}
	return hdrs
}

// Invoke performs a unary RPC: one request message, one response message. It
// is the common case expressed in terms of Stream.
// A caller that needs the response headers, trailers or the full Status uses
// NewStream instead; Invoke is sugar for the case where only the message
// matters.
func (cc *ClientConn) Invoke(ctx context.Context, method string, req []byte, md []conn.HeaderField, opts ...CallOption) ([]byte, error) {
	s, err := cc.NewStream(ctx, method, md, opts...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()

	// conn.ErrStreamClosed here is the RFC 9113 §8.1 case benignHalfClose
	// describes: the server wrote a complete response and reset the stream
	// with NO_ERROR rather than wait for a request body it never read. The
	// answer is already buffered, so the receive side decides the outcome.
	//
	// Every other send failure stays fatal, because nothing is coming back to
	// read and Recv would block until ctx expires. Not because nothing was
	// sent — conn chunks a message across several DATA frames and flushes each
	// (see the sendErr comment in stream.go), so a failure partway leaves
	// frames with the peer. The distinction that matters is whether a response
	// is on its way, and only a peer that closed the stream after answering
	// gives us one.
	if err := s.Send(ctx, req); err != nil && !errors.Is(err, conn.ErrStreamClosed) {
		return nil, err
	}
	if err := s.CloseSend(ctx); err != nil {
		return nil, err
	}
	resp, err := s.Recv(ctx)
	if err != nil {
		// A bare io.EOF means the call completed with status OK but carried no
		// message. Every other Invoke failure is a *Status, so this one is too
		// rather than making the caller special-case io.EOF.
		if errors.Is(err, io.EOF) {
			return nil, &Status{Code: Internal, Message: "unary method returned no message"}
		}
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
