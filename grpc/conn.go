package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
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

	// AllowReservedMetadata exempts specific metadata names from the rule that
	// the whole "grpc-" namespace belongs to the protocol. Names are matched
	// case-insensitively.
	//
	// The default refuses the prefix outright, because the specification reserves
	// it for FUTURE protocol use, not only for the names in use today — so a
	// caller's header could collide with a field gRPC adds later. That default is
	// right for application metadata and wrong for one specific case: tracing.
	// grpc-go's own instrumentation writes grpc-trace-bin and grpc-tags-bin, which
	// are an integration convention rather than Custom-Metadata, and a client that
	// cannot emit them cannot join a census-instrumented deployment's traces at all.
	//
	// It exempts a name from THAT CHECK ONLY. Name syntax is still validated, and
	// the transport's own headers (content-type, te, grpc-timeout, the
	// connection-specific fields HTTP/2 forbids, pseudo-headers) are still
	// refused however they are spelled. Anything listed beyond tracing headers is
	// the caller's own risk.
	//
	//	Options{AllowReservedMetadata: []string{"grpc-trace-bin"}}
	AllowReservedMetadata []string

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
	// allowReserved is Options.AllowReservedMetadata as a lowercase set, built
	// once so the per-field send-path check is a map lookup. nil when empty,
	// and a nil map reads as "nothing exempted".
	allowReserved map[string]struct{}
	// scheme, authority and userAgent are the byte forms of the Options fields
	// of the same name. They are converted once here rather than per RPC: a
	// []byte(string) conversion in buildHeaders escapes into the header slice
	// and therefore allocates, and none of the three changes for the life of
	// the connection.
	scheme    []byte
	authority []byte
	userAgent []byte
}

// newClientConn builds a ClientConn with the per-connection header bytes
// precomputed. opts must already have been through defaulted().
func newClientConn(c *conn.Conn, opts Options, owned bool) *ClientConn {
	return &ClientConn{
		c:             c,
		opts:          opts,
		owned:         owned,
		scheme:        []byte(opts.Scheme),
		authority:     []byte(opts.Authority),
		userAgent:     []byte(opts.UserAgent),
		allowReserved: reservedAllowSet(opts.AllowReservedMetadata),
	}
}

// Invoker is the method set that code built on top of this package consumes.
//
// ClientConn is concrete and is one HTTP/2 connection, so anything generated or
// written against it is welded to that connection. The useful things to hand a
// client are not always a *ClientConn: a pool or round-robin over several
// connections (this package deliberately ships none — see docs/GRPC_GUIDE.md), a
// wrapper adding per-call auth, retry or metrics, or a double so a client can be
// unit-tested without a socket.
//
// It is a seam for substituting the CONNECTION, not an abstraction over
// transports. NewStream still returns the concrete *Stream, so a substitute can
// front unary calls but cannot fabricate a streaming one without a real
// conn.Stream behind it — *Stream owns the send/receive lifetime contract, and
// abstracting that is a much larger question than this interface answers.
type Invoker interface {
	// Invoke performs a unary RPC: one request message, one response message.
	Invoke(ctx context.Context, method string, req []byte, md []conn.HeaderField, opts ...CallOption) ([]byte, error)
	// NewStream opens a call for any of the streaming shapes.
	NewStream(ctx context.Context, method string, md []conn.HeaderField, opts ...CallOption) (*Stream, error)
}

// The whole point of the interface is that the real connection satisfies it, so
// the assertion is here rather than in a test: a signature change that breaks it
// should fail the build, not a test run.
var _ Invoker = (*ClientConn)(nil)

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
	return newClientConn(c, opts, true), nil
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
	return newClientConn(c, opts, false), nil
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
	// md is metadata supplied through WithMetadata. It is kept separate from the
	// positional argument rather than merged into it, so combining the two costs
	// no copy — buildHeaders walks both.
	md []conn.HeaderField
}

// maxRecvCallOption implements CallOption for MaxRecvMessageSize.
type maxRecvCallOption int

func (o maxRecvCallOption) apply(c *callOptions) { c.maxRecvMessageSize = int(o) }

// MaxRecvMessageSize overrides Options.MaxRecvMessageSize for one call — for
// the single method that returns a large payload, without paying its memory
// cost on every other call sharing the connection.
func MaxRecvMessageSize(n int) CallOption { return maxRecvCallOption(n) }

// metadataCallOption implements CallOption for WithMetadata.
type metadataCallOption struct{ md []conn.HeaderField }

func (o metadataCallOption) apply(c *callOptions) { c.md = append(c.md, o.md...) }

// WithMetadata adds request metadata through the CallOption tail rather than the
// positional argument, so a whole call can be expressed as (ctx, in, opts...)
// with nothing left over — the shape generated code needs, since CallOption is a
// closed interface and only this package can add one.
//
// It composes: several WithMetadata options accumulate, and all of it is sent in
// addition to whatever arrived positionally, positional first. Both sources are
// validated identically and both get the never-indexed default for credential
// fields; neither is trusted more than the other.
//
// The slice is not copied, so it must not be mutated until the call has sent its
// headers — the same rule the positional argument already carries.
func WithMetadata(md []conn.HeaderField) CallOption { return metadataCallOption{md: md} }

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
	co := callOptions{maxRecvMessageSize: cc.opts.MaxRecvMessageSize}
	for _, o := range opts {
		o.apply(&co)
	}
	// Caller-built metadata never went through AppendMetadata, so it is validated
	// in full here. Neither conn nor hpack checks field syntax on the send path,
	// which makes this the last gate before the wire — and it has to cover BOTH
	// sources, which is why the options are resolved before the check rather than
	// after it.
	for _, src := range [2][]conn.HeaderField{md, co.md} {
		for i := range src {
			if err := validMetadata(src[i].Name, src[i].Value, cc.allowReserved); err != nil {
				return nil, err
			}
		}
	}
	return cc.openStream(ctx, method, md, co, nil, false)
}

// openStream builds the header block and opens the stream. When body is non-nil
// it is sent WITH the headers as one transport write and the request side is
// half-closed — the unary shape, where the only message is also the last (#451).
// A nil body leaves the request open for Send/SendLast, which is every streaming
// shape.
//
// The fusion is confined to the unary path on purpose. Deferring HEADERS for
// every stream would break a client-streaming caller that opens a stream and
// waits for the server's response headers before sending anything: the server
// would never see the request at all.
func (cc *ClientConn) openStream(ctx context.Context, method string, md []conn.HeaderField, co callOptions, unaryReq []byte, fuse bool) (*Stream, error) {
	// Buffers first: the fused message is length-prefixed into the stream's own
	// pooled send buffer, so folding the send into the open costs no extra
	// allocation. A nil unaryReq is a legitimate empty request, which is why the
	// caller passes an explicit flag rather than a nil check.
	st := &Stream{}
	st.acquireBufs()
	st.dec.max = co.maxRecvMessageSize

	var body []byte
	if fuse {
		buf, err := AppendMessage(st.sendBuf[:0], unaryReq)
		if err != nil {
			st.releaseBufs()
			return nil, err
		}
		st.sendBuf = buf
		body = buf
	}

	sc := headerScratchPool.Get().(*headerScratch)
	hdrs := cc.buildHeaders(ctx, method, md, co.md, sc)

	s, err := cc.c.NewStream(ctx)
	if err != nil {
		putHeaderScratch(sc)
		st.releaseBufs()
		return nil, err
	}
	// Both send calls encode the block synchronously, so the scratch is free the
	// moment they return — on the error path too.
	if fuse {
		err = s.SendHeadersAndData(ctx, hdrs, body, true)
	} else {
		err = s.SendHeaders(ctx, hdrs, false)
	}
	putHeaderScratch(sc)
	if err != nil {
		// RFC 9113 §8.1: a server may answer in full and then reset the stream
		// with NO_ERROR rather than read a request body it does not want. The
		// response is already buffered, so the stream must be handed back for
		// the receive side to decide the outcome — discarding it here would
		// throw away an answer the server has already sent. This is the case
		// InvokeInto used to reach by tolerating ErrStreamClosed from SendLast;
		// folding the send into the open moved it here.
		if !errors.Is(err, conn.ErrStreamClosed) {
			_ = s.Close()
			st.releaseBufs()
			return nil, err
		}
		st.s = s
		st.sendErr = err // no END_STREAM reached the wire; CloseSend must still try
		return st, nil
	}
	st.s = s
	if fuse {
		// The request is complete. Latched only on success, for the reason
		// SendLast gives: a failure partway leaves DATA on the wire without
		// END_STREAM, and recording the request as half-closed would stop a
		// later CloseSend from finishing the job.
		st.sentEnd = true
	}
	return st, nil
}

// Header names and the fixed values that go with them, in the byte form the
// HPACK encoder takes. The encoder reads them and never retains or mutates
// them (see hpack.HeaderField), so one shared copy serves every connection and
// every concurrent RPC.
var (
	hdrMethod         = []byte(":method")
	hdrScheme         = []byte(":scheme")
	hdrPath           = []byte(":path")
	hdrAuthority      = []byte(":authority")
	hdrContentType    = []byte("content-type")
	hdrUserAgent      = []byte("user-agent")
	hdrTE             = []byte("te")
	hdrAcceptEncoding = []byte("grpc-accept-encoding")
	hdrTimeout        = []byte("grpc-timeout")

	valPOST            = []byte("POST")
	valApplicationGRPC = []byte("application/grpc")
	valTrailers        = []byte("trailers")
	valIdentity        = []byte("identity")
)

// headerScratch is the per-RPC working memory buildHeaders writes into: the
// field slice itself plus the two values that are not constant and would
// otherwise each cost an allocation. It is pooled and reused, so a steady
// stream of RPCs on a connection builds its header blocks without allocating
// at all.
//
// The borrow lasts from buildHeaders until SendHeaders returns. That is safe
// because the HPACK encoder copies every byte it is given — into the wire
// output, and into the dynamic table arena for an indexed field — so nothing
// downstream still points at this memory once the block is encoded.
type headerScratch struct {
	fields []conn.HeaderField
	// path holds the method name, which varies per call.
	path []byte
	// timeout holds the rendered grpc-timeout value, present only when the
	// call's context carries a deadline.
	timeout []byte
}

var headerScratchPool = sync.Pool{
	New: func() any {
		return &headerScratch{
			fields:  make([]conn.HeaderField, 0, 12),
			path:    make([]byte, 0, 64),
			timeout: make([]byte, 0, 16),
		}
	},
}

// putHeaderScratch returns sc to the pool. The field slice is cleared rather
// than merely truncated: its entries point at caller-supplied metadata, which
// for gRPC routinely means credentials, and a pool is exactly the wrong place
// to keep those reachable after the RPC that carried them is over.
func putHeaderScratch(sc *headerScratch) {
	clear(sc.fields)
	sc.fields = sc.fields[:0]
	headerScratchPool.Put(sc)
}

// buildHeaders assembles the request header block into sc: pseudo-headers
// first (RFC 9113 §8.3 requires it), then the fixed gRPC headers, then caller
// metadata. The returned slice is valid until sc goes back to the pool.
func (cc *ClientConn) buildHeaders(ctx context.Context, method string, md, optMD []conn.HeaderField, sc *headerScratch) []conn.HeaderField {
	sc.path = append(sc.path[:0], method...)
	hdrs := append(sc.fields[:0],
		conn.HeaderField{Name: hdrMethod, Value: valPOST},
		conn.HeaderField{Name: hdrScheme, Value: cc.scheme},
		conn.HeaderField{Name: hdrPath, Value: sc.path},
		conn.HeaderField{Name: hdrAuthority, Value: cc.authority},
		conn.HeaderField{Name: hdrContentType, Value: valApplicationGRPC},
		conn.HeaderField{Name: hdrUserAgent, Value: cc.userAgent},
		// te: trailers is mandatory: it tells the server this client
		// understands the trailers that carry grpc-status. RFC 9113 §8.2.2
		// permits te only with this exact value.
		conn.HeaderField{Name: hdrTE, Value: valTrailers},
		conn.HeaderField{Name: hdrAcceptEncoding, Value: valIdentity},
	)
	if dl, ok := ctx.Deadline(); ok {
		sc.timeout = appendTimeout(sc.timeout[:0], time.Until(dl))
		hdrs = append(hdrs, conn.HeaderField{
			Name:  hdrTimeout,
			Value: sc.timeout,
			// The remaining time, at the finest unit that fits 8 digits — so a
			// different value on essentially every RPC. Indexing it would insert
			// an entry that can never be matched again and evict one that could,
			// churning the connection's dynamic table for the life of the conn.
			// Not IndexNever: a deadline is not a credential, and §7.1.3 binds
			// intermediaries to preserve what that mode marks.
			Indexing: conn.IndexWithout,
		})
	}
	// Positional metadata first, then whatever WithMetadata supplied. Walking the
	// two rather than concatenating them keeps this allocation-free.
	for _, src := range [2][]conn.HeaderField{md, optMD} {
		for i := range src {
			f := src[i]
			// The floor, not the ceiling: a caller who never asks for
			// never-indexed would otherwise have their credential added to the
			// connection's HPACK dynamic table, where it outlives the RPC and is
			// emitted as a one-byte index on every later call. Mirrors
			// http3/request.go.
			if defaultSensitiveField(f.Name) {
				f.Indexing = conn.IndexNever
			}
			hdrs = append(hdrs, f)
		}
	}
	// Hand the (possibly regrown) backing array back to sc so the growth is
	// kept rather than repeated on the next borrow.
	sc.fields = hdrs
	return hdrs
}

// Invoke performs a unary RPC: one request message, one response message. It
// is the common case expressed in terms of Stream.
// A caller that needs the response headers, trailers or the full Status uses
// NewStream instead; Invoke is sugar for the case where only the message
// matters.
func (cc *ClientConn) Invoke(ctx context.Context, method string, req []byte, md []conn.HeaderField, opts ...CallOption) ([]byte, error) {
	return cc.InvokeInto(ctx, method, req, nil, md, opts...)
}

// InvokeInto is Invoke appending the response into dst instead of allocating,
// the unary counterpart of Stream.RecvInto:
//
//	buf, err = cc.InvokeInto(ctx, method, req, buf[:0], nil)
//
// The response is appended to dst[:0], so any length dst carries is discarded
// and only its capacity is used. Semantics are otherwise Invoke's, exactly: the
// SendLast fast path and its benign-half-close tolerance, io.EOF turned into an
// Internal status for a method that answered with nothing, and the drain to the
// terminal event that catches a unary method sending two messages.
//
// On error the returned slice is dst[:0] rather than nil, for the reason
// RecvInto gives: a caller looping unary calls on one buffer would otherwise
// lose it to the garbage collector on the first failure. Invoke passes dst=nil
// and so still returns nil.
func (cc *ClientConn) InvokeInto(ctx context.Context, method string, req, dst []byte, md []conn.HeaderField, opts ...CallOption) ([]byte, error) {
	dst = dst[:0]
	if !strings.HasPrefix(method, "/") {
		return dst, fmt.Errorf("%w: %q", ErrBadMethod, method)
	}
	co := callOptions{maxRecvMessageSize: cc.opts.MaxRecvMessageSize}
	for _, o := range opts {
		o.apply(&co)
	}
	for _, src := range [2][]conn.HeaderField{md, co.md} {
		for i := range src {
			if err := validMetadata(src[i].Name, src[i].Value, cc.allowReserved); err != nil {
				return dst, err
			}
		}
	}

	// The request goes out WITH the headers, in one transport write. A unary
	// call knows its only message is its last, so END_STREAM rides that
	// message's DATA frame — and because the body is known before the stream
	// opens, the HEADERS need not be flushed on their own first (#451).
	//
	// conn.ErrStreamClosed here is the RFC 9113 §8.1 case benignHalfClose
	// describes: the server wrote a complete response and reset the stream with
	// NO_ERROR rather than wait for a request body it never read. The answer is
	// already buffered, so the receive side decides the outcome — but unlike the
	// old two-step path there is no stream to read it from when the open itself
	// fails, so this stays an error.
	s, err := cc.openStream(ctx, method, md, co, req, true)
	if err != nil {
		return dst, err
	}
	defer func() { _ = s.Close() }()
	resp, err := s.RecvInto(ctx, dst)
	if err != nil {
		// A bare io.EOF means the call completed with status OK but carried no
		// message. Every other Invoke failure is a *Status, so this one is too
		// rather than making the caller special-case io.EOF.
		if errors.Is(err, io.EOF) {
			return resp, &Status{Code: Internal, Message: "unary method returned no message"}
		}
		return resp, err
	}
	// Drain to the terminal event so a non-OK grpc-status that follows the
	// message is reported rather than swallowed, and so a server that sends
	// two messages to a unary method is caught instead of silently truncated.
	//
	// Deliberately Recv, not RecvInto(resp): handing the response buffer to the
	// drain would let a second message overwrite the answer this call is about
	// to return. Recv allocates only when a message actually arrives, which is
	// the error case.
	if _, err := s.Recv(ctx); !errors.Is(err, io.EOF) {
		if err == nil {
			return resp[:0], &Status{Code: Internal, Message: "unary method returned more than one message"}
		}
		return resp[:0], err
	}
	return resp, nil
}
