package client

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TransportKind selects which transport strategy a Client uses.
type TransportKind int

const (
	// TransportSingleConn is the C.1 default: at most one *conn.Conn
	// per Client, lazy dial, conn-only auto-redial.
	TransportSingleConn TransportKind = iota

	// TransportPool routes requests through *Pool. PoolOptions
	// must be non-nil.
	TransportPool

	// TransportManaged routes requests through a managedPool driven by
	// a Resolver and Selector. Requires ClientOptions.Resolver != nil.
	TransportManaged

	// TransportH1SingleConn is the HTTP/1.1 analogue of TransportSingleConn:
	// at most one *http1.Conn per Client. Requests are serialized (no
	// pipelining). ConnOpts.Dialer must NOT assert ALPN "h2" — use a plain
	// TCP dialer or a TLS dialer with NextProtos containing only "http/1.1".
	TransportH1SingleConn

	// TransportALPN dials with conn.FlexDialer (offers "h2" and "http/1.1")
	// and permanently routes to the protocol negotiated on the first
	// connection. For servers that speak H2 the behavior is identical to
	// TransportSingleConn; for servers that only speak HTTP/1.1 it falls
	// back automatically. ConnOpts.Dialer should be *conn.FlexDialer.
	TransportALPN
)

// DefaultMaxDecompressedSize is the default maximum decompressed body
// size (10 MiB) applied when ClientOptions.MaxDecompressedSize is zero.
const DefaultMaxDecompressedSize int64 = 10 << 20

// DefaultMaxResponseBodySize is the default maximum raw (pre-decompression)
// response body size (32 MiB) applied when ClientOptions.MaxResponseBodySize
// is zero.
const DefaultMaxResponseBodySize int64 = 32 << 20

// ClientOptions tunes a Client. ConnOpts.Dialer is always required.
// Addr is required for TransportSingleConn and TransportPool; it must
// be empty for TransportManaged (the Resolver owns addressing).
type ClientOptions struct {
	// Addr is the "host:port" target used both as the dial target and
	// as the default :authority for requests that don't set one.
	Addr string

	// ConnOpts is forwarded verbatim to conn.Dial. ConnOpts.Dialer
	// must be non-nil.
	ConnOpts conn.ConnOptions

	// DialBackoff suppresses repeated dial attempts within this window
	// after a failed dial. Zero disables suppression (immediate retry).
	// Used by TransportSingleConn. For TransportPool see PoolOptions.DialBackoff.
	DialBackoff time.Duration

	// Transport selects the transport strategy. Zero value =
	// TransportSingleConn.
	Transport TransportKind

	// Pool is required iff Transport == TransportPool. Otherwise it
	// MUST be nil; non-nil with TransportSingleConn is rejected.
	Pool *PoolOptions

	// Resolver is required when Transport == TransportManaged.
	// It discovers backend addresses; the managedPool fans Acquire
	// across per-address sub-pools.
	Resolver Resolver

	// Selector overrides the per-request address selection strategy for
	// TransportManaged. nil → RoundRobin().
	Selector Selector

	// DrainMode governs sub-pool lifecycle when the Resolver removes
	// an address. Zero value = DrainGraceful.
	DrainMode DrainMode

	// Hooks is an optional set of lifecycle callbacks. nil → no hooks
	// fire. May be replaced at runtime via Client.SetHooks.
	Hooks *Hooks

	// DefaultScheme is used as the :scheme pseudo-header when Request.Scheme
	// is empty. Defaults to "https" when zero. Set to "http" for H2C targets.
	// PushHandler is invoked when the server sends a PUSH_PROMISE frame
	// (RFC 7540 §8.2). When non-nil, ConnOpts.EnablePush is automatically
	// set to true at client construction so the peer knows push is allowed.
	//
	// The handler runs in a dedicated goroutine. promisedHeaders are the
	// request headers the server promises to fulfil (decoded from
	// PUSH_PROMISE). resp is the fully drained pushed response — Body is
	// always populated regardless of WantBody. err is non-nil if the push
	// failed (reset, connection closed, etc.).
	//
	// When PushHandler is nil, server push is disabled and PUSH_PROMISE
	// frames trigger PROTOCOL_ERROR at the conn layer.
	PushHandler PushHandler

	// DefaultScheme is used as the :scheme pseudo-header when Request.Scheme
	// is empty. Defaults to "https" when zero. Set to "http" for H2C targets.
	DefaultScheme string

	// RateLimitPerSecond caps the client's outgoing request rate
	// using a token-bucket algorithm. Zero disables rate limiting.
	// Burst capacity is controlled by RateLimitBurst; when the
	// limit is reached, Do/DoStream blocks until a token is
	// available or ctx is cancelled. Useful for load generators
	// enforcing a strict QPS budget.
	RateLimitPerSecond float64

	// RateLimitBurst is the maximum number of tokens that can be
	// consumed back-to-back without replenishment. Zero (the
	// default) means burst equals RateLimitPerSecond — i.e. one
	// second of accumulated tokens. Only meaningful when
	// RateLimitPerSecond > 0. Larger bursts smooth over short
	// traffic spikes at the cost of worse steady-state QPS
	// enforcement.
	RateLimitBurst float64

	// MaxDecompressedSize caps the decompressed response body size to
	// guard against gzip/zlib bombs (decompression ratio attacks).
	// Zero → DefaultMaxDecompressedSize (10 MiB). When the decompressed
	// payload exceeds this limit, drainResponse returns ErrBodyTooLarge.
	MaxDecompressedSize int64

	// MaxResponseBodySize caps the total raw bytes received on a single
	// response (pre-decompression, summed across all DATA frames).
	// Zero → DefaultMaxResponseBodySize (32 MiB). Exceeding this limit
	// causes drainResponse to return ErrBodyTooLarge without reading
	// further frames.
	MaxResponseBodySize int64
}

// PushHandler is invoked when the server pushes a resource in response
// to a client request. The client automatically drains the pushed stream
// into resp before calling the handler. If the push fails (RST_STREAM,
// connection error), err is non-nil and resp may be partially populated.
type PushHandler func(ctx context.Context, promisedHeaders []conn.HeaderField, resp *Response, err error)

// Client is a high-level HTTP/2 client wrapping a single connection.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	tr                  transport
	authority           string
	defaultScheme       string
	hooksPtr            *atomic.Pointer[Hooks]
	metrics             *Metrics
	pushHandler         PushHandler
	rateLimiter         *rateLimiter // nil when RateLimitPerSecond is 0
	maxDecompressedSize int64
	maxResponseBodySize int64
}

// NewClient validates opts and constructs a Client. It does NOT dial;
// the first Do or DoStream call triggers a lazy connection establish.
func NewClient(opts ClientOptions) (*Client, error) {
	if opts.Transport != TransportManaged {
		if opts.Addr == "" || containsAnyWhitespace(opts.Addr) {
			return nil, fmt.Errorf("client: ClientOptions.Addr must be a non-empty host:port without whitespace")
		}
	}
	if opts.ConnOpts.Dialer == nil {
		return nil, fmt.Errorf("client: ClientOptions.ConnOpts.Dialer is required")
	}
	if opts.PushHandler != nil {
		opts.ConnOpts.EnablePush = true
	}
	switch opts.Transport {
	case TransportSingleConn, TransportH1SingleConn, TransportALPN:
		if opts.Pool != nil {
			return nil, fmt.Errorf("%w: Pool must be nil for this transport kind", ErrInvalidPoolOptions)
		}
	case TransportPool:
		if opts.Pool == nil {
			return nil, fmt.Errorf("%w: Pool is required for TransportPool", ErrInvalidPoolOptions)
		}
	case TransportManaged:
		if opts.Resolver == nil {
			return nil, fmt.Errorf("%w: Resolver is required for TransportManaged", ErrInvalidOptions)
		}
		if opts.Addr != "" {
			return nil, fmt.Errorf("%w: Addr must be empty for TransportManaged (Resolver owns addressing)", ErrInvalidOptions)
		}
	default:
		return nil, fmt.Errorf("%w: %d", ErrInvalidTransportKind, int(opts.Transport))
	}
	metrics := &Metrics{}
	hooksPtr := new(atomic.Pointer[Hooks])
	hooksPtr.Store(opts.Hooks)
	var tr transport
	switch opts.Transport {
	case TransportSingleConn:
		tr = &singleConn{
			addr:     opts.Addr,
			connOpts: opts.ConnOpts,
			backoff:  opts.DialBackoff,
			hooksRef: hooksPtr,
			metrics:  metrics,
		}
	case TransportPool:
		tr = newPoolTransport(opts.Addr, opts.ConnOpts, *opts.Pool, hooksPtr, metrics)
	case TransportManaged:
		po := PoolOptions{}
		if opts.Pool != nil {
			po = *opts.Pool
		}
		mp, err := newManagedPool(opts.Resolver, opts.Selector, opts.DrainMode, opts.ConnOpts, po, hooksPtr, metrics)
		if err != nil {
			return nil, err
		}
		tr = &managedTransport{mp: mp}
	case TransportH1SingleConn:
		tr = &h1singleConn{
			addr:     opts.Addr,
			dialer:   opts.ConnOpts.Dialer,
			backoff:  opts.DialBackoff,
			hooksRef: hooksPtr,
			metrics:  metrics,
		}
	case TransportALPN:
		tr = &alpnSingleConn{
			addr:     opts.Addr,
			connOpts: opts.ConnOpts,
			backoff:  opts.DialBackoff,
			hooksRef: hooksPtr,
			metrics:  metrics,
		}
	}
	scheme := opts.DefaultScheme
	if scheme == "" {
		scheme = "https"
	}
	var rl *rateLimiter
	if opts.RateLimitPerSecond > 0 {
		burst := opts.RateLimitBurst
		if burst <= 0 {
			burst = opts.RateLimitPerSecond
		}
		rl = newRateLimiter(opts.RateLimitPerSecond, burst)
	}
	maxDecompressed := opts.MaxDecompressedSize
	if maxDecompressed <= 0 {
		maxDecompressed = DefaultMaxDecompressedSize
	}
	maxBody := opts.MaxResponseBodySize
	if maxBody <= 0 {
		maxBody = DefaultMaxResponseBodySize
	}
	c := &Client{
		tr:                  tr,
		authority:           deriveAuthority(opts.Addr),
		defaultScheme:       scheme,
		hooksPtr:            hooksPtr,
		metrics:             metrics,
		pushHandler:         opts.PushHandler,
		rateLimiter:         rl,
		maxDecompressedSize: maxDecompressed,
		maxResponseBodySize: maxBody,
	}
	return c, nil
}

// Close releases the underlying transport. Subsequent Do/DoStream
// calls return ErrClosed. Idempotent.
func (c *Client) Close() error {
	return c.tr.close()
}

// Shutdown performs a graceful close. New requests receive
// ErrConnDraining. The transport is given gracefulTimeout to complete
// in-flight requests; after that it is force-closed. Idempotent.
// For a single-conn transport, Shutdown sends GOAWAY and waits
// for the inflight count to reach zero on the underlying *conn.Conn.
// For pool transports, all conns are closed in parallel (no
// per-conn draining is exposed at the pool level).
func (c *Client) Shutdown(gracefulTimeout time.Duration) error {
	return c.tr.shutdown(gracefulTimeout)
}

// Warmup pre-dials up to n connections in the background, returning
// immediately. n is capped at the underlying transport's MaxConnsPerHost
// (1 for TransportSingleConn). Use this before a workload burst to
// avoid paying TLS handshake + HTTP/2 setup on the first request.
// Dial errors are surfaced via the OnDial hook. Idempotent — calling
// Warmup on an already-warm client is a no-op.
func (c *Client) Warmup(n int) {
	c.tr.warmup(n)
}

// PoolStats returns a snapshot of the underlying pool's state. It
// returns the zero Stats when the transport is not a *poolTransport
// or *managedTransport (e.g. TransportSingleConn) or the pool is
// already closed.
func (c *Client) PoolStats() Stats {
	switch t := c.tr.(type) {
	case *poolTransport:
		return t.p.Stats()
	case *managedTransport:
		return t.mp.stats()
	}
	return Stats{}
}

// Do issues a synchronous request and writes the result into resp.
// The caller must allocate resp once and call resp.Reset() before each reuse.
// On error, resp fields are undefined; call resp.Reset() before reuse regardless.
// observeStart fires the OnRequestStart hook and increments RequestsStarted.
func (c *Client) observeStart(req *Request, authority string) {
	if h := c.hooksPtr.Load(); h != nil && h.OnRequestStart != nil {
		h.OnRequestStart(RequestStartEvent{
			Method: req.Method, Path: req.Path, Authority: authority, Attempt: 0,
		})
	}
	c.metrics.Counters.RequestsStarted.Add(1)
}

// observeDone records latency, success/error counters, and fires
// the OnRequestComplete hook.
func (c *Client) observeDone(req *Request, authority string, status int, bytesSent, bytesRecv int64, err error, latency time.Duration) {
	c.metrics.Latency.Request.Observe(latency)
	if err == nil {
		c.metrics.Counters.RequestsSucceeded.Add(1)
	} else {
		c.metrics.Counters.RequestsErrored.Add(1)
	}
	if h := c.hooksPtr.Load(); h != nil && h.OnRequestComplete != nil {
		h.OnRequestComplete(RequestCompleteEvent{
			Method: req.Method, Path: req.Path, Authority: authority,
			Status: status, Err: err, Latency: latency,
			BytesSent: bytesSent, BytesRecv: bytesRecv,
			Attempt: 0,
		})
	}
}

// Do executes a synchronous HTTP/2 request, populating resp on success.
func (c *Client) Do(ctx context.Context, req *Request, resp *Response) error {
	if err := validateRequest(req); err != nil {
		return err
	}
	if c.rateLimiter != nil {
		if err := c.rateLimiter.Take(ctx); err != nil {
			return err
		}
	}
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	authority := req.Authority
	if authority == "" {
		authority = c.authority
	}
	c.observeStart(req, authority)
	start := time.Now()

	err := c.do(ctx, req, resp)

	var status int
	var bytesRecv int64
	if err == nil && resp != nil {
		status = resp.Status
		bytesRecv = resp.BytesReceived
	}
	c.observeDone(req, authority, status, int64(len(req.Body)), bytesRecv, err, time.Since(start))
	return err
}

// do is the inner request transport, without hook/metric wrapping.
// sendRequest (defined just above do) returns (s, cn, release, err)
// by value; the caller unpacks them and is responsible for calling
// release() and s.Close().

// sendRequest opens a protocol exchange, builds and sends request headers,
// writes the body and trailers, and returns the exchange ready for response
// reading. On error the transport is released and no cleanup is needed.
//
// Returns (s, pushLookup, release, err). pushLookup is non-nil only for H2
// transports and is passed to drainResponse to handle server push.
//
// Avoids heap-escaping the implicit struct: escape analysis confirmed via
// -gcflags=-m that returning fields by value keeps them on the stack
// (verified 2026-06-15 for the H2 hot path).
func (c *Client) sendRequest(ctx context.Context, req *Request) (s protoStream, pushLookup func(uint32) (*conn.Stream, bool), release func(), err error) {
	s, pushLookup, release, err = c.tr.openExchange(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	hdrs := buildHeaders(req, c.authority, c.defaultScheme, sp)
	trailers := hasTrailers(req)
	endStream := len(req.Body) == 0 && req.BodyReader == nil && !trailers
	if err = s.SendHeadersWithPriority(ctx, hdrs, endStream, req.Priority); err != nil {
		*sp = (*sp)[:0]
		hdrSlicePool.Put(sp)
		_ = s.Close()
		release()
		return nil, nil, nil, err
	}
	*sp = (*sp)[:0]
	hdrSlicePool.Put(sp)

	if !endStream || trailers {
		if err = writeRequestBody(ctx, s, req, !trailers); err != nil {
			_ = s.Close()
			release()
			return nil, nil, nil, err
		}
		if trailers {
			if err = writeRequestTrailers(ctx, s, req); err != nil {
				_ = s.Close()
				release()
				return nil, nil, nil, err
			}
		}
	}

	return s, pushLookup, release, nil
}

func (c *Client) do(ctx context.Context, req *Request, resp *Response) error {
	s, pushLookup, release, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}

	if req.StreamBody {
		if resp == nil {
			_ = s.Close()
			release()
			return fmt.Errorf("client: StreamBody requires a non-nil *Response")
		}
		ev, err := s.Recv(ctx)
		if err != nil {
			_ = s.Close()
			release()
			return err
		}
		if ev.Type != conn.EventHeaders {
			_ = s.Close()
			release()
			return fmt.Errorf("client: expected initial HEADERS, got %s", ev.Type)
		}
		n, perr := parseStatus(ev.Headers, &resp.Headers)
		if perr != nil {
			_ = s.Close()
			release()
			return perr
		}
		resp.Status = n
		if ev.Slab != nil {
			resp.slabs = append(resp.slabs, ev.Slab)
		}
		// StreamBody requires a *conn.Stream for responseBodyReader.
		// H1.1 does not support StreamBody; the protoStream is always
		// a *conn.Stream here for that feature.
		cs, ok := s.(*conn.Stream)
		if !ok {
			_ = s.Close()
			release()
			return fmt.Errorf("client: StreamBody not supported for HTTP/1.1 connections")
		}
		resp.BodyReader = &responseBodyReader{
			ctx:     ctx,
			stream:  cs,
			release: release,
			resp:    resp,
		}
		if !req.DisableDecompression {
			enc := detectEncoding(resp.Headers)
			if enc != EncodingIdentity {
				dr, derr := newDecompressingReader(enc, resp.BodyReader)
				if derr != nil {
					// Release via the responseBodyReader: its closeOnce makes
					// the single stream.Close()+release() idempotent against the
					// caller's later resp.Reset(). Clearing BodyReader drops the
					// now-dead reference.
					_ = resp.BodyReader.Close()
					resp.BodyReader = nil
					return derr
				}
				resp.BodyReader = dr
			}
		}
		return nil // release deferred to resp.BodyReader.Close()
	}

	err = drainResponse(ctx, pushLookup, s, req, resp, c.pushHandler, c.maxDecompressedSize, c.maxResponseBodySize)
	_ = s.Close()
	release()
	return err
}

// DoStream issues a request and returns once the initial HEADERS frame
// has arrived. The caller pumps StreamResponse.Recv for subsequent
// DATA / trailers / reset events. The caller MUST call
// StreamResponse.Close if it does not drain the stream.
//
// The caller may allocate StreamResponse once and reuse it across calls;
// DoStream calls sr.reset() internally before populating fields.
func (c *Client) DoStream(ctx context.Context, req *Request, sr *StreamResponse) error {
	if err := validateRequest(req); err != nil {
		return err
	}
	if c.rateLimiter != nil {
		if err := c.rateLimiter.Take(ctx); err != nil {
			return err
		}
	}
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	sr.reset()
	authority := req.Authority
	if authority == "" {
		authority = c.authority
	}
	c.observeStart(req, authority)
	start := time.Now()

	err := c.doStream(ctx, req, sr)

	var status int
	if err == nil {
		status = sr.Status
	}
	c.observeDone(req, authority, status, int64(len(req.Body)), 0, err, time.Since(start))
	return err
}

// doStream is the inner streaming transport, without hook/metric wrapping.
func (c *Client) doStream(ctx context.Context, req *Request, sr *StreamResponse) error {
	s, _, release, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}

	ev, err := s.Recv(ctx)
	if err != nil {
		_ = s.Close()
		release()
		return err
	}
	if ev.Type != conn.EventHeaders {
		_ = s.Close()
		release()
		return fmt.Errorf("client: expected initial HEADERS, got %s", ev.Type)
	}
	n, perr := parseStatus(ev.Headers, &sr.Headers)
	if perr != nil {
		_ = s.Close()
		release()
		return perr
	}
	sr.Status = n
	if ev.Slab != nil {
		sr.slabs = append(sr.slabs, ev.Slab) // transfer slab ownership
	}
	// DoStream requires a *conn.Stream; H1.1 connections do not support it.
	cs, ok := s.(*conn.Stream)
	if !ok {
		_ = s.Close()
		release()
		return fmt.Errorf("client: DoStream not supported for HTTP/1.1 connections")
	}
	sr.stream = cs
	sr.release = release
	if ev.EndStream {
		sr.drained = true
	}
	return nil
}

// SetHooks atomically replaces the active hook set. Pass nil to
// disable hooks. Safe to call concurrently with Do/DoStream.
func (c *Client) SetHooks(h *Hooks) {
	c.hooksPtr.Store(h)
}

// Metrics returns the live metrics struct. The returned pointer is
// stable for the lifetime of the Client; do not value-copy.
// Use MetricsSnapshot for a value-safe view.
func (c *Client) Metrics() *Metrics {
	return c.metrics
}

// MetricsSnapshot returns a frozen, value-copyable view of metrics.
func (c *Client) MetricsSnapshot() MetricsSnapshot {
	return c.metrics.Snapshot()
}

// Pseudo-header name bytes. The HPACK encoder reads these but never
// mutates them, so sharing across concurrent callers is safe.
var (
	hdrMethod        = []byte(":method")
	hdrScheme        = []byte(":scheme")
	hdrAuthority     = []byte(":authority")
	hdrPath          = []byte(":path")
	hdrProtocol      = []byte(":protocol")
	hdrContentLength = []byte("content-length")
	hdrTrailer       = []byte("trailer")
	hdrStatus        = []byte(":status")
)

// unsafeStringToBytes returns a []byte backed by the same memory as s.
// The caller must not mutate the returned slice. This avoids the
// allocation of []byte(string) in the hot header-building path.
func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s)) //nolint:gosec
}

// hdrSlicePool recycles the []conn.HeaderField backing array used by
// buildHeaders. EncodeBlock is synchronous, so the slice is safe to
// return immediately after SendHeaders returns. The buildHeaders
// caller (sendRequest) does the Get/Put directly so that no
// put-closure escapes to the heap.
var hdrSlicePool = sync.Pool{
	New: func() any {
		s := make([]conn.HeaderField, 0, 10)
		return &s
	},
}

// buildHeaders assembles the on-wire HEADERS slice with pseudo-headers
// first. sp is the pooled backing array (caller obtains from
// hdrSlicePool.Get and is responsible for Put after SendHeaders).
// Returns the populated slice. No put-closure is returned — callers
// do \`*sp = (*sp)[:0]; hdrSlicePool.Put(sp)\` inline.
func buildHeaders(req *Request, defaultAuthority, defaultScheme string, sp *[]conn.HeaderField) []conn.HeaderField {
	scheme := req.Scheme
	if scheme == "" {
		scheme = defaultScheme
	}
	authority := req.Authority
	if authority == "" {
		authority = defaultAuthority
	}
	*sp = (*sp)[:0]
	*sp = append(*sp,
		conn.HeaderField{Name: hdrMethod, Value: unsafeStringToBytes(req.Method)},
		conn.HeaderField{Name: hdrScheme, Value: unsafeStringToBytes(scheme)},
		conn.HeaderField{Name: hdrAuthority, Value: unsafeStringToBytes(authority)},
		conn.HeaderField{Name: hdrPath, Value: unsafeStringToBytes(req.Path)},
	)
	if req.Protocol != "" {
		*sp = append(*sp, conn.HeaderField{Name: hdrProtocol, Value: unsafeStringToBytes(req.Protocol)})
	}
	*sp = append(*sp, req.Headers...)
	if !req.DisableDecompression && shouldSendAcceptEncoding(req) {
		*sp = append(*sp, conn.HeaderField{Name: hdrAcceptEncoding, Value: encGzip})
	}
	if req.BodyReader != nil && req.ContentLength > 0 {
		*sp = append(*sp, conn.HeaderField{
			Name:  hdrContentLength,
			Value: []byte(strconv.FormatInt(req.ContentLength, 10)),
		})
	}
	// Announce trailers in the initial HEADERS frame so the peer can
	// allocate a Trailer map before the body arrives (required by the
	// Go net/http HTTP/2 server and recommended by RFC 7230 §4.4).
	if tv := trailerAnnouncement(req); len(tv) > 0 {
		*sp = append(*sp, conn.HeaderField{Name: hdrTrailer, Value: tv})
	}
	return *sp
}

// resolveTrailerFields returns the effective trailer fields for req.
// TrailerFunc wins; falls back to Trailers when TrailerFunc returns nil.
func resolveTrailerFields(req *Request) []conn.HeaderField {
	if req.TrailerFunc != nil {
		if result := req.TrailerFunc(); result != nil {
			return result
		}
	}
	return req.Trailers
}

// trailerAnnouncement returns a comma-separated list of lowercase trailer
// field names for the "trailer" request header, or nil when no trailers
// will be sent. Pseudo-headers are silently skipped — they are invalid in
// trailers and are caught (with error) by resolveTrailers at send time.
func trailerAnnouncement(req *Request) []byte {
	fields := resolveTrailerFields(req)
	if len(fields) == 0 {
		return nil
	}
	var b []byte
	for _, f := range fields {
		if len(f.Name) > 0 && f.Name[0] == ':' {
			continue // pseudo-headers are invalid in trailers; skip announcement
		}
		if len(b) > 0 {
			b = append(b, ',')
		}
		b = append(b, f.Name...)
	}
	return b
}

// hasTrailers reports whether req will send a trailer HEADERS frame.
func hasTrailers(req *Request) bool {
	return len(req.Trailers) > 0 || req.TrailerFunc != nil
}

// resolveTrailers returns the trailer fields to send. TrailerFunc wins;
// falls back to Trailers when TrailerFunc returns nil.
// Returns error if resolved fields contain pseudo-headers.
func resolveTrailers(req *Request) ([]conn.HeaderField, error) {
	fields := resolveTrailerFields(req)
	for i := range fields {
		if len(fields[i].Name) > 0 && fields[i].Name[0] == ':' {
			return nil, fmt.Errorf("%w: pseudo-header %q in trailer",
				ErrInvalidRequest, fields[i].Name)
		}
	}
	return fields, nil
}

// writeRequestTrailers resolves and sends the trailer HEADERS frame.
func writeRequestTrailers(ctx context.Context, s protoStream, req *Request) error {
	fields, err := resolveTrailers(req)
	if err != nil {
		return err
	}
	return s.SendHeaders(ctx, fields, true)
}

// writeRequestBody writes Body or BodyReader as DATA frames.
// endStream controls whether the final DATA frame sets END_STREAM.
// Pass false when a trailer HEADERS frame will follow.
func writeRequestBody(ctx context.Context, s protoStream, req *Request, endStream bool) error {
	if req.BodyReader != nil {
		return writeBodyReader(ctx, s, req.BodyReader, endStream)
	}
	if len(req.Body) == 0 {
		// No body content; skip DATA entirely when trailers follow
		// (endStream=false). The caller sends END_STREAM via HEADERS.
		if !endStream {
			return nil
		}
		return s.SendData(ctx, nil, true)
	}
	return s.SendData(ctx, req.Body, endStream)
}

// readChunkSize is the per-Read buffer for streaming uploads. The
// underlying conn layer further chunks at the peer's MAX_FRAME_SIZE
// and respects flow control.
const readChunkSize = 16 * 1024

// uploadBufPool recycles the per-call read buffer used by writeBodyReader.
var uploadBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, readChunkSize)
		return &b
	},
}

// writeBodyReader streams an io.Reader into DATA frames.
// endStream controls whether the final DATA frame sets END_STREAM.
func writeBodyReader(ctx context.Context, s protoStream, r io.Reader, endStream bool) error {
	bufp := uploadBufPool.Get().(*[]byte)
	defer uploadBufPool.Put(bufp)
	buf := *bufp
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			final := rerr == io.EOF && endStream
			if werr := s.SendData(ctx, buf[:n], final); werr != nil {
				return werr
			}
			if rerr == io.EOF {
				return nil
			}
		}
		if rerr == io.EOF {
			if endStream {
				return s.SendData(ctx, nil, true)
			}
			return nil // trailers follow; caller sends END_STREAM via HEADERS
		}
		if rerr != nil {
			// On read error other than io.EOF, abort the upload.
			return fmt.Errorf("client: read request body: %w", rerr)
		}
	}
}

// drainResponse pumps stream events until the response side ends or
// the stream resets, writing into resp in place.
// pushLookup is non-nil for H2 connections and resolves push-promised stream IDs;
// it is nil for H1.1 (which has no server push).
func drainResponse(ctx context.Context, pushLookup func(uint32) (*conn.Stream, bool), s protoStream, req *Request, resp *Response, h PushHandler, maxDecompressed, maxBody int64) error {
	var gotHeaders bool
	var enc ContentEncoding
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			return err
		}
		switch ev.Type {
		case conn.EventHeaders:
			done, perr := handleHeadersEvent(ev, req, resp, &gotHeaders, &enc)
			if perr != nil {
				return perr
			}
			if done {
				return nil
			}
		case conn.EventData:
			done, derr := handleDataEvent(ev, req, resp, enc, maxBody, maxDecompressed)
			if derr != nil {
				return derr
			}
			if done {
				return nil
			}
		case conn.EventTrailers:
			if req.WantTrailers {
				resp.Trailers = append(resp.Trailers, ev.Headers...)
				if ev.Slab != nil {
					resp.slabs = append(resp.slabs, ev.Slab)
				}
			}
			if ev.EndStream {
				return nil
			}
		case conn.EventReset:
			return &StreamResetError{Code: ev.RSTCode}
		case conn.EventPushPromise:
			if h != nil && pushLookup != nil && ev.PushStreamID > 0 {
				if ps, ok := pushLookup(ev.PushStreamID); ok {
					// Copy promised headers to decouple from the slab
					// lifetime (slab is returned when parent resp is Reset).
					hdrs := copyHeaders(ev.Headers)
					go drainPushedStream(ctx, pushLookup, h, hdrs, ps, maxDecompressed, maxBody)
				}
			}
		}
	}
}

// handleHeadersEvent processes a single EventHeaders from drainResponse.
// Returns (done=true, nil) when the stream is complete.
func handleHeadersEvent(ev conn.StreamEvent, req *Request, resp *Response, gotHeaders *bool, enc *ContentEncoding) (done bool, err error) {
	if *gotHeaders {
		return ev.EndStream, nil
	}
	n, perr := parseStatus(ev.Headers, &resp.Headers)
	if perr != nil {
		return false, perr
	}
	resp.Status = n
	if ev.Slab != nil {
		resp.slabs = append(resp.slabs, ev.Slab)
	}
	if !req.DisableDecompression {
		*enc = detectEncoding(resp.Headers)
	}
	*gotHeaders = true
	return ev.EndStream, nil
}

// handleDataEvent processes a single EventData from drainResponse.
// Returns (done=true, nil) when the stream is complete.
func handleDataEvent(ev conn.StreamEvent, req *Request, resp *Response, enc ContentEncoding, maxBody, maxDecompressed int64) (done bool, err error) {
	resp.BytesReceived += int64(len(ev.Data))
	over := resp.BytesReceived > maxBody
	if req.WantBody && len(ev.Data) > 0 && !over {
		resp.Body = append(resp.Body, ev.Data...)
	}
	// Payload consumed (copied out, unwanted, or over-limit): return the pooled
	// buffer on every exit path.
	if ev.DataSlab != nil {
		conn.GetDataBufPool().Put(ev.DataSlab)
	}
	if over {
		return false, fmt.Errorf("%w: received %d bytes, limit %d", ErrBodyTooLarge, resp.BytesReceived, maxBody)
	}
	if ev.EndStream {
		if req.WantBody && enc != EncodingIdentity {
			decoded, derr := decompressFully(enc, resp.Body, maxDecompressed)
			if derr != nil {
				return false, derr
			}
			resp.Body = decoded
		}
		return true, nil
	}
	return false, nil
}

// drainPushedStream reads the pushed stream's response and invokes the
// push handler with the result. Handles nested PUSH_PROMISE recursively.
func drainPushedStream(ctx context.Context, pushLookup func(uint32) (*conn.Stream, bool), h PushHandler, promisedHeaders []conn.HeaderField, s *conn.Stream, maxDecompressed, maxBody int64) {
	pr := &Response{}
	derr := drainResponse(ctx, pushLookup, s, &Request{WantBody: true}, pr, h, maxDecompressed, maxBody)
	_ = s.Close()
	h(ctx, promisedHeaders, pr, derr)
}

// copyHeaders returns a deep copy of the header fields, duplicating the
// Name and Value byte slices so the result does not alias slab memory.
func copyHeaders(in []conn.HeaderField) []conn.HeaderField {
	if len(in) == 0 {
		return nil
	}
	out := make([]conn.HeaderField, len(in))
	for i := range in {
		out[i].Name = append([]byte(nil), in[i].Name...)
		out[i].Value = append([]byte(nil), in[i].Value...)
		out[i].Sensitive = in[i].Sensitive
	}
	return out
}

// deriveAuthority strips the port if it equals 80 (http) or 443
// (https). Handles IPv6 literals via net.SplitHostPort.
func deriveAuthority(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return addr
	}
	if port == "80" || port == "443" {
		// Re-bracket IPv6 literals so the result is a valid :authority
		// host (RFC 3986 §3.2.2).
		if strings.Contains(host, ":") {
			return "[" + host + "]"
		}
		return host
	}
	return addr
}
