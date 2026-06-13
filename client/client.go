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

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/hpack"
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
)

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
	DefaultScheme string
}

// Client is a high-level HTTP/2 client wrapping a single connection.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	tr            transport
	authority     string
	defaultScheme string
	hooksPtr      *atomic.Pointer[Hooks]
	metrics       *Metrics
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
	switch opts.Transport {
	case TransportSingleConn:
		if opts.Pool != nil {
			return nil, fmt.Errorf("%w: Pool must be nil for TransportSingleConn", ErrInvalidPoolOptions)
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
	}
	scheme := opts.DefaultScheme
	if scheme == "" {
		scheme = "https"
	}
	c := &Client{
		tr:            tr,
		authority:     deriveAuthority(opts.Addr),
		defaultScheme: scheme,
		hooksPtr:      hooksPtr,
		metrics:       metrics,
	}
	return c, nil
}

// Close releases the underlying transport. Subsequent Do/DoStream
// calls return ErrClosed. Idempotent.
func (c *Client) Close() error {
	return c.tr.close()
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

func (c *Client) Do(ctx context.Context, req *Request, resp *Response) error {
	if err := validateRequest(req); err != nil {
		return err
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
// sentRequest holds the state after a request has been fully written to the
// wire: the conn.Stream ready for response reading, plus the transport release
// callback. The caller is responsible for calling release() and s.Close().
type sentRequest struct {
	s       *conn.Stream
	release func()
}

// sendRequest acquires a connection, opens a stream, builds and sends headers,
// writes the body and trailers, and returns the stream ready for response
// reading. On error the transport is released and no cleanup is needed.
func (c *Client) sendRequest(ctx context.Context, req *Request) (*sentRequest, error) {
	cn, release, err := c.tr.acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := cn.NewStream(ctx)
	if err != nil {
		release()
		return nil, err
	}

	hdrs, putHdrs := buildHeaders(req, c.authority, c.defaultScheme)
	trailers := hasTrailers(req)
	endStream := len(req.Body) == 0 && req.BodyReader == nil && !trailers
	if err := s.SendHeaders(ctx, hdrs, endStream); err != nil {
		putHdrs()
		_ = s.Close()
		release()
		return nil, err
	}
	putHdrs()

	if !endStream || trailers {
		if err := writeRequestBody(ctx, s, req, !trailers); err != nil {
			_ = s.Close()
			release()
			return nil, err
		}
		if trailers {
			if err := writeRequestTrailers(ctx, s, req); err != nil {
				_ = s.Close()
				release()
				return nil, err
			}
		}
	}

	return &sentRequest{s: s, release: release}, nil
}

func (c *Client) do(ctx context.Context, req *Request, resp *Response) error {
	sr, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}
	s, release := sr.s, sr.release

	if req.StreamBody {
		// StreamBody requires a non-nil Response to populate BodyReader.
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
		resp.BodyReader = &responseBodyReader{
			ctx:     ctx,
			stream:  s,
			release: release,
			resp:    resp,
		}
		return nil // release deferred to resp.BodyReader.Close()
	}

	err = drainResponse(ctx, s, req, resp)
	_ = s.Close() // recycles stream when both sides ended
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
	sent, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}
	s, release := sent.s, sent.release

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
	sr.stream = s
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
	hdrContentLength = []byte("content-length")
	hdrTrailer       = []byte("trailer")
	hdrStatus        = []byte(":status")
)

// hdrSlicePool recycles the []hpack.HeaderField backing array used by
// buildHeaders. EncodeBlock is synchronous, so the slice is safe to
// return immediately after SendHeaders returns.
var hdrSlicePool = sync.Pool{
	New: func() any {
		s := make([]hpack.HeaderField, 0, 10)
		return &s
	},
}

// buildHeaders assembles the on-wire HEADERS slice with pseudo-headers
// first. Returns the slice and a put function; caller MUST call put()
// after SendHeaders returns to return the slice to the pool.
func buildHeaders(req *Request, defaultAuthority, defaultScheme string) ([]hpack.HeaderField, func()) {
	scheme := req.Scheme
	if scheme == "" {
		scheme = defaultScheme
	}
	authority := req.Authority
	if authority == "" {
		authority = defaultAuthority
	}
	sp := hdrSlicePool.Get().(*[]hpack.HeaderField)
	*sp = (*sp)[:0]
	*sp = append(*sp,
		hpack.HeaderField{Name: hdrMethod, Value: []byte(req.Method)},
		hpack.HeaderField{Name: hdrScheme, Value: []byte(scheme)},
		hpack.HeaderField{Name: hdrAuthority, Value: []byte(authority)},
		hpack.HeaderField{Name: hdrPath, Value: []byte(req.Path)},
	)
	*sp = append(*sp, req.Headers...)
	if req.BodyReader != nil && req.ContentLength > 0 {
		*sp = append(*sp, hpack.HeaderField{
			Name:  hdrContentLength,
			Value: []byte(strconv.FormatInt(req.ContentLength, 10)),
		})
	}
	// Announce trailers in the initial HEADERS frame so the peer can
	// allocate a Trailer map before the body arrives (required by the
	// Go net/http HTTP/2 server and recommended by RFC 7230 §4.4).
	if tv := trailerAnnouncement(req); len(tv) > 0 {
		*sp = append(*sp, hpack.HeaderField{Name: hdrTrailer, Value: tv})
	}
	return *sp, func() {
		*sp = (*sp)[:0]
		hdrSlicePool.Put(sp)
	}
}

// resolveTrailerFields returns the effective trailer fields for req.
// TrailerFunc wins; falls back to Trailers when TrailerFunc returns nil.
func resolveTrailerFields(req *Request) []hpack.HeaderField {
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
func resolveTrailers(req *Request) ([]hpack.HeaderField, error) {
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
func writeRequestTrailers(ctx context.Context, s *conn.Stream, req *Request) error {
	fields, err := resolveTrailers(req)
	if err != nil {
		return err
	}
	return s.SendHeaders(ctx, fields, true)
}

// writeRequestBody writes Body or BodyReader as DATA frames.
// endStream controls whether the final DATA frame sets END_STREAM.
// Pass false when a trailer HEADERS frame will follow.
func writeRequestBody(ctx context.Context, s *conn.Stream, req *Request, endStream bool) error {
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
func writeBodyReader(ctx context.Context, s *conn.Stream, r io.Reader, endStream bool) error {
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
func drainResponse(ctx context.Context, s *conn.Stream, req *Request, resp *Response) error {
	var gotHeaders bool
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			return err
		}
		switch ev.Type {
		case conn.EventHeaders:
			if gotHeaders {
				if ev.EndStream {
					return nil
				}
				continue
			}
			n, perr := parseStatus(ev.Headers, &resp.Headers)
			if perr != nil {
				return perr
			}
			resp.Status = n
			if ev.Slab != nil {
				resp.slabs = append(resp.slabs, ev.Slab)
			}
			gotHeaders = true
			if ev.EndStream {
				return nil
			}
		case conn.EventData:
			resp.BytesReceived += int64(len(ev.Data))
			if req.WantBody && len(ev.Data) > 0 {
				resp.Body = append(resp.Body, ev.Data...)
			}
			if ev.EndStream {
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
		}
	}
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
