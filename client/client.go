package client

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
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
}

// Client is a high-level HTTP/2 client wrapping a single connection.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	tr        transport
	authority string
	hooksPtr  *atomic.Pointer[Hooks]
	metrics   *Metrics
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
	c := &Client{tr: tr, authority: deriveAuthority(opts.Addr), hooksPtr: hooksPtr, metrics: metrics}
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

// Do issues a synchronous request and returns a fully-buffered Response.
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	start := time.Now()
	authority := req.Authority
	if authority == "" {
		authority = c.authority
	}
	if h := c.hooksPtr.Load(); h != nil && h.OnRequestStart != nil {
		h.OnRequestStart(RequestStartEvent{
			Method: req.Method, Path: req.Path, Authority: authority, Attempt: 0,
		})
	}
	c.metrics.Counters.RequestsStarted.Add(1)

	resp, err := c.do(ctx, req)

	latency := time.Since(start)
	c.metrics.Latency.Request.Observe(latency)
	var status int
	var bytesRecv int64
	if resp != nil {
		status = resp.Status
		bytesRecv = resp.BytesReceived
	}
	if err == nil {
		c.metrics.Counters.RequestsSucceeded.Add(1)
	} else {
		c.metrics.Counters.RequestsErrored.Add(1)
	}
	if h := c.hooksPtr.Load(); h != nil && h.OnRequestComplete != nil {
		h.OnRequestComplete(RequestCompleteEvent{
			Method: req.Method, Path: req.Path, Authority: authority,
			Status: status, Err: err, Latency: latency,
			BytesSent: int64(len(req.Body)), BytesRecv: bytesRecv,
			Attempt: 0,
		})
	}
	return resp, err
}

// do is the inner request transport, without hook/metric wrapping.
func (c *Client) do(ctx context.Context, req *Request) (*Response, error) {
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

	resp, err := drainResponse(ctx, s, req)
	if err != nil {
		_ = s.Close()
	}
	return resp, err
}

// DoStream issues a request and returns once the initial HEADERS frame
// has arrived. The caller pumps StreamResponse.Recv for subsequent
// DATA / trailers / reset events. The caller MUST call
// StreamResponse.Close if it does not drain the stream.
func (c *Client) DoStream(ctx context.Context, req *Request) (*StreamResponse, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	start := time.Now()
	authority := req.Authority
	if authority == "" {
		authority = c.authority
	}
	if h := c.hooksPtr.Load(); h != nil && h.OnRequestStart != nil {
		h.OnRequestStart(RequestStartEvent{
			Method: req.Method, Path: req.Path, Authority: authority, Attempt: 0,
		})
	}
	c.metrics.Counters.RequestsStarted.Add(1)

	sr, err := c.doStream(ctx, req)

	latency := time.Since(start)
	c.metrics.Latency.Request.Observe(latency)
	var status int
	if sr != nil {
		status = sr.Status
	}
	if err == nil {
		c.metrics.Counters.RequestsSucceeded.Add(1)
	} else {
		c.metrics.Counters.RequestsErrored.Add(1)
	}
	if h := c.hooksPtr.Load(); h != nil && h.OnRequestComplete != nil {
		h.OnRequestComplete(RequestCompleteEvent{
			Method: req.Method, Path: req.Path, Authority: authority,
			Status: status, Err: err, Latency: latency,
			BytesSent: int64(len(req.Body)),
			Attempt:   0,
		})
	}
	return sr, err
}

// doStream is the inner streaming transport, without hook/metric wrapping.
func (c *Client) doStream(ctx context.Context, req *Request) (*StreamResponse, error) {
	cn, release, err := c.tr.acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := cn.NewStream(ctx)
	if err != nil {
		release()
		return nil, err
	}

	hdrs := buildHeaders(req, c.authority)
	endStream := len(req.Body) == 0 && req.BodyReader == nil
	if err := s.SendHeaders(ctx, hdrs, endStream); err != nil {
		_ = s.Close()
		release()
		return nil, err
	}
	if !endStream {
		if err := writeRequestBody(ctx, s, req); err != nil {
			_ = s.Close()
			release()
			return nil, err
		}
	}

	ev, err := s.Recv(ctx)
	if err != nil {
		_ = s.Close()
		release()
		return nil, err
	}
	if ev.Type != conn.EventHeaders {
		_ = s.Close()
		release()
		return nil, fmt.Errorf("client: expected initial HEADERS, got %s", ev.Type)
	}
	status, regular, perr := parseStatus(ev.Headers)
	if perr != nil {
		_ = s.Close()
		release()
		return nil, perr
	}
	sr := &StreamResponse{
		Status:  status,
		Headers: regular,
		stream:  s,
		release: release,
	}
	if ev.EndStream {
		sr.drained = true
	}
	return sr, nil
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
			// The conn package now copies header fields per-event, so
			// ev.Headers is owned by the event and safe to retain.
			if gotHeaders {
				// Spurious second HEADERS without trailer flag — peer
				// protocol oddity. Skip.
				if ev.EndStream {
					return &resp, nil
				}
				continue
			}
			status, regular, perr := parseStatus(ev.Headers)
			if perr != nil {
				return nil, perr
			}
			resp.Status = status
			resp.Headers = regular
			gotHeaders = true
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
				resp.Trailers = ev.Headers
			}
			if ev.EndStream {
				return &resp, nil
			}
		case conn.EventReset:
			return nil, &StreamResetError{Code: ev.RSTCode}
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
