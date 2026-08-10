package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
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
	"github.com/lodgvideon/poseidon-http-client/quic"
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
	// pipelining). ConnOpts.Dialer must NOT assert ALPN "h2" — use
	// &conn.H1TLSDialer{} over TLS or &conn.PlaintextDialer{} in cleartext.
	// A dialer asserting "h2" (conn.TLSDialer) is rejected by NewClient with
	// ErrALPNProtocolMismatch, and a connection that negotiates any protocol
	// other than "http/1.1" is refused at dial time with the same error.
	TransportH1SingleConn

	// TransportALPN dials with conn.FlexDialer (offers "h2" and "http/1.1")
	// and permanently routes to the protocol negotiated on the first
	// connection. For servers that speak H2 the behavior is identical to
	// TransportSingleConn; for servers that only speak HTTP/1.1 it falls
	// back automatically. ConnOpts.Dialer should be *conn.FlexDialer.
	TransportALPN

	// TransportH3 speaks HTTP/3 over QUIC using a single lazy-dialled
	// *http3.Client. It owns UDP/QUIC dialing via http3.Dial, so it requires
	// ClientOptions.TLSConfig (with ServerName) and Addr rather than
	// ConnOpts.Dialer. Both the buffered request path (Client.Do) and the
	// streaming paths (Client.DoStream / Do with BodyMode=BodyStream) are
	// supported.
	TransportH3

	// TransportH3Pool pools up to Pool.MaxConnsPerHost QUIC connections to one
	// Addr, distributing streams across them (Pool.MaxStreamsPerConn per conn)
	// and evicting dead conns. Like TransportH3 it owns QUIC dialing and requires
	// ClientOptions.TLSConfig (with ServerName) and Addr; ClientOptions.Pool is
	// required.
	TransportH3Pool

	// TransportH3Managed is the HTTP/3 analogue of TransportManaged: a Resolver
	// discovers backends, a Selector (RoundRobin by default) picks one per
	// request, and a per-address HTTP/3 sub-pool fans out. Requires
	// ClientOptions.Resolver and ClientOptions.TLSConfig; Addr must be empty.
	TransportH3Managed

	// TransportH1Pool pools up to Pool.MaxConnsPerHost HTTP/1.1 connections to one
	// Addr. Unlike the H2/H3 pools this is an exclusive-checkout pool: HTTP/1.1
	// carries one exchange per connection at a time, so MaxConnsPerHost IS the
	// request concurrency and Pool.MaxStreamsPerConn does not apply. A request
	// that finds every connection busy blocks until one frees or ctx is done.
	// Connections are kept alive and reused, and discarded when the response says
	// the connection will not persist. ClientOptions.Pool is required, and
	// ConnOpts.Dialer must NOT assert ALPN "h2" — use &conn.H1TLSDialer{} over
	// TLS or &conn.PlaintextDialer{} in cleartext (see TransportH1SingleConn).
	//
	// New TransportKinds MUST be appended here, at the end of the block: inserting
	// one mid-block silently renumbers every kind below it.
	TransportH1Pool

	// TransportH1Managed is the HTTP/1.1 analogue of TransportManaged: a Resolver
	// discovers backends, a Selector (RoundRobin by default) picks one per
	// request, and a per-address exclusive-checkout HTTP/1.1 sub-pool fans out.
	// Requires ClientOptions.Resolver; Addr must be empty. As with
	// TransportH1Pool, ConnOpts.Dialer must not assert ALPN "h2".
	TransportH1Managed
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
	// must be non-nil for every transport except TransportH3, which owns
	// its own QUIC dialing.
	ConnOpts conn.ConnOptions

	// TLSConfig is the TLS configuration used by TransportH3 (HTTP/3 over
	// QUIC). It should set ServerName; the "h3" ALPN token and TLS 1.3
	// minimum are applied automatically by the http3 dialer. Required for
	// TransportH3 and ignored by every other transport (which dial via
	// ConnOpts.Dialer).
	TLSConfig *tls.Config

	// H3ConnOptions are forwarded to every QUIC connection dialled by the
	// HTTP/3 transports (TransportH3, TransportH3Pool, TransportH3Managed) —
	// e.g. quic.WithCongestionControl(quic.CCBBR) to opt into BBR congestion
	// control. Ignored by the H1/H2 transports, which do not dial QUIC.
	H3ConnOptions []quic.ConnOption

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

	// PushHandler is invoked when the server sends a PUSH_PROMISE frame
	// (RFC 7540 §8.2). When non-nil, ConnOpts.EnablePush is automatically
	// set to true at client construction so the peer knows push is allowed.
	//
	// The handler runs in a dedicated goroutine. promisedHeaders are the
	// request headers the server promises to fulfil (decoded from
	// PUSH_PROMISE). resp is the fully drained pushed response — Body is
	// always populated regardless of Request.BodyMode. err is non-nil if the
	// push failed (reset, connection closed, etc.).
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
	// Managed transports let the Resolver own addressing, so they must not carry
	// an Addr; every other transport requires one.
	if !isManagedTransport(opts.Transport) {
		if opts.Addr == "" || containsAnyWhitespace(opts.Addr) {
			return nil, fmt.Errorf("client: ClientOptions.Addr must be a non-empty host:port without whitespace")
		}
	}
	// HTTP/3 transports own their own QUIC dialing and need a *tls.Config, not a
	// conn.Dialer — carve them out of the Dialer-required check and require the
	// TLS config instead.
	if isH3Transport(opts.Transport) {
		if opts.TLSConfig == nil {
			return nil, fmt.Errorf("client: ClientOptions.TLSConfig is required for HTTP/3 transports")
		}
	} else if opts.ConnOpts.Dialer == nil {
		return nil, fmt.Errorf("client: ClientOptions.ConnOpts.Dialer is required")
	} else if err := validateDialerALPN(opts.Transport, opts.ConnOpts.Dialer); err != nil {
		return nil, err
	}
	if opts.PushHandler != nil {
		opts.ConnOpts.EnablePush = true
	}
	if opts.ConnOpts.StreamEventBuffer <= 0 {
		opts.ConnOpts.StreamEventBuffer = defaultStreamEventBuffer(opts.ConnOpts.Settings.MaxFrameSize)
	}
	if err := validateTransportOptions(opts); err != nil {
		return nil, err
	}
	metrics := &Metrics{}
	hooksPtr := new(atomic.Pointer[Hooks])
	hooksPtr.Store(opts.Hooks)
	tr, err := buildTransport(opts, hooksPtr, metrics)
	if err != nil {
		return nil, err
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

// validateTransportOptions checks the Pool/Resolver/Addr cross-field matrix per
// transport kind. Split out of NewClient to keep it under the gocyclo budget.
func validateTransportOptions(opts ClientOptions) error {
	switch opts.Transport {
	case TransportSingleConn, TransportH1SingleConn, TransportALPN, TransportH3:
		if opts.Pool != nil {
			return fmt.Errorf("%w: Pool must be nil for this transport kind", ErrInvalidPoolOptions)
		}
	case TransportPool:
		if opts.Pool == nil {
			return fmt.Errorf("%w: Pool is required for TransportPool", ErrInvalidPoolOptions)
		}
	case TransportH3Pool:
		if opts.Pool == nil {
			return fmt.Errorf("%w: Pool is required for TransportH3Pool", ErrInvalidPoolOptions)
		}
	case TransportH1Pool:
		if opts.Pool == nil {
			return fmt.Errorf("%w: Pool is required for TransportH1Pool", ErrInvalidPoolOptions)
		}
	case TransportManaged, TransportH3Managed, TransportH1Managed:
		if opts.Resolver == nil {
			return fmt.Errorf("%w: Resolver is required for this managed transport", ErrInvalidOptions)
		}
		if opts.Addr != "" {
			return fmt.Errorf("%w: Addr must be empty for a managed transport (Resolver owns addressing)", ErrInvalidOptions)
		}
	default:
		return fmt.Errorf("%w: %d", ErrInvalidTransportKind, int(opts.Transport))
	}
	return nil
}

// transportALPN returns the ALPN protocol kind speaks over TLS, or "" when the
// pairing is not fixed: TransportALPN routes to whichever protocol the server
// selects, and the HTTP/3 transports do not dial through a conn.Dialer at all.
func transportALPN(kind TransportKind) string {
	switch kind {
	case TransportSingleConn, TransportPool, TransportManaged:
		return "h2"
	case TransportH1SingleConn, TransportH1Pool, TransportH1Managed:
		return "http/1.1"
	default:
		return ""
	}
}

// validateDialerALPN rejects a dialer whose ALPN assertion the transport cannot
// use. Pairing conn.TLSDialer (asserts "h2") with an HTTP/1.1 transport dials
// successfully and then fails every exchange with "read status line: EOF" — the
// peer framed the connection as HTTP/2 — so the pairing is refused up front.
// Dialers that assert nothing (conn.FlexDialer, conn.PlaintextDialer, anything
// not implementing conn.ALPNAsserter) are left alone: the negotiated protocol is
// re-checked at dial time by the HTTP/1.1 transports.
func validateDialerALPN(kind TransportKind, d conn.Dialer) error {
	a, ok := d.(conn.ALPNAsserter)
	if !ok {
		return nil
	}
	asserted := a.AssertsALPN()
	want := transportALPN(kind)
	if asserted == "" || want == "" || asserted == want {
		return nil
	}
	return fmt.Errorf("%w: dialer asserts %q but this transport speaks %q", ErrALPNProtocolMismatch, asserted, want)
}

// isH3Transport reports whether kind speaks HTTP/3 over QUIC and therefore dials
// via http3.Dial (needing a *tls.Config) instead of a conn.Dialer.
//
// There is deliberately no isH1Transport analogue: the HTTP/1.1 transports dial
// through ConnOpts.Dialer exactly like the HTTP/2 ones, so they need no carve-out
// from the Dialer-required check.
func isH3Transport(kind TransportKind) bool {
	switch kind {
	case TransportH3, TransportH3Pool, TransportH3Managed:
		return true
	default:
		return false
	}
}

// isManagedTransport reports whether kind discovers its addresses through a
// Resolver rather than a fixed ClientOptions.Addr.
func isManagedTransport(kind TransportKind) bool {
	switch kind {
	case TransportManaged, TransportH3Managed, TransportH1Managed:
		return true
	default:
		return false
	}
}

// buildTransport constructs the concrete transport for opts.Transport. opts has
// already been validated by NewClient, so the only error path is the managed
// pool's own constructor. Kept separate from NewClient so the latter stays under
// the gocyclo budget.
func buildTransport(opts ClientOptions, hooksPtr *atomic.Pointer[Hooks], metrics *Metrics) (transport, error) {
	switch opts.Transport {
	case TransportSingleConn:
		return &singleConn{
			addr:     opts.Addr,
			connOpts: opts.ConnOpts,
			backoff:  opts.DialBackoff,
			hooksRef: hooksPtr,
			metrics:  metrics,
		}, nil
	case TransportPool:
		return newPoolTransport(opts.Addr, opts.ConnOpts, *opts.Pool, hooksPtr, metrics), nil
	case TransportManaged:
		po := PoolOptions{}
		if opts.Pool != nil {
			po = *opts.Pool
		}
		mp, err := newManagedPool(opts.Resolver, opts.Selector, opts.DrainMode, opts.ConnOpts, po, hooksPtr, metrics)
		if err != nil {
			return nil, err
		}
		return &managedTransport{mp: mp}, nil
	case TransportH1SingleConn:
		return &h1singleConn{
			addr:     opts.Addr,
			dialer:   opts.ConnOpts.Dialer,
			backoff:  opts.DialBackoff,
			hooksRef: hooksPtr,
			metrics:  metrics,
		}, nil
	case TransportALPN:
		return &alpnSingleConn{
			addr:     opts.Addr,
			connOpts: opts.ConnOpts,
			backoff:  opts.DialBackoff,
			hooksRef: hooksPtr,
			metrics:  metrics,
		}, nil
	case TransportH3:
		return &singleH3Conn{
			addr:      opts.Addr,
			tlsConfig: opts.TLSConfig,
			backoff:   opts.DialBackoff,
			dialFn:    makeH3DialFn(opts.H3ConnOptions),
			hooksRef:  hooksPtr,
			metrics:   metrics,
		}, nil
	case TransportH3Pool:
		return &h3PoolTransport{
			p: newH3Pool(opts.Addr, opts.TLSConfig, *opts.Pool, makeH3DialFn(opts.H3ConnOptions), hooksPtr, metrics),
		}, nil
	case TransportH3Managed:
		po := PoolOptions{}
		if opts.Pool != nil {
			po = *opts.Pool
		}
		mp, err := newH3ManagedPool(opts.Resolver, opts.Selector, opts.DrainMode, opts.TLSConfig, po, makeH3DialFn(opts.H3ConnOptions), hooksPtr, metrics)
		if err != nil {
			return nil, err
		}
		return &h3ManagedTransport{mp: mp}, nil
	case TransportH1Pool:
		return &h1PoolTransport{
			p: newH1Pool(opts.Addr, opts.ConnOpts.Dialer, *opts.Pool, hooksPtr, metrics),
		}, nil
	case TransportH1Managed:
		po := PoolOptions{}
		if opts.Pool != nil {
			po = *opts.Pool
		}
		mp, err := newH1ManagedPool(opts.Resolver, opts.Selector, opts.DrainMode, opts.ConnOpts.Dialer, po, hooksPtr, metrics)
		if err != nil {
			return nil, err
		}
		return &h1ManagedTransport{mp: mp}, nil
	}
	return nil, fmt.Errorf("%w: %d", ErrInvalidTransportKind, int(opts.Transport))
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
	case *h3PoolTransport:
		return t.p.Stats()
	case *h3ManagedTransport:
		return t.mp.stats()
	case *h1PoolTransport:
		return t.p.Stats()
	case *h1ManagedTransport:
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
		if status >= 200 && status < 300 {
			c.metrics.Counters.Responses2xx.Add(1)
		} else {
			c.metrics.Counters.ResponsesNon2xx.Add(1)
		}
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
	// Encode the body before the hooks fire, so BytesSent below reports what
	// actually goes on the wire. Released once the body has been written, which
	// c.do has done by the time it returns on every BodyMode.
	req, release, err := prepareCompressedRequest(req)
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}
	authority := req.Authority
	if authority == "" {
		authority = c.authority
	}
	c.observeStart(req, authority)
	start := time.Now()

	err = c.do(ctx, req, resp)

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
// Returns (s, pushLookup, release, sendCut, err). pushLookup is non-nil only
// for H2 transports and is passed to drainResponse to handle server push.
//
// sendCut is non-nil when the upload was cut short by the peer closing the
// stream and that failure was deliberately not treated as fatal (see the RFC
// 9113 §8.1 comment below). It is not an error the caller should return on its
// own — the response may well be complete — but if the response path ALSO
// fails, sendCut is the earlier and more accurate cause and must be preferred:
// the upload failing is what set everything after it in motion.
//
// It was introduced for a sharper reason that no longer applies. conn used to
// shed an unbuffered response with a forged RST_STREAM(REFUSED_STREAM), which
// the retry classifier read as "the server did not process this request" and
// replayed; keeping sendCut as the outcome hid that code from the classifier.
// conn now sends CANCEL there, so the lie is fixed at its source and this is
// no longer load-bearing for retry — only for reporting the first cause.
//
// Avoids heap-escaping the implicit struct: escape analysis confirmed via
// -gcflags=-m that returning fields by value keeps them on the stack
// (verified 2026-06-15 for the H2 hot path).
func (c *Client) sendRequest(ctx context.Context, req *Request) (s protoStream, pushLookup pushLookuper, release func(), sendCut error, err error) {
	s, pushLookup, release, err = c.tr.openExchange(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Validate dynamic trailers before buildHeaders emits the Trailer
	// announcement. TrailerFunc output never passes through validateRequest, so
	// an injected trailer name would otherwise ride the initial HEADERS frame
	// before writeRequestTrailers could reject it. TrailerFunc is idempotent
	// (see Request.TrailerFunc), so resolving it here is safe.
	if hasTrailers(req) {
		if _, verr := resolveTrailers(req); verr != nil {
			_ = s.Close()
			release()
			return nil, nil, nil, nil, verr
		}
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
		return nil, nil, nil, nil, err
	}
	*sp = (*sp)[:0]
	hdrSlicePool.Put(sp)

	if !endStream || trailers {
		// A failed upload is not decisive on its own. RFC 9113 §8.1 lets a
		// server that has already sent a complete response reset the stream
		// with NO_ERROR to stop an upload it does not need — "Clients MUST NOT
		// discard responses as a result of receiving such a RST_STREAM" — and
		// conn closes the stream on any reset per §5.1, so that benign signal
		// reaches here as ErrStreamClosed on a request the server has answered.
		// Hand the stream back and let the response path decide, recording the
		// failure in sendCut so it can be preferred if that path fails too.
		//
		// Reaching the response path is safe because the routes that produce
		// ErrStreamClosed here all deliver a stream event as well: a peer
		// RST_STREAM (conn.connHandler.OnRSTStream), a GOAWAY teardown, and
		// conn's own event-buffer overflow each enqueue an EventReset or signal
		// one before setting the flag. The two routes that would NOT — a caller
		// Close() and a healthy stream that has already sent END_STREAM — are
		// unreachable from here: this method never closes the stream it is
		// about to return, and it emits END_STREAM exactly once. That is a
		// property of THIS function, not of conn, so it has to be re-checked if
		// the send sequence below ever grows a second END_STREAM or a Close.
		//
		// Any other write failure stays fatal. Nothing arrives to read: those
		// failures come from a dead transport or a cancelled context, not from
		// a peer that answered early — though the frames written before the
		// failure may well have reached the peer, so "nothing was sent" is not
		// the reason.
		if err = writeRequestBody(ctx, s, req, !trailers); err != nil {
			if !errors.Is(err, conn.ErrStreamClosed) {
				_ = s.Close()
				release()
				return nil, nil, nil, nil, err
			}
			sendCut = err
		} else if trailers {
			if err = writeRequestTrailers(ctx, s, req); err != nil {
				if !errors.Is(err, conn.ErrStreamClosed) {
					_ = s.Close()
					release()
					return nil, nil, nil, nil, err
				}
				sendCut = err
			}
		}
	}

	return s, pushLookup, release, sendCut, nil
}

// preferSendCut picks which failure to report when the response path failed on
// a request whose upload had already been cut short. sendCut came first and
// names the real cause; respErr may be conn's own forged
// RST_STREAM(REFUSED_STREAM) from an event-buffer overflow, which the retry
// layer reads as "the server did not process this request" and would replay.
// A nil respErr means the response arrived despite the cut — the §8.1 case
// this all exists for — and nothing is reported.
//
// respErr is folded into the message rather than the chain on purpose: wrapping
// it would put a *StreamResetError back where errors.As can reach it, and the
// retry classifier keys on exactly that. The text keeps the peer's code
// visible for diagnosis without making it decide anything.
func preferSendCut(respErr, sendCut error) error {
	if respErr == nil {
		return nil
	}
	if sendCut == nil {
		return respErr
	}
	// respErr.Error(), not %w or %v on the error itself: this is the one place
	// where keeping it out of the chain is the point, and %v would read as an
	// oversight to errorlint and to the next person.
	return fmt.Errorf("%w (the upload was cut short and the response then failed: %s)", sendCut, respErr.Error())
}

func (c *Client) do(ctx context.Context, req *Request, resp *Response) error {
	s, pushLookup, release, sendCut, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}

	if req.BodyMode == BodyStream {
		if resp == nil {
			_ = s.Close()
			release()
			return fmt.Errorf("client: BodyStream requires a non-nil *Response")
		}
		rs, err := beginRespStream(ctx, s)
		if err != nil {
			_ = s.Close()
			release()
			return preferSendCut(err, sendCut)
		}
		ev, err := recvFinalHeaders(ctx, rs)
		if err != nil {
			_ = rs.Close()
			release()
			return preferSendCut(err, sendCut)
		}
		n, perr := parseStatus(ev.Headers, &resp.Headers)
		if perr != nil {
			_ = rs.Close()
			release()
			return perr
		}
		resp.Status = n
		if ev.Slab != nil {
			resp.slabs = append(resp.slabs, ev.Slab)
		}
		// The reader gets its own cancellable context, not the caller's ctx
		// directly. Close cancels it, which is what unblocks a Read parked in
		// Recv: closing the stream alone does not wake that goroutine, so an
		// abort through the io.ReadCloser left the reader hanging until the
		// caller's own deadline fired.
		bodyCtx, bodyCancel := context.WithCancel(ctx)
		resp.BodyReader = &responseBodyReader{
			ctx:     bodyCtx,
			cancel:  bodyCancel,
			stream:  rs,
			release: release,
			resp:    resp,
			// A response that ended on its HEADERS frame — 204/304, a HEAD, or
			// any status-only reply — has no DATA or trailer event to end the
			// body on, so without this the reader pumps one event too many. It
			// then blocks until ctx (no reset), or reports a benign
			// RST_STREAM(NO_ERROR) as a *StreamResetError (§8.1, the very thing
			// this release fixes on the buffered path). doStream sets the same
			// flag on StreamResponse; this literal was the sibling that did not.
			done: ev.EndStream,
			lg:   armLeakGuard("Response.BodyReader"),
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
	return preferSendCut(err, sendCut)
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
	// See Do: encode before the hooks fire. doStream has written the whole
	// request body by the time it returns, so releasing here is safe even though
	// the response stream stays open.
	req, release, err := prepareCompressedRequest(req)
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}
	sr.reset()
	authority := req.Authority
	if authority == "" {
		authority = c.authority
	}
	c.observeStart(req, authority)
	start := time.Now()

	err = c.doStream(ctx, req, sr)

	var status int
	if err == nil {
		status = sr.Status
	}
	c.observeDone(req, authority, status, int64(len(req.Body)), 0, err, time.Since(start))
	return err
}

// doStream is the inner streaming transport, without hook/metric wrapping.
func (c *Client) doStream(ctx context.Context, req *Request, sr *StreamResponse) error {
	s, _, release, sendCut, err := c.sendRequest(ctx, req)
	if err != nil {
		return err
	}

	rs, err := beginRespStream(ctx, s)
	if err != nil {
		_ = s.Close()
		release()
		return preferSendCut(err, sendCut)
	}
	ev, err := recvFinalHeaders(ctx, rs)
	if err != nil {
		_ = rs.Close()
		release()
		return preferSendCut(err, sendCut)
	}
	n, perr := parseStatus(ev.Headers, &sr.Headers)
	if perr != nil {
		_ = rs.Close()
		release()
		return perr
	}
	sr.Status = n
	if ev.Slab != nil {
		sr.slabs = append(sr.slabs, ev.Slab) // transfer slab ownership
	}
	sr.stream = rs
	sr.release = release
	// Pre-merge the caller's context with an abort Close can fire. conn.Stream
	// Recv parks on the event channel and the stream's own signals, so closing
	// the stream does not wake a reader already blocked in it; this is what
	// does. Building it here rather than per Recv keeps the streaming path
	// allocation-free for a caller that passes this same context back.
	sr.doCtx = ctx
	sr.recvCtx, sr.abortCancel = context.WithCancel(ctx)
	sr.lg = armLeakGuard("StreamResponse")
	if ev.EndStream {
		sr.drained = true
	}
	return nil
}

// beginRespStream selects the incremental streaming reader for a protoStream,
// per protocol. HTTP/2 uses the *conn.Stream directly — it is already a streaming
// stream, and boxing the pointer into respStream costs no allocation. HTTP/3
// switches the buffered h3Exchange over to http3.Client.DoStream. HTTP/1.1 uses
// h1Exchange directly: its Recv already reads one body chunk at a time through
// http1.Exchange.ReadBodyChunk and marks the last one EndStream, which is the
// same Recv → conn.StreamEvent surface the other two present.
//
// h1Exchange used to fall through to ErrStreamingUnsupported on the claim that it
// "buffers whole responses and has no incremental path". That was never true of
// the code: the chunk loop and the pooled DataSlab handover were there from the
// start. Only this dispatch rejected it.
//
// Releasing the connection stays with h1Exchange: its release fires on the
// terminal chunk or on Close, and the release the streaming caller holds is the
// no-op both h1 transports return for exactly this reason, so there is one owner
// either way.
//
// Correcting what this comment said when the case was added: that release is
// sync.Once-guarded only on the POOL transport. On h1singleConn it is a plain
// closure, and calling it twice would double-unlock the in-flight mutex. What
// keeps it to one call there is the e.done contract — Recv sets it on the
// terminal chunk and Close returns early once set — not a structural guard.
func beginRespStream(ctx context.Context, s protoStream) (respStream, error) {
	switch v := s.(type) {
	case conn.StreamRef:
		// A value type, not a pointer: Conn.NewStream hands out a handle to one
		// stream lifetime rather than the pooled struct itself. Keep this an
		// explicit case — collapsing the switch into an s.(respStream) assertion
		// would always succeed, respStream being a strict subset of protoStream,
		// and HTTP/3 would silently take this arm instead of beginStream.
		return v, nil
	case *h3Exchange:
		return v.beginStream(ctx)
	case *h1Exchange:
		return v, nil
	default:
		return nil, ErrStreamingUnsupported
	}
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

// lowerHeaderName returns name with ASCII A–Z folded to lowercase, per RFC 9113
// §8.2.1 ("Field names MUST be converted to lowercase when constructing an HTTP/2
// message"). It returns the original slice unchanged — no allocation — when the
// name is already lowercase, the common case; only a name carrying an uppercase
// letter is copied.
func lowerHeaderName(name []byte) []byte {
	upper := false
	for _, b := range name {
		if b >= 'A' && b <= 'Z' {
			upper = true
			break
		}
	}
	if !upper {
		return name
	}
	out := make([]byte, len(name))
	for i, b := range name {
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		out[i] = b
	}
	return out
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
	// RFC 9113 §8.5: a (non-extended) CONNECT omits :scheme and :path — the tunnel
	// target rides in :authority. An extended CONNECT (RFC 8441, :protocol set)
	// keeps both, so it is not a regularConnect and takes the normal path.
	regularConnect := req.Method == "CONNECT" && req.Protocol == ""
	*sp = (*sp)[:0]
	*sp = append(*sp, conn.HeaderField{Name: hdrMethod, Value: unsafeStringToBytes(req.Method)})
	if !regularConnect {
		*sp = append(*sp, conn.HeaderField{Name: hdrScheme, Value: unsafeStringToBytes(scheme)})
	}
	*sp = append(*sp, conn.HeaderField{Name: hdrAuthority, Value: unsafeStringToBytes(authority)})
	if !regularConnect {
		*sp = append(*sp, conn.HeaderField{Name: hdrPath, Value: unsafeStringToBytes(req.Path)})
	}
	if req.Protocol != "" {
		*sp = append(*sp, conn.HeaderField{Name: hdrProtocol, Value: unsafeStringToBytes(req.Protocol)})
	}
	// A known-length body gets a Content-Length, whichever field carries it.
	//
	// Only the streaming case used to qualify, so a buffered req.Body went out
	// with no Content-Length at all — which made WriteRequest frame it chunked,
	// on a connection whose peer version the client had never observed. RFC 9112
	// §6.1/§6.3 forbid sending Transfer-Encoding unless the client knows the
	// server handles HTTP/1.1, and §6.3 asks for a Content-Length whenever the
	// length is known in advance; a buffered body's length is known by
	// definition. Declaring it also lets WriteBody reconcile the octets it writes
	// against the declaration, so the header is provably exact rather than
	// merely intended.
	managedCL := (req.BodyReader != nil && req.ContentLength > 0) ||
		(req.BodyReader == nil && len(req.Body) > 0 && req.CompressBody == EncodingIdentity)
	for i := range req.Headers {
		// Drop a caller-supplied Content-Length when this call is about to
		// append its own. Appending beside it put TWO disagreeing
		// Content-Length field lines on the wire — the CL.CL desync (RFC 9112
		// §11.2), generated by us, where a front end honouring one value and a
		// back end the other disagree about where the request ends.
		//
		// Replacing rather than refusing matches what compressedHeaders already
		// does for the compressed path, and what Request.CompressBody's doc
		// already promises: "content-length is managed for you and any
		// caller-supplied one is replaced". Request.ContentLength is what
		// actually governs how many body bytes get written, so it is the value
		// that can be true.
		//
		// The comparison folds case because RFC 9110 §5.1 makes field names
		// case-insensitive. #253 fixed this same defect in http1's WriteRequest,
		// where an exact match let the canonical spelling through; this sibling
		// append is one layer up, is shared with the HTTP/2 path, and never got
		// the guard.
		if managedCL && bytes.EqualFold(req.Headers[i].Name, hdrContentLength) {
			continue
		}
		// RFC 9113 §8.2.1: "Field names MUST be converted to lowercase when
		// constructing an HTTP/2 message." The client lowercases the names it
		// synthesizes; a caller-supplied Request.Headers name is folded here — no
		// allocation when it is already lowercase — so an uppercase name is never
		// emitted verbatim as a malformed HTTP/2 field.
		hf := req.Headers[i]
		hf.Name = lowerHeaderName(hf.Name)
		*sp = append(*sp, hf)
	}
	if !req.DisableDecompression && shouldSendAcceptEncoding(req) {
		*sp = append(*sp, conn.HeaderField{Name: hdrAcceptEncoding, Value: acceptEncodingValue})
	}
	if managedCL {
		n := req.ContentLength
		if req.BodyReader == nil {
			n = int64(len(req.Body)) // the buffered body's length is what goes out
		}
		*sp = append(*sp, conn.HeaderField{
			Name:  hdrContentLength,
			Value: []byte(strconv.FormatInt(n, 10)),
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
//
// It validates the resolved fields: no pseudo-headers, names are RFC 9110 §5.6.2
// tokens, values carry no CR/LF/NUL. Static req.Trailers are already checked by
// validateRequest, but TrailerFunc output is dynamic and never passed through
// it, so a name or value carrying an injection byte would otherwise ride the
// trailer HEADERS frame — and, via trailerAnnouncement, the initial HEADERS —
// verbatim (RFC 7540 §8.1.2.6 makes such a message malformed; §10.3 is the
// downgrade-splitting vector). sendRequest calls this before buildHeaders so the
// announcement cannot carry an unvalidated name.
func resolveTrailers(req *Request) ([]conn.HeaderField, error) {
	fields := resolveTrailerFields(req)
	for i := range fields {
		name := fields[i].Name
		if len(name) > 0 && name[0] == ':' {
			return nil, fmt.Errorf("%w: pseudo-header %q in trailer", ErrInvalidRequest, name)
		}
		if !isTokenName(name) {
			return nil, fmt.Errorf("%w: trailer name %q is not a token (RFC 9110 §5.6.2)",
				ErrInvalidRequest, name)
		}
		if hasFieldInjectionByte(fields[i].Value) {
			return nil, fmt.Errorf("%w: trailer %q value carries CR, LF or NUL (request-splitting vector, RFC 7540 §10.3)",
				ErrInvalidRequest, name)
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
func drainResponse(ctx context.Context, pushLookup pushLookuper, s protoStream, req *Request, resp *Response, h PushHandler, maxDecompressed, maxBody int64) error {
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
		case conn.EventInterimHeaders:
			// Informational 1xx (RFC 7540 §8.1). Client.Do surfaces only the
			// final response — matching http1 (ReadResponse skips 1xx) and
			// http3 (h3_transport drops resp.Interim) — so drop it and keep
			// pumping. Raw conn callers still see EventInterimHeaders.
			recycleHeaderSlab(ev.Slab)
		case conn.EventTrailers:
			if req.WantTrailers {
				// Overwrite rather than accumulate: a response has at most one
				// trailer section. conn enforces §8.1's END_STREAM requirement
				// on trailers, so a second block cannot arrive; the [:0] keeps
				// Trailers bounded regardless, matching body.go's handling of
				// the same event. Slabs stay in resp.slabs and are returned as
				// one batch by Response.Reset — never Put twice.
				resp.Trailers = append(resp.Trailers[:0], ev.Headers...)
				if ev.Slab != nil {
					resp.slabs = append(resp.slabs, ev.Slab)
				}
			} else {
				// Trailers unwanted: return the slab instead of dropping it.
				recycleHeaderSlab(ev.Slab)
			}
			if ev.EndStream {
				return nil
			}
		case conn.EventReset:
			return &StreamResetError{Code: ev.RSTCode}
		case conn.EventPushPromise:
			if h != nil && pushLookup != nil && ev.PushStreamID > 0 {
				if ps, ok := pushLookup.LookupStream(ev.PushStreamID); ok {
					// Copy promised headers to decouple from the slab
					// lifetime (slab is returned when parent resp is Reset).
					hdrs := copyHeaders(ev.Headers)
					go drainPushedStream(ctx, pushLookup, h, hdrs, ps, maxDecompressed, maxBody)
				}
			}
		}
	}
}

// recvFinalHeaders reads stream events until the final (non-informational)
// response HEADERS arrives, dropping any 1xx blocks that precede it (RFC 7540
// §8.1). Informational responses are not surfaced by Client.Do / DoStream —
// the same policy http1 (ReadResponse skips 1xx) and http3 (h3_transport drops
// resp.Interim) apply — so all three protocols agree on what the caller sees.
// Raw conn users still observe EventInterimHeaders. The loop is bounded by
// conn's maxInterimResponses, which resets the stream past the cap.
func recvFinalHeaders(ctx context.Context, rs respStream) (conn.StreamEvent, error) {
	for {
		ev, err := rs.Recv(ctx)
		if err != nil {
			return conn.StreamEvent{}, err
		}
		if ev.Type == conn.EventInterimHeaders {
			recycleHeaderSlab(ev.Slab)
			continue
		}
		if ev.Type != conn.EventHeaders {
			return conn.StreamEvent{}, fmt.Errorf("client: expected initial HEADERS, got %s", ev.Type)
		}
		return ev, nil
	}
}

// recycleHeaderSlab returns a header slab to conn's pool. nil-safe. Used for
// header blocks the client layer decodes but does not retain (informational
// 1xx, unwanted trailers) — dropping them would still be correct (sync.Pool
// tolerates buffers that are never Put) but would forfeit the reuse.
func recycleHeaderSlab(slab *[]byte) {
	if slab != nil {
		*slab = (*slab)[:0]
		conn.GetHeaderSlabPool().Put(slab)
	}
}

// handleHeadersEvent processes a single EventHeaders from drainResponse.
// Returns (done=true, nil) when the stream is complete.
func handleHeadersEvent(ev conn.StreamEvent, req *Request, resp *Response, gotHeaders *bool, enc *ContentEncoding) (done bool, err error) {
	if *gotHeaders {
		// Unreachable via conn: a second non-informational block after the
		// final status is classified as trailers (RFC 7540 §8.1). Kept as a
		// guard for other protoStream implementations; the final status set
		// below must win, so never re-parse — just release the slab.
		recycleHeaderSlab(ev.Slab)
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
	if req.BodyMode == BodyBuffer && len(ev.Data) > 0 && !over {
		resp.Body = append(resp.Body, ev.Data...)
	}
	// Payload consumed (copied out, unwanted, or over-limit): return the pooled
	// buffer on every exit path.
	if ev.DataSlab != nil {
		putDataSlab(ev.DataSlab)
	}
	if over {
		return false, fmt.Errorf("%w: received %d bytes, limit %d", ErrBodyTooLarge, resp.BytesReceived, maxBody)
	}
	if ev.EndStream {
		if req.BodyMode == BodyBuffer && enc != EncodingIdentity {
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
func drainPushedStream(ctx context.Context, pushLookup pushLookuper, h PushHandler, promisedHeaders []conn.HeaderField, s conn.StreamRef, maxDecompressed, maxBody int64) {
	pr := &Response{}
	derr := drainResponse(ctx, pushLookup, s, &Request{BodyMode: BodyBuffer}, pr, h, maxDecompressed, maxBody)
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
		out[i].Indexing = in[i].Indexing
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

// streamEventBufferBudget is the memory a single stream may pin in queued
// events while its consumer is behind. It is a byte figure divided by the
// advertised frame size rather than a slot count, because a queued EventData
// holds a pooled DATA buffer that ratchets up to one frame: slots x frame size
// is what the peer can make this client retain, so that product is the thing to
// bound. A caller who raises MaxFrameSize for throughput would otherwise
// multiply the ceiling silently — at 1 MiB frames a flat 64 slots is 64 MiB per
// stream, and 6.4 GiB across a pool of 8 connections at 100 streams each.
const streamEventBufferBudget = 1 << 20

// minStreamEventBuffer keeps a floor for the case the budget cannot express:
// slots are charged per FRAME, not per byte, so a server flushing many small
// chunks exhausts them at almost no memory. Eight 512-byte chunks is 4 KiB and
// fills conn's default of 8.
const minStreamEventBuffer = 16

// maxStreamEventBuffer caps the slot count independently of the byte budget, so
// a small advertised frame size cannot turn the budget into a huge channel.
const maxStreamEventBuffer = 64

// defaultStreamEventBuffer sizes conn's per-stream event channel for the client.
//
// conn's own default is 8, which is a floor with no knowledge of what a response
// looks like. That is the whole of #344: a chunked response with 8 flushed
// chunks delivers 10 events, the channel sheds the stream, and a response the
// server wrote in full becomes an error. grpc has had a computed default since
// it shipped; the client never got the equivalent.
//
// This does not make shedding impossible, and no finite size could: a consumer
// that falls more than a channel behind a flushing server still loses the
// stream, and nothing here distinguishes "momentarily descheduled" from
// "genuinely slower than the peer". Only refunding flow-control window on
// consumption rather than on receipt would, which is a different and much
// larger change — conn refunds at receipt today, so HTTP/2 backpressure never
// throttles a peer to its consumer's pace and this channel is the only bound
// there is.
func defaultStreamEventBuffer(maxFrameSize uint32) int {
	if maxFrameSize == 0 {
		maxFrameSize = 16384 // conn's advertised default
	}
	n := streamEventBufferBudget / int(maxFrameSize)
	if n < minStreamEventBuffer {
		return minStreamEventBuffer
	}
	if n > maxStreamEventBuffer {
		return maxStreamEventBuffer
	}
	return n
}
