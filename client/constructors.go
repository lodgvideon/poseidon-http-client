package client

import (
	"crypto/tls"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Focused constructors. ClientOptions is a flat struct with an implicit
// cross-field validity matrix (Addr required for single-conn/pool but must be
// empty for managed; Pool required iff TransportPool; Resolver required iff
// managed). These constructors encode the valid combinations in their
// signatures, so the common setups are correct by construction and the invalid
// ones are unrepresentable — no need to learn the matrix from NewClient's body.
// NewClient(ClientOptions{...}) remains available for full control.

// Option tweaks a ClientOptions built by one of the focused constructors.
// Options are applied in order, after the constructor sets the base fields.
type Option func(*ClientOptions)

// WithHooks sets lifecycle callbacks (replaceable later via Client.SetHooks).
func WithHooks(h *Hooks) Option { return func(o *ClientOptions) { o.Hooks = h } }

// WithPushHandler enables server push and routes PUSH_PROMISE responses to h.
func WithPushHandler(h PushHandler) Option { return func(o *ClientOptions) { o.PushHandler = h } }

// WithDefaultScheme sets the :scheme used when Request.Scheme is empty
// ("https" by default; pass "http" for H2C).
func WithDefaultScheme(s string) Option { return func(o *ClientOptions) { o.DefaultScheme = s } }

// WithRateLimit caps outgoing QPS via a token bucket. burst<=0 → perSecond.
func WithRateLimit(perSecond, burst float64) Option {
	return func(o *ClientOptions) { o.RateLimitPerSecond = perSecond; o.RateLimitBurst = burst }
}

// WithMaxResponseBodySize caps raw response bytes (0 → default 32 MiB).
func WithMaxResponseBodySize(n int64) Option {
	return func(o *ClientOptions) { o.MaxResponseBodySize = n }
}

// WithMaxDecompressedSize caps decompressed body bytes (0 → default 10 MiB).
func WithMaxDecompressedSize(n int64) Option {
	return func(o *ClientOptions) { o.MaxDecompressedSize = n }
}

// WithDialBackoff suppresses redials within d after a failed dial
// (TransportSingleConn).
func WithDialBackoff(d time.Duration) Option { return func(o *ClientOptions) { o.DialBackoff = d } }

// WithSelector overrides the managed-transport address selection strategy
// (default RoundRobin). No effect on single-conn/pool clients.
func WithSelector(s Selector) Option { return func(o *ClientOptions) { o.Selector = s } }

// WithDrainMode sets the managed-transport sub-pool drain behavior when the
// resolver drops an address. No effect on single-conn/pool clients.
func WithDrainMode(d DrainMode) Option { return func(o *ClientOptions) { o.DrainMode = d } }

// WithConnOptions mutates the underlying conn.ConnOptions in place (Settings,
// KeepaliveInterval, EnablePush, …) without clobbering the dialer the
// constructor already set.
//
//	c, err := client.NewSingleConnClient(addr, dialer,
//	    client.WithConnOptions(func(co *conn.ConnOptions) {
//	        co.KeepaliveInterval = 30 * time.Second
//	    }))
func WithConnOptions(fn func(*conn.ConnOptions)) Option {
	return func(o *ClientOptions) { fn(&o.ConnOpts) }
}

// NewSingleConnClient builds a single-connection client (the default
// transport): one HTTP/2 conn with auto-redial, lazy-dialed on first request.
func NewSingleConnClient(addr string, dialer conn.Dialer, opts ...Option) (*Client, error) {
	return buildClient(ClientOptions{
		Addr:      addr,
		Transport: TransportSingleConn,
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
	}, opts)
}

// NewPoolClient builds a pooled client: up to pool.MaxConnsPerHost connections
// to one addr, each multiplexing up to pool.MaxStreamsPerConn streams, with
// idle eviction and health checks.
func NewPoolClient(addr string, dialer conn.Dialer, pool PoolOptions, opts ...Option) (*Client, error) {
	p := pool // copy so the caller's value is not retained/aliased
	return buildClient(ClientOptions{
		Addr:      addr,
		Transport: TransportPool,
		Pool:      &p,
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
	}, opts)
}

// NewH3Client builds a buffered HTTP/3 client: one QUIC connection to addr,
// lazy-dialled on first request via http3.Dial. tlsConfig should set ServerName;
// the "h3" ALPN token and TLS 1.3 minimum are applied automatically. Both the
// buffered path (Client.Do) and streaming response bodies (Client.DoStream) are
// supported; for request multiplexing across several QUIC connections use
// NewH3PoolClient, and for BBR congestion control set
// ClientOptions.H3ConnOptions via NewClient.
func NewH3Client(addr string, tlsConfig *tls.Config, opts ...Option) (*Client, error) {
	return buildClient(ClientOptions{
		Addr:      addr,
		Transport: TransportH3,
		TLSConfig: tlsConfig,
	}, opts)
}

// NewH3PoolClient builds a pooled HTTP/3 client: up to pool.MaxConnsPerHost QUIC
// connections to one addr, each carrying up to pool.MaxStreamsPerConn concurrent
// streams, with dead-conn eviction and idle/health-check sweeps. Streams are
// distributed across the connections; a new QUIC conn is dialled when the live
// ones are all at their MaxStreamsPerConn cap (up to MaxConnsPerHost). tlsConfig
// should set ServerName; the "h3" ALPN token and TLS 1.3 minimum are applied
// automatically. The per-conn cap is a pool policy — the peer's QUIC
// initial_max_streams_bidi is enforced underneath by the http3/quic layer's
// stream-credit backpressure.
func NewH3PoolClient(addr string, tlsConfig *tls.Config, pool PoolOptions, opts ...Option) (*Client, error) {
	p := pool // copy so the caller's value is not retained/aliased
	return buildClient(ClientOptions{
		Addr:      addr,
		Transport: TransportH3Pool,
		Pool:      &p,
		TLSConfig: tlsConfig,
	}, opts)
}

// NewH1Client builds a single-connection HTTP/1.1 client: one connection to addr,
// lazy-dialed on first request, with auto-redial. Requests are serialized — HTTP/1.1
// has no multiplexing and pipelining is deliberately unsupported, so a second
// concurrent request waits for the first to finish. For concurrent load use
// NewH1PoolClient, where MaxConnsPerHost sets the concurrency.
//
// The dialer must not offer or assert ALPN "h2": use a plain TCP dialer
// (&conn.PlaintextDialer{}), or a TLS dialer whose Config.NextProtos contains only
// "http/1.1". A dialer asserting "h2" fails the handshake against an HTTP/1.1-only
// server; for automatic negotiation use TransportALPN via NewClient instead.
func NewH1Client(addr string, dialer conn.Dialer, opts ...Option) (*Client, error) {
	return buildClient(ClientOptions{
		Addr:      addr,
		Transport: TransportH1SingleConn,
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
	}, opts)
}

// NewH1PoolClient builds a pooled HTTP/1.1 client: up to pool.MaxConnsPerHost
// connections to one addr, with keep-alive reuse, idle eviction, and health-check
// sweeps.
//
// Unlike the HTTP/2 and HTTP/3 pools this is an exclusive-checkout pool. HTTP/1.1
// carries exactly one request/response exchange per connection at a time, so:
//
//   - pool.MaxConnsPerHost IS the request concurrency — it is the only knob that
//     matters here, and the pool never exceeds it.
//   - pool.MaxStreamsPerConn does NOT apply to HTTP/1.1 and is ignored. (PoolOptions
//     is shared with the H2/H3 pools; the per-conn cap here is always 1.)
//   - A request arriving when every connection is busy blocks until one frees or
//     its ctx is done — it is never serialized onto a busy connection.
//
// Connections are reused after each exchange and discarded when the response says
// the connection will not persist ("Connection: close", HTTP/1.0 without
// keep-alive), on any exchange error, or when the peer closed the socket.
//
// The dialer must not offer or assert ALPN "h2" — see NewH1Client.
func NewH1PoolClient(addr string, dialer conn.Dialer, pool PoolOptions, opts ...Option) (*Client, error) {
	p := pool // copy so the caller's value is not retained/aliased
	return buildClient(ClientOptions{
		Addr:      addr,
		Transport: TransportH1Pool,
		Pool:      &p,
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
	}, opts)
}

// NewManagedH1Client builds a managed multi-address HTTP/1.1 client: the resolver
// discovers backends, a selector (RoundRobin by default) picks one per request, and
// a per-address exclusive-checkout HTTP/1.1 sub-pool fans out. No Addr — the
// resolver owns addressing. Tune the per-address sub-pools (notably
// MaxConnsPerHost, which sets per-address concurrency) with the Pool field via
// NewClient; MaxStreamsPerConn does not apply to HTTP/1.1.
//
// The dialer must not offer or assert ALPN "h2" — see NewH1Client.
func NewManagedH1Client(resolver Resolver, dialer conn.Dialer, opts ...Option) (*Client, error) {
	return buildClient(ClientOptions{
		Transport: TransportH1Managed,
		Resolver:  resolver,
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
	}, opts)
}

// NewManagedClient builds a managed multi-address client: resolver discovers
// backends, a selector (RoundRobin by default) picks one per request, and a
// per-address sub-pool fans out. No Addr — the resolver owns addressing.
func NewManagedClient(resolver Resolver, dialer conn.Dialer, opts ...Option) (*Client, error) {
	return buildClient(ClientOptions{
		Transport: TransportManaged,
		Resolver:  resolver,
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
	}, opts)
}

// NewManagedH3Client builds a managed multi-address HTTP/3 client: the resolver
// discovers backends, a selector (RoundRobin by default) picks one per request,
// and a per-address HTTP/3 sub-pool fans out over QUIC. No Addr — the resolver
// owns addressing. tlsConfig should set ServerName; the "h3" ALPN token and TLS
// 1.3 minimum are applied automatically. Pass WithConnOptions is not applicable to
// HTTP/3; use the Pool option via NewClient for per-sub-pool tuning, or the
// WithSelector / WithDrainMode options here.
func NewManagedH3Client(resolver Resolver, tlsConfig *tls.Config, opts ...Option) (*Client, error) {
	return buildClient(ClientOptions{
		Transport: TransportH3Managed,
		Resolver:  resolver,
		TLSConfig: tlsConfig,
	}, opts)
}

func buildClient(base ClientOptions, opts []Option) (*Client, error) {
	for _, opt := range opts {
		opt(&base)
	}
	return NewClient(base)
}
