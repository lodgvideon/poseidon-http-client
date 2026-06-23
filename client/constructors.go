package client

import (
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

func buildClient(base ClientOptions, opts []Option) (*Client, error) {
	for _, opt := range opts {
		opt(&base)
	}
	return NewClient(base)
}
