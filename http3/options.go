package http3

import "github.com/lodgvideon/poseidon-http-client/quic"

// Option configures a Client at construction time. Options are additive and
// applied in order; passing none leaves every default in place, so Dial behaves
// byte-for-byte as it did before options existed.
//
// The shape follows quic.ConnOption, which this package already accepts and
// forwards. One variadic carries both levels rather than two: Go allows a single
// variadic parameter, and a second constructor taking the other kind would be two
// doors onto one object.
type Option func(*clientConfig)

// clientConfig is what the options accumulate into before a Client exists. It is
// unexported: the settable surface is the With* functions, so adding a knob later
// does not widen a struct callers construct by hand.
type clientConfig struct {
	// maxResponseBytes is 0 when unset, which Client.responseByteCap reads as the
	// default. Nothing here resolves it — a resolved value stored in two places is
	// two places to disagree.
	maxResponseBytes uint64
	connOpts         []quic.ConnOption
}

// WithMaxResponseBytes bounds the bytes a single response may retain in memory:
// the per-frame declared-length cap and the cumulative cap on header, body,
// trailer and 1xx payloads held together. Exceeding it fails the request with
// ErrResponseTooLarge.
//
// Zero selects the 128 MiB default rather than forbidding every response, so
// WithMaxResponseBytes(0) is a no-op rather than a way to break a client. Callers
// that want a genuinely tiny cap should say so: 1 is a legal value.
//
// This is the knob a load generator actually needs. Many concurrent large
// responses against one default-configured client is 128 MiB of headroom per
// in-flight request, and the value was a package var until #712 — reachable from
// a test in the same package, and from nowhere else.
//
// The streamed path (DoStream) is unaffected by the cumulative half: a chunk
// handed to the BodyReader is not retained, so it does not count. The per-frame
// cap still applies, since a frame is buffered before it can be handed off.
func WithMaxResponseBytes(n uint64) Option {
	return func(c *clientConfig) { c.maxResponseBytes = n }
}

// WithConnOptions forwards QUIC-level options to the connection Dial creates.
// It replaces the `opts ...quic.ConnOption` parameter Dial used to take, so
//
//	http3.Dial(ctx, addr, cfg, quic.WithCongestionControl(quic.CCBBR))
//
// becomes
//
//	http3.Dial(ctx, addr, cfg, http3.WithConnOptions(quic.WithCongestionControl(quic.CCBBR)))
//
// Repeated calls append rather than replace, so options assembled in separate
// places both take effect.
func WithConnOptions(opts ...quic.ConnOption) Option {
	return func(c *clientConfig) { c.connOpts = append(c.connOpts, opts...) }
}

// apply folds opts into a config. Nil options are skipped so a caller building a
// slice conditionally does not have to filter it.
func apply(opts []Option) clientConfig {
	var cfg clientConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	return cfg
}
