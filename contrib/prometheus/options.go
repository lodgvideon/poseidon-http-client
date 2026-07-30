package prometheus

import prom "github.com/prometheus/client_golang/prometheus"

// DefaultNamespace prefixes every metric name unless [WithNamespace]
// overrides it.
const DefaultNamespace = "poseidon"

// HookSubsystem is the subsystem for the per-request series produced by
// [HookMetrics]. It keeps them in their own name space
// (poseidon_http_*) so they cannot collide with the aggregate series
// [Collector] publishes (poseidon_*).
const HookSubsystem = "http"

// options holds settings shared by Collector and HookMetrics.
type options struct {
	namespace       string
	constLabels     prom.Labels
	durationBuckets []float64
}

// Option customises a [Collector] or [HookMetrics].
type Option func(*options)

// WithNamespace overrides [DefaultNamespace]. An empty string leaves the
// metric names unprefixed.
func WithNamespace(ns string) Option {
	return func(o *options) { o.namespace = ns }
}

// WithConstLabels attaches labels to every metric — useful to tag a
// client instance ("target", "region", …) when one process runs several.
func WithConstLabels(l prom.Labels) Option {
	return func(o *options) { o.constLabels = l }
}

// WithDurationBuckets sets the bucket boundaries, in seconds, of the
// [HookMetrics] duration histograms. It has no effect on [Collector],
// whose boundaries are fixed by the client's log2 histogram layout.
// An empty or nil slice keeps the default, [prom.DefBuckets].
func WithDurationBuckets(b []float64) Option {
	return func(o *options) { o.durationBuckets = b }
}

// resolve applies opts over the defaults.
func resolve(opts []Option) options {
	o := options{
		namespace:       DefaultNamespace,
		durationBuckets: prom.DefBuckets,
	}
	for _, fn := range opts {
		fn(&o)
	}
	if len(o.durationBuckets) == 0 {
		o.durationBuckets = prom.DefBuckets
	}
	return o
}
