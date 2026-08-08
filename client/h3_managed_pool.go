// Package client — h3ManagedPool: per-address HTTP/3 sub-pool fan-out driven by a
// Resolver and Selector. The HTTP/3 analogue of managedPool; it reuses the
// protocol-agnostic Resolver, Selector, DrainMode, Address, and isDialOnlyErr and
// differs only in that each sub-pool is an *h3Pool of QUIC connections.
package client

import (
	"context"
	"crypto/tls"
	"sync/atomic"
)

// h3SubPoolState is the HTTP/3 instantiation of the core's per-address record.
// An alias, not a wrapper: the pinned behaviour tests index mp.subPools and read
// these fields directly, and an alias keeps every one of them compiling untouched.
type h3SubPoolState = coreSubPool[*h3Pool, *h3ManagedConn]

// h3ManagedPool fans Acquire across per-address HTTP/3 sub-pools driven by a
// Resolver and Selector. Goroutine-safe.
//
// An ALIAS of the shared core (managed_core.go). The protocol-specific parts are
// the three closures the constructor supplies: which sub-pool to build, how to
// read the connection out of its managed-conn record, and the release shape.
type h3ManagedPool = managedCore[*h3Pool, *h3ManagedConn, h3Client, func()]

// newH3ManagedPool constructs an h3ManagedPool and starts its Watch/ticker
// goroutine. It performs an initial Resolve to surface hard errors early.
func newH3ManagedPool(r Resolver, s Selector, dm DrainMode, tlsConfig *tls.Config, po PoolOptions, dialFn func(context.Context, string, *tls.Config) (h3Client, error), hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*h3ManagedPool, error) {
	mp, err := buildH3ManagedPool(r, s, dm, tlsConfig, po, dialFn, hooksRef, metrics)
	if err != nil {
		return nil, err
	}
	go mp.run()
	return mp, nil
}

// buildH3ManagedPool constructs and initialises an h3ManagedPool without starting
// its background goroutine. Tests that need to configure fields (e.g. tickerPeriod)
// before the goroutine reads them call this and start the goroutine themselves.
func buildH3ManagedPool(r Resolver, s Selector, dm DrainMode, tlsConfig *tls.Config, po PoolOptions, dialFn func(context.Context, string, *tls.Config) (h3Client, error), hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*h3ManagedPool, error) {
	if s == nil {
		s = RoundRobin()
	}
	if dialFn == nil {
		dialFn = h3DialFn
	}
	if metrics == nil {
		metrics = &Metrics{}
	}
	mp := &h3ManagedPool{
		resolver:  r,
		selector:  s,
		drainMode: dm,
		poolOpts:  po,
		hooksRef:  hooksRef,
		metrics:   metrics,
		subPools:  make(map[string]*h3SubPoolState),
		closed:    make(chan struct{}),

		// The three measured differences, as closures rather than branches. metrics
		// is captured deliberately: every sub-pool of one managed pool must share
		// the SAME *Metrics, and newH3Pool defaults a nil one to its own fresh
		// struct — which would silently under-count Client.Metrics() with the whole
		// suite green.
		newSub: func(key string) *h3Pool {
			return newH3Pool(key, tlsConfig, po, dialFn, hooksRef, metrics)
		},
		connOf: func(mc *h3ManagedConn) h3Client { return mc.cl },
		mkRelease: func(p *h3Pool, mc *h3ManagedConn) func() {
			return func() { p.release(mc) }
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addrs, err := r.Resolve(ctx)
	if err != nil && len(addrs) == 0 {
		return nil, err
	}
	mp.addrs = addrs
	return mp, nil
}
