// Package client — h3ManagedPool: per-address HTTP/3 sub-pool fan-out driven by a
// Resolver and Selector. The HTTP/3 analogue of managedPool; it reuses the
// protocol-agnostic Resolver, Selector, DrainMode, Address, and isDialOnlyErr and
// differs only in that each sub-pool is an *h3Pool of QUIC connections.
package client

import (
	"context"
	"crypto/tls"
	"github.com/lodgvideon/poseidon-http-client/internal/poolcore"
	"sync/atomic"
)

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
	go mp.Run()
	return mp, nil
}

// buildH3ManagedPool constructs and initialises an h3ManagedPool without starting
// its background goroutine. Tests that need to configure fields (e.g. tickerPeriod)
// before the goroutine reads them call this and start the goroutine themselves.
func buildH3ManagedPool(r Resolver, s Selector, dm DrainMode, tlsConfig *tls.Config, po PoolOptions, dialFn func(context.Context, string, *tls.Config) (h3Client, error), hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*h3ManagedPool, error) {
	if dialFn == nil {
		dialFn = h3DialFn
	}
	return poolcore.NewCore(poolcore.CoreConfig[*h3Pool, *h3ManagedConn, h3Client, func()]{
		Resolver: r, Selector: s, DrainMode: dm, PoolOpts: po,
		Obs: observerFor(hooksRef), Rec: recorderFor(metrics),

		// The three measured differences, as closures rather than branches.
		// metrics is captured deliberately: every sub-pool of one managed pool
		// must share the SAME *Metrics, and newH3Pool defaults a nil one to its
		// own fresh struct — which would silently under-count Client.Metrics()
		// with the whole suite green.
		NewSub: func(key string) *h3Pool {
			return newH3Pool(key, tlsConfig, po, dialFn, hooksRef, metrics)
		},
		ConnOf: func(mc *h3ManagedConn) h3Client { return mc.cl },
		MkRelease: func(p *h3Pool, mc *h3ManagedConn) func() {
			return func() { p.release(mc) }
		},
	})
}
