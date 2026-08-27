// Package client — h1ManagedPool: per-address HTTP/1.1 sub-pool fan-out driven by
// a Resolver and Selector. The HTTP/1.1 analogue of managedPool and h3ManagedPool;
// it reuses the protocol-agnostic Resolver, Selector, DrainMode, Address, and
// isDialOnlyErr, and differs only in that each sub-pool is an *h1Pool of
// exclusive-checkout HTTP/1.1 connections (see h1_pool.go) and that release
// carries the keep-alive decision.
package client

import (
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http1"
	"github.com/lodgvideon/poseidon-http-client/internal/poolcore"
)

// h1ManagedPool fans Acquire across per-address HTTP/1.1 sub-pools driven by a
// Resolver and Selector. Goroutine-safe.
//
// An ALIAS of the shared core (managed_core.go). Note the release shape: this is
// the protocol whose checkout is EXCLUSIVE, so its release carries keepAlive,
// while H2 and H3 multiplex and theirs takes nothing. That difference is the R
// type argument and mkRelease closure, not a branch inside the core.
//
// watchDrain is now shared with H2 and H3, and its InFlightStreams == 0 predicate
// means something different here — exclusive exchanges rather than multiplexed
// streams. That stays correct because Stats() dispatches through P to h1Pool's
// own h1SumActive; the predicate asks 'nothing in flight' in each protocol's own
// terms. What DID change is that the drain poll schedule now has one home for
// three protocols: retuning it for one retimes sub-pool teardown for all three.
type h1ManagedPool = managedCore[*h1Pool, *h1ManagedConn, *http1.Conn, func(keepAlive bool)]

// newH1ManagedPool constructs an h1ManagedPool and starts its Watch/ticker
// goroutine. It performs an initial Resolve to surface hard errors early.
func newH1ManagedPool(r Resolver, s Selector, dm DrainMode, dialer conn.Dialer, po PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*h1ManagedPool, error) {
	mp, err := buildH1ManagedPool(r, s, dm, dialer, po, hooksRef, metrics)
	if err != nil {
		return nil, err
	}
	go mp.Run()
	return mp, nil
}

// buildH1ManagedPool constructs and initialises an h1ManagedPool without starting
// its background goroutine. Tests that need to configure fields (e.g.
// tickerPeriod) before the goroutine reads them call this and start it themselves.
func buildH1ManagedPool(r Resolver, s Selector, dm DrainMode, dialer conn.Dialer, po PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*h1ManagedPool, error) {
	return poolcore.NewCore(poolcore.CoreConfig[*h1Pool, *h1ManagedConn, *http1.Conn, func(bool)]{
		Resolver: r, Selector: s, DrainMode: dm, PoolOpts: po,
		Obs: observerFor(hooksRef), Rec: recorderFor(metrics),

		// The three measured differences, as closures rather than branches.
		// metrics is captured deliberately: newH1Pool defaults a nil *Metrics to
		// a fresh struct of its own, so letting each sub-pool default
		// independently would under-count Client.Metrics() with the whole suite
		// green.
		NewSub: func(key string) *h1Pool {
			return newH1Pool(key, dialer, po, hooksRef, metrics)
		},
		ConnOf: func(mc *h1ManagedConn) *http1.Conn { return mc.c },
		MkRelease: func(p *h1Pool, mc *h1ManagedConn) func(bool) {
			return func(keepAlive bool) { p.release(mc, keepAlive) }
		},
	})
}
