// Package client — h1ManagedPool: per-address HTTP/1.1 sub-pool fan-out driven by
// a Resolver and Selector. The HTTP/1.1 analogue of managedPool and h3ManagedPool;
// it reuses the protocol-agnostic Resolver, Selector, DrainMode, Address, and
// isDialOnlyErr, and differs only in that each sub-pool is an *h1Pool of
// exclusive-checkout HTTP/1.1 connections (see h1_pool.go) and that release
// carries the keep-alive decision.
package client

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http1"
	"github.com/lodgvideon/poseidon-http-client/internal/poolcore"
	"github.com/lodgvideon/poseidon-http-client/pool"
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
		NewSub: func(addr Address) *h1Pool {
			return newH1SubPool(addr, dialer, po, hooksRef, metrics)
		},
		ConnOf: func(mc *h1ManagedConn) *http1.Conn { return mc.c },
		MkRelease: func(p *h1Pool, mc *h1ManagedConn) func(bool) {
			return func(keepAlive bool) { p.release(mc, keepAlive) }
		},
	})
}

// newH1SubPool builds the managed sub-pool for resolved: a sub-pool whose
// dial offers the resolver's Address (Attributes included) to a
// pool.AddressDialer, falling back to the plain string Dial otherwise
// (#943). A nil dialer is left alone: pool.DialResolved requires a non-nil
// Dialer, and leaving nil unwrapped keeps the resulting panic at its
// original site (h1Pool.dialOne's p.dialer.Dial call) instead of relocating
// it inside this closure.
//
// The wrap hides any capability interface the caller's dialer implements
// (conn.ALPNAsserter today) behind a plain DialerFunc. Safe because the only
// consumer, client.validateDialerALPN, runs at NewClient time on the
// original dialer — a future capability checked at dial time would need to
// be re-exposed here too.
//
// wrapped, not dialer, is what h1Pool actually dials with. Building the
// wrapped value into its own variable — rather than reassigning dialer and
// closing over it — means the closure always closes over the one dialer
// value that exists for the life of this function: reassign dialer instead
// and a future copy of this pattern that closes over the reassigned
// variable dials itself, an unrecoverable fatal error: stack overflow, not
// a catchable panic.
func newH1SubPool(resolved Address, dialer conn.Dialer, po PoolOptions,
	hooksRef *atomic.Pointer[Hooks], metrics *Metrics,
) *h1Pool {
	wrapped := dialer
	if dialer != nil {
		wrapped = conn.DialerFunc(func(ctx context.Context, _ string) (net.Conn, error) {
			return pool.DialResolved(ctx, dialer, resolved)
		})
	}
	return newH1Pool(resolved.String(), wrapped, po, hooksRef, metrics)
}
