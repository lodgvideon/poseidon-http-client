// Package client — managedPool: per-address sub-pool fan-out driven
// by a Resolver and Selector.
package client

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// defaultManagedPoolTickerPeriod is the fallback poll period when the
// Resolver does not support Watch (returns ErrWatchUnsupported).
const defaultManagedPoolTickerPeriod = 30 * time.Second

// DrainMode governs sub-pool lifecycle when an address is removed
// from the resolver's set.
type DrainMode int

const (
	// DrainGraceful refuses new acquires on the removed sub-pool;
	// existing in-flight requests complete naturally; sub-pool closes
	// when its active stream count reaches zero.
	DrainGraceful DrainMode = iota
	// DrainHard closes every conn in the removed sub-pool immediately;
	// in-flight streams surface as RST_STREAM(CANCEL).
	DrainHard
	// DrainLazy refuses new acquires and leaves closing to idle eviction, which
	// means PoolOptions.IdleTimeout decides whether the conns ever close at all.
	// That field defaults to 0, documented as "never close on idle" — so under
	// default options DrainLazy's removed sub-pool keeps its connections open
	// for the life of the pool, and "eventual" never arrives. Set IdleTimeout,
	// or use DrainGraceful, which closes once the sub-pool goes idle regardless.
	DrainLazy
)

// subPoolState is the HTTP/2 instantiation of the core's per-address record.
// An alias, not a wrapper: the pinned behaviour tests index mp.subPools and read
// these fields directly.
type subPoolState = coreSubPool[*Pool, *managedConn]

// managedPool fans Acquire across per-address HTTP/2 sub-pools driven by a
// Resolver and Selector. Goroutine-safe.
//
// An ALIAS of the shared core (managed_core.go). Verified before the move: every
// method body except acquire and getOrCreateSubPool is identical to the core's
// once identifiers are normalised and comments dropped — including watchDrain,
// whose only divergence was a back-off comment H1 and H3 never carried. The two
// that differ are exactly the injection points.
type managedPool = managedCore[*Pool, *managedConn, *conn.Conn, func()]

// newManagedPool constructs a managedPool and starts its Watch/ticker
// goroutine. It performs an initial Resolve to surface hard errors
// early; if Resolve returns 0 addrs the pool starts empty (Acquire
// returns ErrNoAddresses).
func newManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*managedPool, error) {
	mp, err := buildManagedPool(r, s, dm, co, po, hooksRef, metrics)
	if err != nil {
		return nil, err
	}
	go mp.run()
	return mp, nil
}

// buildManagedPool constructs and initialises a managedPool without
// starting its background goroutine. Tests that need to configure
// fields (e.g. tickerPeriod) before the goroutine reads them call
// this and start the goroutine themselves via go mp.run().
func buildManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*managedPool, error) {
	if s == nil {
		s = RoundRobin()
	}
	if metrics == nil {
		metrics = &Metrics{}
	}
	mp := &managedPool{
		resolver:  r,
		selector:  s,
		drainMode: dm,
		poolOpts:  po,
		hooksRef:  hooksRef,
		metrics:   metrics,
		subPools:  make(map[string]*subPoolState),
		closed:    make(chan struct{}),

		// The three measured differences, as closures rather than branches.
		// metrics is captured deliberately: newPool defaults a nil *Metrics to a
		// fresh struct of its own, so letting each sub-pool default independently
		// would under-count Client.Metrics() with the whole suite green.
		newSub: func(key string) *Pool {
			return newPool(key, co, po, hooksRef, metrics)
		},
		connOf: func(mc *managedConn) *conn.Conn { return mc.c },
		mkRelease: func(p *Pool, mc *managedConn) func() {
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
func isDialOnlyErr(err error) bool {
	if errors.Is(err, ErrDialBackoff) || errors.Is(err, ErrPoolClosed) {
		return true
	}
	var de *DialError
	return errors.As(err, &de)
}
