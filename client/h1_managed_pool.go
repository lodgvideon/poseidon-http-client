// Package client — h1ManagedPool: per-address HTTP/1.1 sub-pool fan-out driven by
// a Resolver and Selector. The HTTP/1.1 analogue of managedPool and h3ManagedPool;
// it reuses the protocol-agnostic Resolver, Selector, DrainMode, Address, and
// isDialOnlyErr, and differs only in that each sub-pool is an *h1Pool of
// exclusive-checkout HTTP/1.1 connections (see h1_pool.go) and that release
// carries the keep-alive decision.
package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// h1SubPoolState wraps an *h1Pool with h1ManagedPool-level metadata.
type h1SubPoolState struct {
	p        *h1Pool
	addr     Address
	draining bool
}

// h1ManagedPool fans acquire across per-address HTTP/1.1 sub-pools driven by a
// Resolver and Selector. Goroutine-safe.
type h1ManagedPool struct {
	resolver  Resolver
	selector  Selector
	drainMode DrainMode
	dialer    conn.Dialer
	poolOpts  PoolOptions

	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics

	mu       sync.RWMutex
	addrs    []Address
	subPools map[string]*h1SubPoolState // keyed by Address.String()

	closeOnce    sync.Once
	closed       chan struct{}
	tickerPeriod atomic.Int64 // nanoseconds; 0 → defaultManagedPoolTickerPeriod; test seam
}

// newH1ManagedPool constructs an h1ManagedPool and starts its Watch/ticker
// goroutine. It performs an initial Resolve to surface hard errors early.
func newH1ManagedPool(r Resolver, s Selector, dm DrainMode, dialer conn.Dialer, po PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*h1ManagedPool, error) {
	mp, err := buildH1ManagedPool(r, s, dm, dialer, po, hooksRef, metrics)
	if err != nil {
		return nil, err
	}
	go mp.run()
	return mp, nil
}

// buildH1ManagedPool constructs and initialises an h1ManagedPool without starting
// its background goroutine. Tests that need to configure fields (e.g.
// tickerPeriod) before the goroutine reads them call this and start it themselves.
func buildH1ManagedPool(r Resolver, s Selector, dm DrainMode, dialer conn.Dialer, po PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*h1ManagedPool, error) {
	if s == nil {
		s = RoundRobin()
	}
	if metrics == nil {
		metrics = &Metrics{}
	}
	mp := &h1ManagedPool{
		resolver:  r,
		selector:  s,
		drainMode: dm,
		dialer:    dialer,
		poolOpts:  po,
		hooksRef:  hooksRef,
		metrics:   metrics,
		subPools:  make(map[string]*h1SubPoolState),
		closed:    make(chan struct{}),
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

// snapshotActive returns a copy of the currently active (non-draining) address set.
func (mp *h1ManagedPool) snapshotActive() []Address {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	out := make([]Address, 0, len(mp.addrs))
	for _, a := range mp.addrs {
		if s, ok := mp.subPools[a.String()]; ok && s.draining {
			continue
		}
		out = append(out, a)
	}
	return out
}

// getOrCreateSubPool returns the sub-pool for addr, creating it lazily under the
// write lock if absent. Returns nil if the pool is closed or the sub-pool is
// draining (TOCTOU guard for acquire failover).
func (mp *h1ManagedPool) getOrCreateSubPool(addr Address) *h1SubPoolState {
	key := addr.String()
	mp.mu.RLock()
	s, ok := mp.subPools[key]
	isDraining := ok && s.draining
	mp.mu.RUnlock()
	if ok && !isDraining {
		return s
	}
	if isDraining {
		return nil
	}

	mp.mu.Lock()
	defer mp.mu.Unlock()
	select {
	case <-mp.closed:
		return nil
	default:
	}
	if s, ok := mp.subPools[key]; ok {
		if s.draining {
			return nil
		}
		return s
	}
	s = &h1SubPoolState{
		p:    newH1Pool(key, mp.dialer, mp.poolOpts, mp.hooksRef, mp.metrics),
		addr: addr,
	}
	mp.subPools[key] = s
	return s
}

// acquire picks an address via Selector, checks a connection out of its sub-pool,
// and returns the conn plus its release closure. On dial-only errors it iterates
// through the remaining addresses (bounded by the active set size).
//
// The release closure takes the exchange's keep-alive verdict: pass false to
// discard the connection instead of returning it to the sub-pool.
func (mp *h1ManagedPool) acquire(ctx context.Context) (*http1.Conn, func(keepAlive bool), error) {
	tried := make(map[string]struct{})
	var lastErr error
	for {
		set := mp.snapshotActive()
		if len(tried) > 0 {
			pruned := set[:0]
			for _, a := range set {
				if _, ok := tried[a.String()]; !ok {
					pruned = append(pruned, a)
				}
			}
			set = pruned
		}
		if len(set) == 0 {
			if lastErr != nil {
				return nil, nil, lastErr
			}
			return nil, nil, ErrNoAddresses
		}
		addr, err := mp.selector.Pick(set, PickContext{})
		if err != nil {
			return nil, nil, err
		}
		sub := mp.getOrCreateSubPool(addr)
		if sub == nil {
			tried[addr.String()] = struct{}{}
			continue
		}
		mc, err := sub.p.acquire(ctx)
		if err == nil {
			release := func(keepAlive bool) { sub.p.release(mc, keepAlive) }
			return mc.c, release, nil
		}
		if !isDialOnlyErr(err) {
			return nil, nil, err
		}
		lastErr = err
		tried[addr.String()] = struct{}{}
	}
}

// run is the Watch consumer goroutine. Subscribes to Resolver.Watch and applies
// address-set updates until the h1ManagedPool is closed. Falls back to ticker mode
// when Watch returns ErrWatchUnsupported.
func (mp *h1ManagedPool) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-mp.closed
		cancel()
	}()

	ch, err := mp.resolver.Watch(ctx)
	if err != nil {
		mp.runTicker(ctx)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case set, ok := <-ch:
			if !ok {
				mp.runTicker(ctx)
				return
			}
			mp.applySet(set)
		}
	}
}

// runTicker polls Resolver.Resolve at tickerPeriod cadence and applies the result
// as if it were a Watch update.
func (mp *h1ManagedPool) runTicker(ctx context.Context) {
	period := time.Duration(mp.tickerPeriod.Load())
	if period <= 0 {
		period = defaultManagedPoolTickerPeriod
	}
	tick := time.NewTicker(period)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		next, err := mp.resolver.Resolve(ctx)
		if err != nil && !errors.Is(err, ErrNoAddresses) {
			continue // transient soft fail — keep the current set
		}
		mp.applySet(next)
	}
}

// stats aggregates Stats across all sub-pools (active and draining).
func (mp *h1ManagedPool) stats() Stats {
	mp.mu.RLock()
	pools := make([]*h1Pool, 0, len(mp.subPools))
	var drainingCount int
	for _, s := range mp.subPools {
		pools = append(pools, s.p)
		if s.draining {
			drainingCount++
		}
	}
	addrCount := len(mp.addrs)
	mp.mu.RUnlock()

	var out Stats
	out.Addresses = addrCount
	out.DrainingSubpools = drainingCount
	for _, p := range pools {
		st := p.Stats()
		out.ActiveConns += st.ActiveConns
		out.InFlightStreams += st.InFlightStreams
		out.Waiters += st.Waiters
		out.InFlightDials += st.InFlightDials
	}
	return out
}

// applySet diffs old vs new active address set. Additions are no-ops (sub-pools
// dial lazily on acquire). Removals mark sub-pools as draining and dispatch drain
// logic. Fires OnResolverUpdate when the set changes.
func (mp *h1ManagedPool) applySet(next []Address) {
	mp.mu.Lock()
	prev := make(map[string]struct{}, len(mp.addrs))
	for _, a := range mp.addrs {
		prev[a.String()] = struct{}{}
	}
	nextSet := make(map[string]struct{}, len(next))
	for _, a := range next {
		nextSet[a.String()] = struct{}{}
	}
	var toDrain []*h1SubPoolState
	added := make([]Address, 0, len(next))
	removed := make([]Address, 0, len(mp.addrs))
	for _, a := range next {
		if _, ok := prev[a.String()]; !ok {
			added = append(added, a)
		}
	}
	for _, a := range mp.addrs {
		if _, ok := nextSet[a.String()]; ok {
			continue
		}
		removed = append(removed, a)
		if s, ok := mp.subPools[a.String()]; ok && !s.draining {
			s.draining = true
			toDrain = append(toDrain, s)
		}
	}
	mp.addrs = append(mp.addrs[:0:0], next...)
	total := len(next)
	mp.mu.Unlock()

	for _, s := range toDrain {
		mp.beginDrain(s)
	}
	if len(added) > 0 || len(removed) > 0 {
		if hr := mp.hooksRef; hr != nil {
			if h := hr.Load(); h != nil && h.OnResolverUpdate != nil {
				h.OnResolverUpdate(ResolverUpdateEvent{
					Added: added, Removed: removed, Total: total,
				})
			}
		}
	}
}

// beginDrain dispatches per-mode drain logic for a removed sub-pool.
func (mp *h1ManagedPool) beginDrain(s *h1SubPoolState) {
	switch mp.drainMode {
	case DrainHard:
		mp.dropSubPool(s, true)
	case DrainLazy:
		// No-op: draining=true blocks new acquires; idle eviction closes conns.
	default: // DrainGraceful
		go mp.watchDrain(s)
	}
}

// watchDrain polls the sub-pool's Stats with exponential back-off. Once
// InFlightStreams == 0 (no exchange is checked out) it closes and removes the
// sub-pool from the registry.
func (mp *h1ManagedPool) watchDrain(s *h1SubPoolState) {
	const (
		drainPollInit = 20 * time.Millisecond
		drainPollMax  = 5 * time.Second
	)
	interval := drainPollInit
	t := time.NewTimer(interval)
	defer t.Stop()
	for {
		select {
		case <-mp.closed:
			return
		case <-t.C:
		}
		if s.p.Stats().InFlightStreams == 0 {
			mp.dropSubPool(s, true)
			return
		}
		interval *= 2
		if interval > drainPollMax {
			interval = drainPollMax
		}
		t.Reset(interval)
	}
}

// dropSubPool removes s from the registry and optionally closes it.
func (mp *h1ManagedPool) dropSubPool(s *h1SubPoolState, doClose bool) {
	mp.mu.Lock()
	delete(mp.subPools, s.addr.String())
	mp.mu.Unlock()
	if doClose {
		_ = s.p.Close()
	}
}

// close stops the h1ManagedPool and closes every sub-pool. Idempotent.
func (mp *h1ManagedPool) close() error {
	mp.closeOnce.Do(func() {
		close(mp.closed)
		mp.mu.Lock()
		defer mp.mu.Unlock()
		for _, s := range mp.subPools {
			_ = s.p.Close()
		}
		mp.subPools = nil
		mp.addrs = nil
	})
	return nil
}

// warmup pre-dials up to n conns distributed across the current set of resolved
// addresses. n is capped at MaxConnsPerHost per sub-pool.
func (mp *h1ManagedPool) warmup(n int) {
	if n <= 0 {
		return
	}
	mp.mu.Lock()
	subs := make([]*h1SubPoolState, 0, len(mp.subPools))
	for _, s := range mp.subPools {
		subs = append(subs, s)
	}
	mp.mu.Unlock()
	if len(subs) == 0 {
		return
	}
	per := (n + len(subs) - 1) / len(subs)
	for _, s := range subs {
		s.p.warmup(per)
	}
}
