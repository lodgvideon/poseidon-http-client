// Package client — h3ManagedPool: per-address HTTP/3 sub-pool fan-out driven by a
// Resolver and Selector. The HTTP/3 analogue of managedPool; it reuses the
// protocol-agnostic Resolver, Selector, DrainMode, Address, and isDialOnlyErr and
// differs only in that each sub-pool is an *h3Pool of QUIC connections.
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// h3SubPoolState wraps an *h3Pool with h3ManagedPool-level metadata.
type h3SubPoolState struct {
	p        *h3Pool
	addr     Address
	draining bool
}

// h3ManagedPool fans Acquire across per-address HTTP/3 sub-pools driven by a
// Resolver and Selector. Goroutine-safe.
type h3ManagedPool struct {
	resolver  Resolver
	selector  Selector
	drainMode DrainMode
	tlsConfig *tls.Config
	poolOpts  PoolOptions
	dialFn    func(context.Context, string, *tls.Config) (h3Client, error)

	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics

	mu       sync.RWMutex
	addrs    []Address
	subPools map[string]*h3SubPoolState // keyed by Address.String()

	closeOnce    sync.Once
	closed       chan struct{}
	tickerPeriod atomic.Int64 // nanoseconds; 0 → defaultManagedPoolTickerPeriod; test seam
}

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
		tlsConfig: tlsConfig,
		poolOpts:  po,
		dialFn:    dialFn,
		hooksRef:  hooksRef,
		metrics:   metrics,
		subPools:  make(map[string]*h3SubPoolState),
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
func (mp *h3ManagedPool) snapshotActive() []Address {
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
func (mp *h3ManagedPool) getOrCreateSubPool(addr Address) *h3SubPoolState {
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
	s = &h3SubPoolState{
		p:    newH3Pool(key, mp.tlsConfig, mp.poolOpts, mp.dialFn, mp.hooksRef, mp.metrics),
		addr: addr,
	}
	mp.subPools[key] = s
	return s
}

// acquire picks an address via Selector, acquires from its sub-pool, and returns
// the h3Client + release closure. On dial-only errors it iterates through the
// remaining addresses (bounded by active set size).
func (mp *h3ManagedPool) acquire(ctx context.Context) (h3Client, func(), error) {
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
			release := func() { sub.p.release(mc, nil) }
			return mc.cl, release, nil
		}
		if !isDialOnlyErr(err) {
			return nil, nil, err
		}
		lastErr = err
		tried[addr.String()] = struct{}{}
	}
}

// run is the Watch consumer goroutine. Subscribes to Resolver.Watch and applies
// address-set updates until the h3ManagedPool is closed. Falls back to ticker mode
// when Watch returns ErrWatchUnsupported.
func (mp *h3ManagedPool) run() {
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
func (mp *h3ManagedPool) runTicker(ctx context.Context) {
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
func (mp *h3ManagedPool) stats() Stats {
	mp.mu.RLock()
	pools := make([]*h3Pool, 0, len(mp.subPools))
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
// dial lazily on Acquire). Removals mark sub-pools as draining and dispatch drain
// logic. Fires OnResolverUpdate when the set changes.
func (mp *h3ManagedPool) applySet(next []Address) {
	mp.mu.Lock()
	prev := make(map[string]struct{}, len(mp.addrs))
	for _, a := range mp.addrs {
		prev[a.String()] = struct{}{}
	}
	nextSet := make(map[string]struct{}, len(next))
	for _, a := range next {
		nextSet[a.String()] = struct{}{}
	}
	var toDrain []*h3SubPoolState
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
func (mp *h3ManagedPool) beginDrain(s *h3SubPoolState) {
	switch mp.drainMode {
	case DrainHard:
		mp.dropSubPool(s, true)
	case DrainLazy:
		// No-op: draining=true blocks new Acquires; idle eviction closes conns.
	default: // DrainGraceful
		go mp.watchDrain(s)
	}
}

// watchDrain polls the sub-pool's Stats with exponential back-off. Once
// InFlightStreams == 0 it closes and removes the sub-pool from the registry.
func (mp *h3ManagedPool) watchDrain(s *h3SubPoolState) {
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
func (mp *h3ManagedPool) dropSubPool(s *h3SubPoolState, doClose bool) {
	mp.mu.Lock()
	delete(mp.subPools, s.addr.String())
	mp.mu.Unlock()
	if doClose {
		_ = s.p.Close()
	}
}

// close stops the h3ManagedPool and closes every sub-pool. Idempotent.
func (mp *h3ManagedPool) close() error {
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
func (mp *h3ManagedPool) warmup(n int) {
	if n <= 0 {
		return
	}
	mp.mu.Lock()
	subs := make([]*h3SubPoolState, 0, len(mp.subPools))
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
