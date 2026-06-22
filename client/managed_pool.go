// Package client — managedPool: per-address sub-pool fan-out driven
// by a Resolver and Selector.
package client

import (
	"context"
	"errors"
	"sync"
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
	// DrainLazy refuses new acquires; idle eviction handles eventual close.
	DrainLazy
)

// subPoolState wraps a *Pool with managedPool-level metadata.
type subPoolState struct {
	p        *Pool
	addr     Address
	draining bool
}

// managedPool fans Acquire across per-address sub-pools driven by
// a Resolver and Selector. Goroutine-safe.
type managedPool struct {
	resolver  Resolver
	selector  Selector
	drainMode DrainMode
	connOpts  conn.ConnOptions
	poolOpts  PoolOptions

	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics

	mu       sync.RWMutex
	addrs    []Address
	subPools map[string]*subPoolState // keyed by Address.String()

	closeOnce    sync.Once
	closed       chan struct{}
	tickerPeriod atomic.Int64 // nanoseconds; 0 → defaultManagedPoolTickerPeriod; test seam
}

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
		connOpts:  co,
		poolOpts:  po,
		hooksRef:  hooksRef,
		metrics:   metrics,
		subPools:  make(map[string]*subPoolState),
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

// snapshotActive returns a copy of the currently active (non-draining)
// address set.
func (mp *managedPool) snapshotActive() []Address {
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

// getOrCreateSubPool returns the sub-pool for addr, creating it lazily
// under the write lock if absent. Returns nil if the pool is closed or
// the sub-pool is draining (TOCTOU guard for acquire failover).
func (mp *managedPool) getOrCreateSubPool(addr Address) *subPoolState {
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
	s = &subPoolState{
		p:    newPool(key, mp.connOpts, mp.poolOpts, mp.hooksRef, mp.metrics),
		addr: addr,
	}
	mp.subPools[key] = s
	return s
}

// acquire picks an address via Selector, acquires from its sub-pool,
// and returns the conn + release closure. On dial-only errors it
// iterates through remaining addresses (bounded by active set size).
func (mp *managedPool) acquire(ctx context.Context) (*conn.Conn, func(), error) {
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
			return mc.c, release, nil
		}
		if !isDialOnlyErr(err) {
			return nil, nil, err
		}
		lastErr = err
		tried[addr.String()] = struct{}{}
	}
}

// isDialOnlyErr returns true for transient per-address failures that warrant
// address-level failover: DialError, ErrDialBackoff, and ErrPoolClosed (the
// last can occur when a DrainHard eviction races with an in-flight acquire).
func isDialOnlyErr(err error) bool {
	if errors.Is(err, ErrDialBackoff) || errors.Is(err, ErrPoolClosed) {
		return true
	}
	var de *DialError
	return errors.As(err, &de)
}

// run is the Watch consumer goroutine. Subscribes to Resolver.Watch
// and applies address-set updates until the managedPool is closed. If
// Watch returns ErrWatchUnsupported, switches to ticker mode.
func (mp *managedPool) run() {
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

// runTicker polls Resolver.Resolve at tickerPeriod cadence and
// applies the result as if it were a Watch update. Used when the
// Resolver returns ErrWatchUnsupported or when the Watch channel
// closes unexpectedly.
func (mp *managedPool) runTicker(ctx context.Context) {
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

// stats aggregates Stats across all sub-pools (active and draining)
// and returns the combined snapshot.
func (mp *managedPool) stats() Stats {
	mp.mu.RLock()
	pools := make([]*Pool, 0, len(mp.subPools))
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

// applySet diffs old vs new active address set. Additions are no-ops
// (sub-pools dial lazily on Acquire). Removals mark sub-pools as
// draining and dispatch drain logic. Fires OnResolverUpdate hook when
// the set changes.
func (mp *managedPool) applySet(next []Address) {
	mp.mu.Lock()
	prev := make(map[string]struct{}, len(mp.addrs))
	for _, a := range mp.addrs {
		prev[a.String()] = struct{}{}
	}
	nextSet := make(map[string]struct{}, len(next))
	for _, a := range next {
		nextSet[a.String()] = struct{}{}
	}
	var toDrain []*subPoolState
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
func (mp *managedPool) beginDrain(s *subPoolState) {
	switch mp.drainMode {
	case DrainHard:
		mp.dropSubPool(s, true)
	case DrainLazy:
		// No-op: draining=true blocks new Acquires; idle eviction closes conns.
	default: // DrainGraceful
		go mp.watchDrain(s)
	}
}

// watchDrain polls the sub-pool's Stats with exponential back-off.
// Once InFlightStreams == 0 it closes and removes the sub-pool from
// the registry. The initial interval is 20 ms and doubles on each
// idle poll up to drainPollMax.
func (mp *managedPool) watchDrain(s *subPoolState) {
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
		// Back off: double the interval, cap at drainPollMax.
		interval *= 2
		if interval > drainPollMax {
			interval = drainPollMax
		}
		t.Reset(interval)
	}
}

// dropSubPool removes s from the registry and optionally closes it.
func (mp *managedPool) dropSubPool(s *subPoolState, doClose bool) {
	mp.mu.Lock()
	delete(mp.subPools, s.addr.String())
	mp.mu.Unlock()
	if doClose {
		_ = s.p.Close()
	}
}

// close stops the managedPool and closes every sub-pool. Idempotent.
func (mp *managedPool) close() error {
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

// warmup pre-dials up to n conns distributed across the current
// set of resolved addresses. n is capped at MaxConnsPerHost.
func (mp *managedPool) warmup(n int) {
	if n <= 0 {
		return
	}
	mp.mu.Lock()
	subs := make([]*subPoolState, 0, len(mp.subPools))
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
