// ManagedCore: the address fan-out shared by the HTTP/1.1,
// HTTP/2 and HTTP/3 managed pools (issue #364).
//
// The three managed pools measured 96.5-97.2% identical: 11 of 16 functions were
// byte-identical after renaming, and the complete set of differences was three,
// all type-level — the constructor payload, an h3-only dialFn default, and the
// shape of what acquire returns. This file is those 11 functions once, with the
// three differences supplied as type arguments and closures.
//
// Each protocol is a type ALIAS of an instantiation, not a wrapper struct. That
// is load-bearing for reviewability: the pinned behaviour tests reach into mp.Mu,
// mp.SubPools, mp.drainMode, mp.resolver and mp.tickerPeriod across ~20 sites, and
// an alias keeps every one of them compiling untouched. A refactor whose evaluator
// had to be edited to accept it would be self-certifying.
//
// DELIBERATELY NOT UNIFIED: the base pools (pool.go, h1_pool.go, h3_pool.go).
// They measure 28% mechanically unifiable and their divergences are the
// load-bearing ones — h1's opposite tick eviction order, h3's active==0 retire
// guard, exclusive checkout versus multiplexing. A generic core there would be
// mostly special cases wearing a shared signature.

package poolcore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/pool"
)

// SubPoolBackend is what the core needs from a per-address pool. Deliberately
// small: acquire for the failover loop, and the three the drain/stats/warmup
// paths call. release is NOT here — its signature differs per protocol, so it is
// supplied to the core as the mkRelease closure instead.
type SubPoolBackend[MC any] interface {
	Acquire(ctx context.Context) (MC, error)
	Stats() Stats
	Close() error
	Warmup(n int)
}

// CoreSubPool wraps a per-address pool with the core's metadata.
//
// INVARIANT (#875): draining is a strict function of set membership, and
// applySet is the only writer of it. applySet, under one mp.Mu.Lock(), clears
// draining for every address in the incoming set, sets it for every address of
// the old mp.addrs absent from the incoming set, and then assigns mp.addrs. So
// on exit from applySet: every address in mp.addrs has a non-draining sub-pool,
// and every draining sub-pool's address is absent from mp.addrs.
//
// Three guards elsewhere are unreachable under that invariant and are kept as
// defence-in-depth rather than deleted: snapshotActive's draining skip and
// applySet's own `!s.draining` both iterate mp.addrs, where no draining sub-pool
// can be; dropSubPool's `cur == s` identity check needs a delete-then-recreate
// that no caller performs. They cost nothing and they are the seatbelt for
// anyone adding a THIRD writer of draining — which is the change that breaks the
// invariant. The sibling guard dropIfDraining's `!s.draining` re-check is NOT in
// that category: it is reachable and load-bearing, protecting the revive-versus-
// drain-watchdog race, and TestManagedPool_ReviveBeatsDrainWatcher goes red when
// it is removed.
type CoreSubPool[P SubPoolBackend[MC], MC any] struct {
	p        P
	addr     Address
	draining bool
}

// ManagedCore fans Acquire across per-address sub-pools driven by a Resolver and
// Selector. Goroutine-safe.
//
// P is the sub-pool, MC its managed-conn record, C the connection handed to the
// caller and R the release closure's shape (func() for H2/H3, func(keepAlive bool)
// for H1, whose checkout is exclusive).
type ManagedCore[P SubPoolBackend[MC], MC any, C any, R any] struct {
	resolver  Resolver
	selector  Selector
	drainMode DrainMode
	poolOpts  PoolOptions

	obs pool.Observer
	rec pool.Recorder

	// The three measured differences, injected rather than branched on.
	newSub    func(key string) P
	connOf    func(MC) C
	mkRelease func(P, MC) R

	Mu       sync.RWMutex
	addrs    []Address
	SubPools map[string]*CoreSubPool[P, MC] // keyed by Address.String()

	closeOnce    sync.Once
	closed       chan struct{}
	tickerPeriod atomic.Int64 // nanoseconds; 0 → defaultManagedPoolTickerPeriod; test seam
}

// SnapshotActive returns a copy of the current active address set, taken
// under the read lock so a caller can range over it without holding one.
func (mp *ManagedCore[P, MC, C, R]) SnapshotActive() []Address {
	mp.Mu.RLock()
	defer mp.Mu.RUnlock()
	out := make([]Address, 0, len(mp.addrs))
	for _, a := range mp.addrs {
		if s, ok := mp.SubPools[a.String()]; ok && s.draining {
			continue
		}
		out = append(out, a)
	}
	return out
}

// GetOrCreateSubPool returns the sub-pool for addr, creating it lazily under the
// write lock if absent. Returns nil if the pool is closed or the sub-pool is
// draining (TOCTOU guard for acquire failover).
func (mp *ManagedCore[P, MC, C, R]) GetOrCreateSubPool(addr Address) *CoreSubPool[P, MC] {
	key := addr.String()
	mp.Mu.RLock()
	s, ok := mp.SubPools[key]
	isDraining := ok && s.draining
	mp.Mu.RUnlock()
	if ok && !isDraining {
		return s
	}
	if isDraining {
		return nil
	}

	mp.Mu.Lock()
	defer mp.Mu.Unlock()
	select {
	case <-mp.closed:
		return nil
	default:
	}
	if s, ok := mp.SubPools[key]; ok {
		if s.draining {
			return nil
		}
		return s
	}
	s = &CoreSubPool[P, MC]{
		p:    mp.newSub(key),
		addr: addr,
	}
	mp.SubPools[key] = s
	return s
}

// Acquire picks an address via Selector, acquires from its sub-pool, and returns
// the protocol connection plus its release handle. On dial-only errors it iterates through the
// remaining addresses (bounded by active set size).
func (mp *ManagedCore[P, MC, C, R]) Acquire(ctx context.Context) (C, R, Address, error) {
	var zeroC C
	var zeroR R
	var zeroA Address
	tried := make(map[string]struct{})
	var lastErr error
	var lastAddr Address
	// lastAddr is the address the most recent failed attempt was made against.
	// It is reported alongside the error so a failed managed request still says
	// WHICH backend it failed against — the address is the whole reason this
	// result exists, and a pool that times out under load is exactly the case
	// worth attributing.
	for {
		set := mp.SnapshotActive()
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
				return zeroC, zeroR, lastAddr, lastErr
			}
			return zeroC, zeroR, zeroA, ErrNoAddresses
		}
		addr, err := mp.selector.Pick(set, PickContext{})
		if err != nil {
			return zeroC, zeroR, zeroA, err
		}
		sub := mp.GetOrCreateSubPool(addr)
		if sub == nil {
			tried[addr.String()] = struct{}{}
			continue
		}
		mc, err := sub.p.Acquire(ctx)
		if err == nil {
			// The address is returned rather than left inside the pool because
			// it is the one fact a managed request cannot recover afterwards:
			// which backend the Selector chose for THIS request. Without it a
			// per-request record from a managed client cannot attribute a slow
			// response to the backend that produced it.
			return mp.connOf(mc), mp.mkRelease(sub.p, mc), addr, nil
		}
		if !IsDialOnlyErr(err) {
			return zeroC, zeroR, addr, err
		}
		lastErr = err
		lastAddr = addr
		tried[addr.String()] = struct{}{}
	}
}

// Run is the Watch consumer goroutine. Subscribes to Resolver.Watch and applies
// address-set updates until the pool is closed. Falls back to ticker mode
// when Watch returns ErrWatchUnsupported.
func (mp *ManagedCore[P, MC, C, R]) Run() {
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
func (mp *ManagedCore[P, MC, C, R]) runTicker(ctx context.Context) {
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

// Stats aggregates Stats across all sub-pools (active and draining).
func (mp *ManagedCore[P, MC, C, R]) Stats() Stats {
	mp.Mu.RLock()
	pools := make([]P, 0, len(mp.SubPools))
	var drainingCount int
	for _, s := range mp.SubPools {
		pools = append(pools, s.p)
		if s.draining {
			drainingCount++
		}
	}
	addrCount := len(mp.addrs)
	mp.Mu.RUnlock()

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
func (mp *ManagedCore[P, MC, C, R]) applySet(next []Address) {
	mp.Mu.Lock()
	prev := make(map[string]struct{}, len(mp.addrs))
	for _, a := range mp.addrs {
		prev[a.String()] = struct{}{}
	}
	nextSet := make(map[string]struct{}, len(next))
	for _, a := range next {
		nextSet[a.String()] = struct{}{}
	}
	var toDrain []*CoreSubPool[P, MC]
	added := make([]Address, 0, len(next))
	removed := make([]Address, 0, len(mp.addrs))
	for _, a := range next {
		if _, ok := prev[a.String()]; !ok {
			added = append(added, a)
		}
		// Revive an address that is back after being removed. draining was only
		// ever set, never cleared, so under DrainLazy — where beginDrain is a
		// no-op and nothing else removes the sub-pool — a resolver flap
		// (remove then re-add, the ordinary DNS case) left the address excluded
		// by snapshotActive and getOrCreateSubPool for the life of the pool. With
		// a single address that is a permanent ErrNoAddresses. Clearing the flag
		// is safe against a DrainGraceful watchDrain still polling this
		// sub-pool: that goroutine re-checks draining before dropping, and
		// dropSubPool only deletes the registry entry it was called for.
		if s, ok := mp.SubPools[a.String()]; ok && s.draining {
			s.draining = false
		}
	}
	for _, a := range mp.addrs {
		if _, ok := nextSet[a.String()]; ok {
			continue
		}
		removed = append(removed, a)
		if s, ok := mp.SubPools[a.String()]; ok && !s.draining {
			s.draining = true
			toDrain = append(toDrain, s)
		}
	}
	mp.addrs = append(mp.addrs[:0:0], next...)
	total := len(next)
	mp.Mu.Unlock()

	for _, s := range toDrain {
		mp.beginDrain(s)
	}
	if len(added) > 0 || len(removed) > 0 {
		mp.obs.OnResolverUpdate(ResolverUpdateEvent{
			Added: added, Removed: removed, Total: total,
		})
	}
}

// beginDrain dispatches per-mode drain logic for a removed sub-pool.
func (mp *ManagedCore[P, MC, C, R]) beginDrain(s *CoreSubPool[P, MC]) {
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
func (mp *ManagedCore[P, MC, C, R]) watchDrain(s *CoreSubPool[P, MC]) {
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
			// dropIfDraining, not dropSubPool: the address may have come back
			// while this watcher was polling, and closing the sub-pool then
			// would tear down a live address's connections.
			mp.dropIfDraining(s)
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
func (mp *ManagedCore[P, MC, C, R]) dropSubPool(s *CoreSubPool[P, MC], doClose bool) {
	mp.Mu.Lock()
	// Identity-checked: deleting by key alone would remove whatever sub-pool
	// currently holds this address, which after a remove/re-add flap is a
	// different, live one.
	if cur, ok := mp.SubPools[s.addr.String()]; ok && cur == s {
		delete(mp.SubPools, s.addr.String())
	}
	mp.Mu.Unlock()
	if doClose {
		_ = s.p.Close()
	}
}

// dropIfDraining removes and closes s only if it is still the registered,
// draining sub-pool for its address. Returns false if it was revived, in which
// case nothing is closed.
//
// The check and the delete are under one lock deliberately. A watcher that
// tested "still draining?" and then called an unguarded drop would leave a
// window for applySet to revive the address in between, and the drop would
// Close a sub-pool serving a live address.
func (mp *ManagedCore[P, MC, C, R]) dropIfDraining(s *CoreSubPool[P, MC]) bool {
	mp.Mu.Lock()
	cur, ok := mp.SubPools[s.addr.String()]
	if !ok || cur != s || !s.draining {
		mp.Mu.Unlock()
		return false
	}
	delete(mp.SubPools, s.addr.String())
	mp.Mu.Unlock()
	_ = s.p.Close()
	return true
}

// Close stops the pool and closes every sub-pool. Idempotent.
func (mp *ManagedCore[P, MC, C, R]) Close() error {
	mp.closeOnce.Do(func() {
		close(mp.closed)
		mp.Mu.Lock()
		defer mp.Mu.Unlock()
		for _, s := range mp.SubPools {
			_ = s.p.Close()
		}
		mp.SubPools = nil
		mp.addrs = nil
	})
	return nil
}

// Warmup pre-dials up to n conns distributed across the current set of resolved
// addresses. n is capped at MaxConnsPerHost per sub-pool.
func (mp *ManagedCore[P, MC, C, R]) Warmup(n int) {
	if n <= 0 {
		return
	}
	// Build the sub-pools from the resolved ADDRESS set, not from mp.SubPools.
	// subPools is populated lazily by getOrCreateSubPool, on the first acquire for an
	// address — so on a freshly constructed pool it is empty, this used to take
	// the len(subs)==0 early return, and Warmup pre-dialled nothing at all. The
	// whole point of a warmup is to run before the first request.
	mp.Mu.RLock()
	addrs := append([]Address(nil), mp.addrs...)
	mp.Mu.RUnlock()
	subs := make([]*CoreSubPool[P, MC], 0, len(addrs))
	for _, a := range addrs {
		if s := mp.GetOrCreateSubPool(a); s != nil {
			subs = append(subs, s)
		}
	}
	if len(subs) == 0 {
		return // no addresses resolved yet, or every one is draining
	}
	per := (n + len(subs) - 1) / len(subs)
	for _, s := range subs {
		s.p.Warmup(per)
	}
}

// CoreConfig is everything a ManagedCore needs at construction. The three
// function fields are the only places the HTTP/1.1, HTTP/2 and HTTP/3 managed
// pools actually differ; everything above them was byte-identical in all three.
type CoreConfig[P SubPoolBackend[MC], MC any, C any, R any] struct {
	Resolver  Resolver
	Selector  Selector
	DrainMode DrainMode
	PoolOpts  PoolOptions
	Obs       pool.Observer
	Rec       pool.Recorder

	// NewSub builds the per-address sub-pool for key.
	//
	// Its closure must capture the SAME Recorder this config carries. Each
	// pool constructor substitutes a fresh recorder for a nil one, so letting
	// every sub-pool default independently would under-count the caller's
	// metrics with the whole suite green.
	NewSub func(key string) P
	// ConnOf extracts the protocol connection from a sub-pool's record.
	ConnOf func(MC) C
	// MkRelease builds the release handle handed back with an acquire.
	MkRelease func(P, MC) R
}

// NewCore builds a ManagedCore and seeds it with the resolver's current answer,
// without starting its goroutine — the caller starts Run when it is ready. It
// reports the resolver's error only when that answer is also empty, so a
// partially failing resolver still produces a usable pool.
func NewCore[P SubPoolBackend[MC], MC any, C any, R any](cfg CoreConfig[P, MC, C, R]) (*ManagedCore[P, MC, C, R], error) {
	if cfg.Selector == nil {
		cfg.Selector = RoundRobin()
	}
	if cfg.Obs == nil {
		cfg.Obs = pool.NopObserver{}
	}
	if cfg.Rec == nil {
		cfg.Rec = pool.NopRecorder{}
	}
	mp := &ManagedCore[P, MC, C, R]{
		resolver:  cfg.Resolver,
		selector:  cfg.Selector,
		drainMode: cfg.DrainMode,
		poolOpts:  cfg.PoolOpts,
		obs:       cfg.Obs,
		rec:       cfg.Rec,
		newSub:    cfg.NewSub,
		connOf:    cfg.ConnOf,
		mkRelease: cfg.MkRelease,
		SubPools:  make(map[string]*CoreSubPool[P, MC]),
		closed:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addrs, err := cfg.Resolver.Resolve(ctx)
	if err != nil && len(addrs) == 0 {
		return nil, err
	}
	mp.addrs = addrs
	return mp, nil
}

// SetTickerPeriod overrides the sweep interval. A test seam: it must be called
// before Run, which is the only reader that latches it.
func (mp *ManagedCore[P, MC, C, R]) SetTickerPeriod(d time.Duration) {
	mp.tickerPeriod.Store(int64(d))
}

// The accessors below are the read seams the per-protocol managed pools' tests
// use. They exist rather than exported fields because every one of these is
// guarded state: mu covers addrs and subPools, and handing a test the mutex
// would make the locking discipline the test's problem.

// Resolver returns the resolver this core was built with.
func (mp *ManagedCore[P, MC, C, R]) Resolver() Resolver { return mp.resolver }

// DrainMode returns the drain mode this core was built with.
func (mp *ManagedCore[P, MC, C, R]) DrainMode() DrainMode { return mp.drainMode }

// HasSubPool reports whether a sub-pool is currently registered for key, which
// is Address.String() of the address it serves.
func (mp *ManagedCore[P, MC, C, R]) HasSubPool(key string) bool {
	mp.Mu.RLock()
	defer mp.Mu.RUnlock()
	_, ok := mp.SubPools[key]
	return ok
}
