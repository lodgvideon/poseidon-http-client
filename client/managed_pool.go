// Package client — managedPool: per-address sub-pool fan-out driven
// by a Resolver and Selector.
package client

import (
	"context"
	"errors"
	"sync"
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

	mu       sync.RWMutex
	addrs    []Address
	subPools map[string]*subPoolState // keyed by Address.String()

	closeOnce    sync.Once
	closed       chan struct{}
	tickerPeriod time.Duration // 0 → defaultManagedPoolTickerPeriod; test seam
}

// newManagedPool constructs a managedPool. It performs an initial
// Resolve to surface hard errors early; if Resolve returns 0 addrs
// the pool starts empty (Acquire returns ErrNoAddresses).
func newManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions) (*managedPool, error) {
	if s == nil {
		s = RoundRobin()
	}
	mp := &managedPool{
		resolver:  r,
		selector:  s,
		drainMode: dm,
		connOpts:  co,
		poolOpts:  po,
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
	go mp.run()
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
	mp.mu.RUnlock()
	if ok && !s.draining {
		return s
	}
	if ok && s.draining {
		return nil // caller's snapshotActive shouldn't have given us this; TOCTOU guard
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
		p:    newPool(key, mp.connOpts, mp.poolOpts),
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

// isDialOnlyErr returns true for transient dial failures that warrant
// address-level failover: DialError and ErrDialBackoff.
func isDialOnlyErr(err error) bool {
	if errors.Is(err, ErrDialBackoff) {
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

// runTicker is a placeholder until Task 10. Blocks until ctx.Done.
func (mp *managedPool) runTicker(ctx context.Context) {
	<-ctx.Done()
}

// applySet diffs old vs new active address set. Additions are no-ops
// (sub-pools dial lazily on Acquire). Removals mark sub-pools as
// draining and dispatch drain logic.
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
	for key := range prev {
		if _, ok := nextSet[key]; ok {
			continue
		}
		if s, ok := mp.subPools[key]; ok && !s.draining {
			s.draining = true
			toDrain = append(toDrain, s)
		}
	}
	mp.addrs = append(mp.addrs[:0:0], next...)
	mp.mu.Unlock()

	for _, s := range toDrain {
		mp.beginDrain(s)
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

// watchDrain polls the sub-pool's Stats; once InFlightStreams == 0,
// closes and removes it from the registry.
func (mp *managedPool) watchDrain(s *subPoolState) {
	t := time.NewTicker(20 * time.Millisecond)
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
