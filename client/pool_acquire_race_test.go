package client

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// Regression tests for the acquire/abandon slot leak shared by the H2 *Pool and
// the H3 *h3Pool (the H1 pool was fixed at birth; see h1_pool.go replyAcquire).
//
// Bug: replyAcquire raced its send against req.ctx.Done(). The reply channel is
// buffered (cap 1), so when the caller had already abandoned AND the buffer was
// empty, BOTH select cases were ready and Go chose uniformly at random. When the
// send won, a response carrying an mc whose active count the actor had already
// incremented landed in a buffer nobody would ever read: mc.active-- never ran
// and the stream slot was leaked permanently. A per-request timeout — the normal
// load-generator workload — triggers it.
//
// Deterministic trigger below: hand the actor a request whose ctx is ALREADY
// cancelled, so the coin is flipped on every iteration rather than only inside a
// narrow race window. A small pool makes leaked slots starve it fast.

// waitStats polls p.Stats() until want(s) holds or the deadline expires, and
// returns the last snapshot. Releases and reclaims are asynchronous (the actor
// may serve statsCh before a queued releaseCh message), so a bare assertion
// would be flaky in the passing direction.
func waitStats(p *Pool, want func(Stats) bool, d time.Duration) Stats {
	deadline := time.Now().Add(d)
	var s Stats
	for {
		s = p.Stats()
		if want(s) || time.Now().After(deadline) {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitH3Stats is waitStats for the H3 pool.
func waitH3Stats(p *h3Pool, want func(Stats) bool, d time.Duration) Stats {
	deadline := time.Now().Add(d)
	var s Stats
	for {
		s = p.Stats()
		if want(s) || time.Now().After(deadline) {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitGoroutines polls until the count drops to at most limit, returning the
// last sample. Used to prove the reclaim goroutines all exit — a reply the actor
// owes but never sends would hang one forever.
func waitGoroutines(limit int, d time.Duration) int {
	deadline := time.Now().Add(d)
	for {
		n := runtime.NumGoroutine()
		if n <= limit || time.Now().After(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}

const abandonIters = 200

// TestPool_AbandonedAcquire_DoesNotLeakStreamSlots drives abandonIters acquires
// whose ctx is cancelled before the actor can serve them, then asserts the pool
// is not starved: every incremented active count came back and a normal acquire
// still succeeds.
//
// Pre-fix, each iteration leaks a slot with probability ~1/8 (three independent
// coin flips: acquireCh-vs-ctx.Done, replyAcquire's send-vs-ctx.Done, and the
// caller's reply-vs-ctx.Done), so with MaxStreamsPerConn=2 the pool starves
// within the first handful of iterations.
func TestPool_AbandonedAcquire_DoesNotLeakStreamSlots(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()

	p := newPool(addrs[0].String(), newConnOpts(), PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 2,
		HealthCheckPeriod: time.Hour, // no tick: isolate the acquire/abandon path
	}, nil, nil)
	defer func() { _ = p.Close() }()

	// Prime: get one live conn into the pool so every abandoned acquire below
	// hits the pickLeastLoaded happy path (the one that increments active).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	mc, err := p.acquire(ctx)
	cancel()
	if err != nil {
		t.Fatalf("priming acquire: %v", err)
	}
	p.release(mc, nil)
	if s := waitStats(p, func(s Stats) bool { return s.InFlightStreams == 0 }, 2*time.Second); s.InFlightStreams != 0 {
		t.Fatalf("priming release did not settle: %+v", s)
	}

	before := runtime.NumGoroutine()

	for i := 0; i < abandonIters; i++ {
		actx, acancel := context.WithCancel(context.Background())
		acancel() // already done: the actor must not strand a committed conn
		amc, aerr := p.acquire(actx)
		if aerr == nil {
			// The reply won the caller's select; releasing is the caller's contract.
			p.release(amc, nil)
		}
	}

	if s := waitStats(p, func(s Stats) bool { return s.InFlightStreams == 0 }, 3*time.Second); s.InFlightStreams != 0 {
		t.Fatalf("stream slots leaked by abandoned acquires: %+v (want InFlightStreams=0 after %d abandons)",
			s, abandonIters)
	}

	// The pool must still be usable: a leaked slot starves it permanently.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	mc2, err2 := p.acquire(ctx2)
	cancel2()
	if err2 != nil {
		t.Fatalf("normal acquire after %d abandons = %v, want success (pool starved by leaked slots)",
			abandonIters, err2)
	}
	p.release(mc2, nil)

	// Every reclaim goroutine must exit: the actor owes each accepted request
	// exactly one reply, and reclaim blocks until it arrives.
	if n := waitGoroutines(before+4, 3*time.Second); n > before+4 {
		t.Errorf("goroutine leak after abandoned acquires: before=%d after=%d (want <= %d)",
			before, n, before+4)
	}
}

// TestH3Pool_AbandonedAcquire_DoesNotLeakStreamSlots is the H3 twin of
// TestPool_AbandonedAcquire_DoesNotLeakStreamSlots.
func TestH3Pool_AbandonedAcquire_DoesNotLeakStreamSlots(t *testing.T) {
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 2,
		HealthCheckPeriod: time.Hour,
	}, newH3FakeDialer().dial, nil, nil)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	mc, err := p.acquire(ctx)
	cancel()
	if err != nil {
		t.Fatalf("priming acquire: %v", err)
	}
	p.release(mc, nil)
	if s := waitH3Stats(p, func(s Stats) bool { return s.InFlightStreams == 0 }, 2*time.Second); s.InFlightStreams != 0 {
		t.Fatalf("priming release did not settle: %+v", s)
	}

	before := runtime.NumGoroutine()

	for i := 0; i < abandonIters; i++ {
		actx, acancel := context.WithCancel(context.Background())
		acancel()
		amc, aerr := p.acquire(actx)
		if aerr == nil {
			p.release(amc, nil)
		}
	}

	if s := waitH3Stats(p, func(s Stats) bool { return s.InFlightStreams == 0 }, 3*time.Second); s.InFlightStreams != 0 {
		t.Fatalf("stream slots leaked by abandoned acquires: %+v (want InFlightStreams=0 after %d abandons)",
			s, abandonIters)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	mc2, err2 := p.acquire(ctx2)
	cancel2()
	if err2 != nil {
		t.Fatalf("normal acquire after %d abandons = %v, want success (pool starved by leaked slots)",
			abandonIters, err2)
	}
	p.release(mc2, nil)

	if n := waitGoroutines(before+4, 3*time.Second); n > before+4 {
		t.Errorf("goroutine leak after abandoned acquires: before=%d after=%d (want <= %d)",
			before, n, before+4)
	}
}

// TestPool_PrunedWaiter_IsStillReplied guards the exactly-one-reply invariant on
// the waiter-pruning path. An abandoning acquire whose request the actor already
// accepted spawns a reclaim goroutine that blocks on the one reply it is owed;
// if pruneExpiredWaiters dropped a queued waiter silently, that goroutine would
// hang forever — a worse leak than the one being fixed.
//
// The pool is held at its cap so abandoned acquires queue as waiters, and the
// health-check tick is fast so pruning is what retires them.
func TestPool_PrunedWaiter_IsStillReplied(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()

	p := newPool(addrs[0].String(), newConnOpts(), PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 1,
		HealthCheckPeriod: 20 * time.Millisecond,
	}, nil, nil)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	held, err := p.acquire(ctx)
	cancel()
	if err != nil {
		t.Fatalf("priming acquire: %v", err)
	}
	// held is NOT released: the pool's single slot stays occupied, so every
	// acquire below is queued as a waiter rather than served.

	before := runtime.NumGoroutine()

	const waiters = 60
	for i := 0; i < waiters; i++ {
		actx, acancel := context.WithCancel(context.Background())
		acancel()
		if _, aerr := p.acquire(actx); aerr == nil {
			t.Fatal("acquire succeeded while the pool's only slot was held")
		}
	}

	// Several ticks' worth of grace for pruning to reply to every queued waiter
	// and for each reclaim goroutine to receive it and exit.
	if n := waitGoroutines(before+4, 3*time.Second); n > before+4 {
		t.Errorf("reclaim goroutines hung on pruned waiters: before=%d after=%d (want <= %d)",
			before, n, before+4)
	}

	p.release(held, nil)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	mc2, err2 := p.acquire(ctx2)
	cancel2()
	if err2 != nil {
		t.Fatalf("acquire after pruning = %v, want success", err2)
	}
	p.release(mc2, nil)
}

// TestH3Pool_PrunedWaiter_IsStillReplied is the H3 twin of
// TestPool_PrunedWaiter_IsStillReplied.
func TestH3Pool_PrunedWaiter_IsStillReplied(t *testing.T) {
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 1,
		HealthCheckPeriod: 20 * time.Millisecond,
	}, newH3FakeDialer().dial, nil, nil)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	held, err := p.acquire(ctx)
	cancel()
	if err != nil {
		t.Fatalf("priming acquire: %v", err)
	}

	before := runtime.NumGoroutine()

	const waiters = 60
	for i := 0; i < waiters; i++ {
		actx, acancel := context.WithCancel(context.Background())
		acancel()
		if _, aerr := p.acquire(actx); aerr == nil {
			t.Fatal("acquire succeeded while the pool's only slot was held")
		}
	}

	if n := waitGoroutines(before+4, 3*time.Second); n > before+4 {
		t.Errorf("reclaim goroutines hung on pruned waiters: before=%d after=%d (want <= %d)",
			before, n, before+4)
	}

	p.release(held, nil)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	mc2, err2 := p.acquire(ctx2)
	cancel2()
	if err2 != nil {
		t.Fatalf("acquire after pruning = %v, want success", err2)
	}
	p.release(mc2, nil)
}
