package client

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ————————————————————————————————————————————————————————————————
// The health sweep must not serialise the pool.
//
// h1Pool.evictDead probes every checked-in conn with http1.Conn.ProbeIdle,
// which sets a 1ms FUTURE read deadline and blocks in Peek. On a HEALTHY idle
// socket that blocks for the whole deadline by construction — the probe only
// returns early when the peer has sent a FIN or unsolicited bytes.
//
// The sweep runs in handleTick, on the actor goroutine, and acquireCh is
// unbuffered, so for the duration of the sweep no caller can acquire or
// release. The cost is therefore (idle conns) x (probe duration), and it grows
// with MaxConnsPerHost — which for HTTP/1.1 IS the concurrency limit, so it is
// the knob a load generator raises to go faster.
//
// The invariant pinned here is a RATIO, not a wall-clock bound: acquire
// latency must not scale with the number of idle connections. An absolute
// bound would encode this machine's speed and flake on a loaded CI box; the
// ratio survives both being slow.
//
// What this test does NOT pin, and must not be read as pinning:
//   - That the sweep runs at all. A pool that never probes anything passes
//     this trivially. TestH1Pool_ProbeEvictsPeerClosedIdleConn is what covers
//     presence; the two travel together and deleting either leaves a hole.
//   - That the probe runs on a separate goroutine. Probing concurrently ON the
//     actor also keeps latency flat in n, and mutation-checking confirms this
//     test cannot tell the two apart. Off-actor is a design choice — the actor
//     does no I/O at all — that measured better, not one this test proves.
// ————————————————————————————————————————————————————————————————

// h1IdleDialer hands out the client half of a net.Pipe whose peer half is never
// written to and never closed. That is the expensive case for ProbeIdle: the
// socket is healthy and silent, so Peek blocks until the deadline expires.
//
// It keeps the peer halves alive for the pool's lifetime — a collected or
// closed peer would make Peek return immediately and erase the effect under
// test.
type h1IdleDialer struct {
	mu   sync.Mutex
	srvs []net.Conn
}

func (d *h1IdleDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	cli, srv := net.Pipe()
	d.mu.Lock()
	d.srvs = append(d.srvs, srv)
	d.mu.Unlock()
	return cli, nil
}

func (d *h1IdleDialer) dialCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.srvs)
}

func (d *h1IdleDialer) closeAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.srvs {
		_ = s.Close()
	}
}

// worstAcquireDuringSweeps fills a pool with nConns idle conns, then hammers
// acquire/release for a fixed window and returns the worst latency observed.
//
// HealthCheckPeriod is far shorter than one sweep takes, so ticks coalesce and
// the pool is sweeping almost continuously — which is what makes the worst case
// observable in a short test rather than once every 30s.
func worstAcquireDuringSweeps(t *testing.T, nConns int) time.Duration {
	t.Helper()

	d := &h1IdleDialer{}
	defer d.closeAll()

	hp := new(atomic.Pointer[Hooks])
	hp.Store(&Hooks{})

	p := newH1Pool("stall.test:80", d, PoolOptions{
		MaxConnsPerHost:   nConns,
		HealthCheckPeriod: 5 * time.Millisecond,
	}, hp, nil)
	defer func() { _ = p.Close() }()

	ctx := context.Background()

	// Open every conn, then hand them all back so the sweep finds nConns
	// checked-in conns to probe. Holding them all at once is what forces the
	// pool to dial nConns rather than reusing one: checkout is exclusive.
	held := make([]*h1ManagedConn, 0, nConns)
	for i := 0; i < nConns; i++ {
		mc, err := p.acquire(ctx)
		if err != nil {
			t.Fatalf("acquire[%d] while filling the pool: %v", i, err)
		}
		held = append(held, mc)
	}
	for _, mc := range held {
		p.release(mc, true)
	}

	var worst time.Duration
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		start := time.Now()
		mc, err := p.acquire(ctx)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("acquire during sweeps: %v", err)
		}
		p.release(mc, true)
		if elapsed > worst {
			worst = elapsed
		}
	}
	return worst
}

// TestH1Pool_HealthSweep_DoesNotScaleAcquireLatency pins that the health sweep
// does not serialise the pool in proportion to its size.
//
// With the probe on the actor goroutine, a 64-conn pool stalls ~32x longer than
// a 2-conn one, because every idle conn costs one full probe deadline and the
// actor cannot service acquireCh until the whole sweep finishes.
func TestH1Pool_HealthSweep_DoesNotScaleAcquireLatency(t *testing.T) {
	t.Parallel()

	const (
		smallPool = 2
		largePool = 64
	)

	small := worstAcquireDuringSweeps(t, smallPool)
	large := worstAcquireDuringSweeps(t, largePool)

	// A 32x difference in conn count must not buy a 32x difference in latency.
	// The slack absorbs scheduler noise and keeps the bound meaningful when
	// `small` is near zero; it is far below the effect being detected.
	limit := 4*small + 20*time.Millisecond
	if large > limit {
		t.Fatalf("acquire latency scales with pool size: %d conns -> %v, %d conns -> %v (limit %v)\n"+
			"the health sweep is serialising the pool: each idle conn costs one blocking ProbeIdle on the actor goroutine",
			smallPool, small, largePool, large, limit)
	}
	t.Logf("worst acquire latency: %d conns = %v, %d conns = %v (limit %v)", smallPool, small, largePool, large, limit)
}

// TestH1Pool_HealthSweep_DoesNotDialOverAReservedConn pins the other half of the
// reservation contract.
//
// The sweep takes every checked-in conn out of the candidate set while it probes.
// Below MaxConnsPerHost that makes pickIdle return nil for a pool that is not
// actually out of capacity, and a naive handleAcquire then dials — opening a
// socket the pool already owns. Nothing defaults IdleTimeout, so evictIdle is a
// no-op and the surplus is never reclaimed; it just ratchets up to the cap.
//
// One conn must therefore serve a strictly serial workload no matter how often
// the sweep runs.
func TestH1Pool_HealthSweep_DoesNotDialOverAReservedConn(t *testing.T) {
	t.Parallel()

	d := &h1IdleDialer{}
	defer d.closeAll()

	hp := new(atomic.Pointer[Hooks])
	hp.Store(&Hooks{})

	// Room to dial three more conns than the workload needs, and a sweep period
	// short enough that most acquires land while one is in flight.
	p := newH1Pool("reserve.test:80", d, PoolOptions{
		MaxConnsPerHost:   4,
		HealthCheckPeriod: time.Millisecond,
	}, hp, nil)
	defer func() { _ = p.Close() }()

	// releaseUnderActor hands the conn back and waits for the actor to record it.
	// release only queues a message, so without this the next acquire can arrive
	// while the conn still reads as checked out — and the pool then dials a second
	// one entirely legitimately, on main just as much as here. That race is not
	// what this test is about, so take it out of the picture.
	releaseUnderActor := func(mc *h1ManagedConn) {
		p.release(mc, true)
		for p.Stats().InFlightStreams != 0 {
		}
	}

	ctx := context.Background()
	mc, err := p.acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	releaseUnderActor(mc)
	if got := d.dialCount(); got != 1 {
		t.Fatalf("dials after one acquire = %d, want 1", got)
	}

	// Strictly serial: never more than one conn checked out, so a correct pool
	// reuses the same one forever.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		mc, err := p.acquire(ctx)
		if err != nil {
			t.Fatalf("acquire during sweeps: %v", err)
		}
		releaseUnderActor(mc)
	}

	if got := d.dialCount(); got != 1 {
		t.Fatalf("pool opened %d conns for a workload one conn can serve;\n"+
			"an acquire arriving mid-sweep dialled over the conn the sweep had reserved "+
			"instead of waiting the one probe deadline for it", got)
	}
}
