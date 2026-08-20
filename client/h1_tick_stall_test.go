package client

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
//   - That the HEALTH SWEEP specifically runs. The reads counter below proves
//     only that the pool touched the sockets at all — checkout-time residue
//     peeks read them too — which rules out the degenerate "a pool that never
//     looks at a socket passes trivially" case and nothing more.
//     TestH1Pool_ProbeEvictsPeerClosedIdleConn is what covers the sweep's
//     presence; the two travel together and deleting either leaves a hole.
//   - That the probe runs on a separate goroutine. Probing concurrently ON the
//     actor also keeps latency flat in n, and mutation-checking confirms this
//     test cannot tell the two apart. Off-actor is a design choice — the actor
//     does no I/O at all — that measured better, not one this test proves.
// ————————————————————————————————————————————————————————————————

// h1CountingConn forwards everything to the pipe half beneath it and counts the
// reads the pool performs, so a run in which the pool never touched a socket can
// be told apart from a real pass. Nothing else is intercepted: net.Pipe is not a
// syscall.Conn, so no HasResidue fast path exists here to preserve.
type h1CountingConn struct {
	net.Conn
	reads *atomic.Int64
}

func (c *h1CountingConn) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(p)
}

// h1IdleDialer hands out the client half of a net.Pipe whose peer half is never
// written to and never closed. That is the expensive case for ProbeIdle: the
// socket is healthy and silent, so Peek blocks until the deadline expires.
//
// It keeps the peer halves alive for the pool's lifetime — a collected or
// closed peer would make Peek return immediately and erase the effect under
// test.
type h1IdleDialer struct {
	mu    sync.Mutex
	srvs  []net.Conn
	reads atomic.Int64
}

func (d *h1IdleDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	cli, srv := net.Pipe()
	d.mu.Lock()
	d.srvs = append(d.srvs, srv)
	d.mu.Unlock()
	return &h1CountingConn{Conn: cli, reads: &d.reads}, nil
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
// acquire/release for a fixed window and returns the worst latency observed and
// the number of socket reads the pool performed while doing it.
//
// HealthCheckPeriod is far shorter than one sweep takes, so ticks coalesce and
// the pool is sweeping almost continuously — which is what makes the worst case
// observable in a short test rather than once every 30s.
func worstAcquireDuringSweeps(t *testing.T, nConns int) (time.Duration, int64) {
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
		require.NoErrorf(t, err, "acquire[%d] while filling the pool", i)
		held = append(held, mc)
	}
	for _, mc := range held {
		p.release(mc, true)
	}

	before := d.reads.Load()
	var worst time.Duration
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		start := time.Now()
		mc, err := p.acquire(ctx)
		elapsed := time.Since(start)
		require.NoError(t, err, "acquire during sweeps")
		p.release(mc, true)
		if elapsed > worst {
			worst = elapsed
		}
	}
	return worst, d.reads.Load() - before
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

	small, smallReads := worstAcquireDuringSweeps(t, smallPool)
	large, largeReads := worstAcquireDuringSweeps(t, largePool)

	// A 32x difference in conn count must not buy a 32x difference in latency.
	// The slack absorbs scheduler noise and keeps the bound meaningful when
	// `small` is near zero; it is far below the effect being detected.
	limit := 4*small + 20*time.Millisecond
	// Logged BEFORE the assertion, so the numbers survive a failure: a bound
	// quoted without the measurement behind it is how a tolerance ends up
	// passing with the mechanism it guards removed.
	t.Logf("worst acquire latency: %d conns = %v (%d socket reads), %d conns = %v (%d socket reads), limit %v",
		smallPool, small, smallReads, largePool, large, largeReads, limit)
	require.Positive(t, smallReads,
		"the pool never read a socket in the small arm: nothing was measured, and a "+
			"pool that never probes anything satisfies the latency bound trivially")
	require.Positive(t, largeReads,
		"the pool never read a socket in the large arm: nothing was measured")
	assert.LessOrEqualf(t, large, limit,
		"acquire latency scales with pool size: %d conns -> %v, %d conns -> %v (limit %v)\n"+
			"the health sweep is serialising the pool: each idle conn costs one blocking "+
			"ProbeIdle on the actor goroutine",
		smallPool, small, largePool, large, limit)
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
	require.NoError(t, err, "first acquire")
	releaseUnderActor(mc)
	require.Equal(t, 1, d.dialCount(), "one acquire must produce exactly one dial")

	// Strictly serial: never more than one conn checked out, so a correct pool
	// reuses the same one forever.
	acquires := 0
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		mc, err := p.acquire(ctx)
		require.NoError(t, err, "acquire during sweeps")
		releaseUnderActor(mc)
		acquires++
	}

	t.Logf("%d serial acquire/release cycles over 300ms produced %d dials (%d socket reads)",
		acquires, d.dialCount(), d.reads.Load())
	require.Positivef(t, acquires,
		"no acquire completed inside the window, so no acquire could have landed mid-sweep")
	assert.Equalf(t, 1, d.dialCount(),
		"pool opened %d conns for a workload one conn can serve;\n"+
			"an acquire arriving mid-sweep dialled over the conn the sweep had reserved "+
			"instead of waiting the one probe deadline for it", d.dialCount())
}
