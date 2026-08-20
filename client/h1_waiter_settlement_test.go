package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// ————————————————————————————————————————————————————————————————
// What the h1 pool owes a queued waiter, at every site that can change the
// answer. Two defects, both filed off the adversarial review of #411:
//
//   - #412: the pool reaches {no live conns, no in-flight dials, waiters
//     queued} through handleTick and handleRelease as well as through
//     handleDialDone, and only the last one refused it. Left queued, a caller
//     waits a full HealthCheckPeriod while a FRESH acquire arriving an instant
//     later is refused immediately by handleAcquire's fast-refuse — the same
//     priority inversion handleDialDone's own comment says was fixed.
//
//   - #413: handleDialDone handed its dial error to the FRONT waiter. After
//     #411 the front waiters are the ones queued against a health-sweep
//     reservation, with no dial of their own, on the promise that a reserved
//     conn serves them within a probe deadline. FIFO refused the one caller
//     the pool was about to have capacity for.
//
// These are white-box, in the idiom of pool_tick_behaviour_test.go: the state
// is built directly and one handler is called on it. The states involved are
// reachable but rare — the timing needed to drive a live actor into "two conns
// reserved, three waiters, one dial failing" is exactly the timing that makes a
// test flaky, and none of what is pinned here is about timing.
// ————————————————————————————————————————————————————————————————

// h1SettleConn returns a live *http1.Conn over a pipe. IsAlive is a local flag,
// so closing the returned conn is how a test makes evictDead take it.
func h1SettleConn(t *testing.T) *http1.Conn {
	t.Helper()
	cli, srv := net.Pipe()
	t.Cleanup(func() { _ = srv.Close() })
	c := http1.NewConn(cli)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// h1SettleWaiter builds a queued acquire. The reply channel is cap-1 and used
// once, exactly as acquireOnce makes it, so replyAcquire never blocks and the
// test can read the answer without a goroutine.
func h1SettleWaiter(ctx context.Context) h1AcquireReq {
	return h1AcquireReq{ctx: ctx, reply: make(chan h1AcquireResp, 1)}
}

// h1SettleAnswer returns the reply a waiter received, and whether it got one.
func h1SettleAnswer(w h1AcquireReq) (h1AcquireResp, bool) {
	select {
	case resp := <-w.reply:
		return resp, true
	default:
		return h1AcquireResp{}, false
	}
}

// h1SettlePool builds a pool whose every timing knob is far longer than the
// test, so nothing here depends on a clock. HealthCheckPeriod is the interval
// the bugs make a waiter wait; DialBackoff is what keeps ensureDialForWaiters
// from rescuing it.
func h1SettlePool(t *testing.T) *h1Pool {
	t.Helper()
	// Cap deliberately above the conn counts used below: the reserved-idle guard
	// has to be the thing under test, not the cap. At MaxConnsPerHost == 2
	// ensureDialForWaiters would return on its cap check and mask it.
	return h1SettlePoolCap(t, 4)
}

func h1SettlePoolCap(t *testing.T, maxConns int) *h1Pool {
	t.Helper()
	p := newH1Pool("127.0.0.1:1", newH1FakeDialer(), PoolOptions{
		MaxConnsPerHost:   maxConns,
		HealthCheckPeriod: time.Hour,
		DialBackoff:       time.Hour,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// TestH1HandleTick_RefusesWaitersStrandedByTheLastConnGoing pins #412 on the
// tick path.
//
// handleTick's evictDead can take the pool's last conn while a dial backoff is
// open and waiters are queued behind it — waiters that queued with no dial of
// their own because the pool was at cap when they arrived. ensureDialForWaiters
// returns on the backoff check, and before the fix nothing else looked at them:
// the state was terminal until the NEXT tick, a full HealthCheckPeriod away.
func TestH1HandleTick_RefusesWaitersStrandedByTheLastConnGoing(t *testing.T) {
	p := h1SettlePool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dead := h1SettleConn(t)
	_ = dead.Close() // IsAlive() == false, so evictDead takes it inside the tick
	w := h1SettleWaiter(ctx)
	rs := &h1RunState{
		conns:         []*h1ManagedConn{{c: dead}},
		waiters:       []h1AcquireReq{w},
		lastDialErrAt: time.Now(), // a dial failed a moment ago: backoff is open
	}

	p.handleTick(rs)

	resp, got := h1SettleAnswer(w)
	require.Truef(t, got,
		"the tick evicted the pool's last conn during dial backoff and left the waiter queued "+
			"(waiters = %d); it now waits a full HealthCheckPeriod while a fresh acquire is "+
			"refused instantly by handleAcquire's fast-refuse", len(rs.waiters))
	assert.Truef(t, errors.Is(resp.err, ErrDialBackoff),
		"stranded waiter got %v, want ErrDialBackoff — the same answer handleAcquire "+
			"gives a new request in this exact state", resp.err)
	assert.Truef(t, resp.mc == nil, "a refused waiter was also handed a conn: %v", resp.mc)
	assert.Empty(t, rs.waiters, "waiters remained queued after every one was answered")
}

// TestH1HandleRelease_RefusesWaitersStrandedByAConnectionClose pins #412 on the
// release path, which the issue does not name and which is the likelier of the
// two to fire in production.
//
// ensureDialForWaiters' own comment says it: eviction routinely removes the last
// conn, and "a server 'Connection: close' is the ordinary trigger, not an error
// path". That release evicts, serveWaiters finds nothing, ensureDialForWaiters
// returns on backoff — and the queue is stranded exactly as in the tick case.
func TestH1HandleRelease_RefusesWaitersStrandedByAConnectionClose(t *testing.T) {
	p := h1SettlePool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mc := &h1ManagedConn{c: h1SettleConn(t), active: 1}
	w := h1SettleWaiter(ctx)
	rs := &h1RunState{
		conns:         []*h1ManagedConn{mc},
		waiters:       []h1AcquireReq{w},
		lastDialErrAt: time.Now(),
	}

	// The peer answered "Connection: close", so the transport releases the pool's
	// only conn as non-reusable.
	p.handleRelease(rs, h1ReleaseMsg{mc: mc, keepAlive: false})

	resp, got := h1SettleAnswer(w)
	require.Truef(t, got,
		"a keepAlive=false release evicted the pool's last conn during dial backoff and "+
			"left the waiter queued (waiters = %d) until the next health tick", len(rs.waiters))
	assert.Truef(t, errors.Is(resp.err, ErrDialBackoff),
		"stranded waiter got %v, want ErrDialBackoff", resp.err)
	assert.Empty(t, rs.waiters, "waiters remained queued after every one was answered")
}

// TestH1HandleDialDone_RefusesTheWaiterNoReservationCovers pins #413.
//
// A dial error must be given to a waiter that has nothing else coming, not to
// whoever is at the front. serveWaiters drains from the front, so a health
// sweep's reservations cover the front waiters — refusing one of those refuses a
// caller the pool is about to serve, roughly a probe deadline later.
//
// FIFO ordering is preserved for SERVICE; only the choice of who absorbs an
// error moves to the back of the queue.
func TestH1HandleDialDone_RefusesTheWaiterNoReservationCovers(t *testing.T) {
	dialErr := &DialError{Addr: "reserved.test:80", Err: errors.New("connection refused")}

	// Two conns held by an in-flight health sweep: alive, idle, and not
	// checkout candidates, so h1CountReservedIdle counts both.
	newReserved := func(t *testing.T) []*h1ManagedConn {
		t.Helper()
		return []*h1ManagedConn{
			{c: h1SettleConn(t), probing: true},
			{c: h1SettleConn(t), probing: true},
		}
	}

	t.Run("refuses the one waiter past what the reservations cover", func(t *testing.T) {
		p := h1SettlePool(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// A and B queued against the reservation with no dial of their own; C is
		// past what it covers, so C is the acquire that dialled.
		a, b, c := h1SettleWaiter(ctx), h1SettleWaiter(ctx), h1SettleWaiter(ctx)
		rs := &h1RunState{
			conns:         newReserved(t),
			waiters:       []h1AcquireReq{a, b, c},
			inFlightDials: 1,
		}

		p.handleDialDone(rs, h1DialResult{err: dialErr})

		resp, got := h1SettleAnswer(c)
		require.True(t, got,
			"nobody was refused, though one waiter was past every reservation")
		assert.Truef(t, errors.Is(resp.err, dialErr),
			"refused waiter got %v, want the dial error", resp.err)
		for _, tc := range []struct {
			name string
			w    h1AcquireReq
		}{{"A", a}, {"B", b}} {
			_, answered := h1SettleAnswer(tc.w)
			assert.Falsef(t, answered,
				"waiter %s was refused a dial error, but the health sweep is holding a conn "+
					"for it — serveWaiters drains from the front, so %s is served the moment "+
					"handleSweepDone clears the reservation", tc.name, tc.name)
		}
		require.Lenf(t, rs.waiters, 2,
			"queue = %d waiters, want exactly [A B] still queued in arrival order", len(rs.waiters))
		assert.Equal(t, a.reply, rs.waiters[0].reply, "A must stay at the front: FIFO holds for SERVICE")
		assert.Equal(t, b.reply, rs.waiters[1].reply, "B must stay behind A: FIFO holds for SERVICE")
	})

	t.Run("refuses nobody when every waiter is covered", func(t *testing.T) {
		p := h1SettlePool(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Both queued waiters have a reserved conn coming for them. The failed
		// dial was started for a caller the reservations can absorb, so refusing
		// anyone here refuses someone the pool is about to serve.
		a, b := h1SettleWaiter(ctx), h1SettleWaiter(ctx)
		rs := &h1RunState{
			conns:         newReserved(t),
			waiters:       []h1AcquireReq{a, b},
			inFlightDials: 1,
		}

		p.handleDialDone(rs, h1DialResult{err: dialErr})

		for _, tc := range []struct {
			name string
			w    h1AcquireReq
		}{{"A", a}, {"B", b}} {
			resp, answered := h1SettleAnswer(tc.w)
			assert.Falsef(t, answered,
				"waiter %s got %v; both waiters are covered by a reservation, so the dial "+
					"error belongs to nobody in this queue", tc.name, resp.err)
		}
		assert.Len(t, rs.waiters, 2, "both waiters must still be queued")
	})
}

// TestH1EnsureDialForWaiters_DialsForEveryUncoveredWaiter answers the third
// question #412 raises: whether ensureDialForWaiters should start more than one
// dial.
//
// It was written as a backstop that rescues a terminal state, where exactly one
// dial is right. #411 made it something else as well — the path that has to
// re-dial for a whole BATCH at once, because handleAcquire now queues callers
// against a health-sweep reservation with no dial of their own. When that
// reservation comes back dead, k waiters are re-dialled one at a time: each dial
// completes, serveWaiters hands the conn to one waiter, and only then does the
// next dial start. k sequential round-trips where k parallel acquires on the
// pre-#411 pool would each have dialled for themselves.
//
// The cost scales with MaxConnsPerHost, which for HTTP/1.1 IS the concurrency
// limit — so, as with the stall #411 removed, it grows with the knob a load
// generator raises to go faster, and it is worst on the link where a round trip
// is expensive.
func TestH1EnsureDialForWaiters_DialsForEveryUncoveredWaiter(t *testing.T) {
	t.Run("a dead sweep re-dials for the whole batch at once", func(t *testing.T) {
		p := h1SettlePool(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Three conns held by the sweep, three callers queued against them, and
		// the probe comes back saying every one is dead.
		reserved := []*h1ManagedConn{
			{c: h1SettleConn(t), probing: true},
			{c: h1SettleConn(t), probing: true},
			{c: h1SettleConn(t), probing: true},
		}
		rs := &h1RunState{
			conns:   reserved,
			waiters: []h1AcquireReq{h1SettleWaiter(ctx), h1SettleWaiter(ctx), h1SettleWaiter(ctx)},
		}

		// probed and dead are copies, as they are in production: startHealthSweep
		// builds its candidate slice by appending, and runHealthSweep filters dead
		// out of its own array, so neither ever aliases rs.conns. handing the same
		// slice three times would let evict's in-place filtering rewrite the list
		// being ranged over.
		p.handleSweepDone(rs, h1SweepResult{
			probed: append([]*h1ManagedConn(nil), reserved...),
			dead:   append([]*h1ManagedConn(nil), reserved...),
		})

		assert.Equalf(t, 3, rs.inFlightDials,
			"inFlightDials = %d for 3 waiters with nothing coming, want 3; "+
				"one dial per call serialises the batch into 3 round trips, and the "+
				"acquires that queued against the reservation would each have dialled "+
				"for themselves before #411", rs.inFlightDials)
	})

	t.Run("never dials past MaxConnsPerHost", func(t *testing.T) {
		p := h1SettlePoolCap(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rs := &h1RunState{
			waiters: []h1AcquireReq{h1SettleWaiter(ctx), h1SettleWaiter(ctx), h1SettleWaiter(ctx)},
		}

		p.ensureDialForWaiters(rs)

		assert.Equalf(t, 2, rs.inFlightDials,
			"inFlightDials = %d for 3 waiters at MaxConnsPerHost=2, want 2", rs.inFlightDials)
	})

	t.Run("a dial already in flight covers a waiter", func(t *testing.T) {
		p := h1SettlePool(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// One waiter, one dial already on its way for it. A second socket would
		// be surplus the pool never reclaims — nothing defaults IdleTimeout, so
		// evictIdle is disabled and the count just ratchets towards the cap. Same
		// failure mode #411's second commit fixed for reserved conns.
		rs := &h1RunState{
			waiters:       []h1AcquireReq{h1SettleWaiter(ctx)},
			inFlightDials: 1,
		}

		p.ensureDialForWaiters(rs)

		assert.Equalf(t, 1, rs.inFlightDials,
			"inFlightDials = %d for 1 waiter that already has a dial coming, want 1", rs.inFlightDials)
	})
}
