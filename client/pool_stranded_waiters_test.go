package client

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// ————————————————————————————————————————————————————————————————
// #425: the H2 and H3 pools strand queued waiters on their EVICTION paths, the
// way h1Pool did before #412.
//
// {no live conns, no in-flight dials, waiters queued, dial backoff open} is
// terminal. EnsureDialForWaiters / dialForWaiters returns on the backoff check,
// so nothing re-enters the decision until the next tick — a whole
// HealthCheckPeriod — while a FRESH acquire arriving an instant later is
// refused immediately by handleAcquire's fast-refuse on the same three
// conditions. Both pools refuse that state in HandleDialDone and nowhere else,
// so it is reachable through eviction but not answerable there.
//
// What does NOT port from #424: the back-of-queue refusal (#413). It exists on
// h1 because a health-sweep reservation covers the FRONT of the queue, and
// neither sibling has a reservation — h1CountReservedIdle came from #411's
// off-actor probe and is HTTP/1.1-only. FIFO stays right here.
//
// White-box, in the pool_tick_behaviour_test.go idiom: build the state, call
// one handler, read the reply. Nothing pinned here is about timing.
// ————————————————————————————————————————————————————————————————

// h2DeadConn returns a real *conn.Conn that has been closed, so IsAlive is
// false. It has to be real rather than hand-built: every eviction path calls
// Close on it, and conn.Conn's liveness flags are unexported.
func h2DeadConn(t *testing.T) *conn.Conn {
	t.Helper()
	cli, srv := net.Pipe()
	stopSrv := make(chan struct{})
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		runFakeH2Server(srv, func(*frame.Framer) { <-stopSrv })
	}()
	t.Cleanup(func() { close(stopSrv); <-srvDone })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := conn.NewClientConn(ctx, cli, conn.ConnOptions{})
	require.NoError(t, err, "NewClientConn")
	t.Cleanup(func() { _ = c.Close() })
	_ = c.Close() // IsAlive() == false from here on
	return c
}

// h2StrandPool builds an H2 pool whose timing knobs are all far longer than the
// test: HealthCheckPeriod is the interval the bug makes a waiter wait, and
// DialBackoff is what keeps EnsureDialForWaiters from rescuing it.
func h2StrandPool(t *testing.T) *Pool {
	t.Helper()
	p := newPool("127.0.0.1:1", newConnOpts(), PoolOptions{
		MaxConnsPerHost:   4,
		HealthCheckPeriod: time.Hour,
		DialBackoff:       time.Hour,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func h2StrandWaiter(ctx context.Context) AcquireReq {
	return AcquireReq{Ctx: ctx, Reply: make(chan AcquireResp, 1)}
}

// assertRefusedWithBackoff checks the single reply a stranded waiter is owed.
func assertRefusedWithBackoff(t *testing.T, err error, answered bool, mc any, left int, site string) {
	t.Helper()
	require.Truef(t, answered,
		"%s lost its last live conn during dial backoff and left the waiter queued "+
			"(waiters = %d); it now waits a full HealthCheckPeriod while a fresh acquire is "+
			"refused instantly by handleAcquire's fast-refuse", site, left)
	require.ErrorIsf(t, err, ErrDialBackoff,
		"%s: stranded waiter got %v, want ErrDialBackoff — the same answer "+
			"handleAcquire gives a new request in this exact state", site, err)
	// mc is `any` holding a *managedConn / *h3ManagedConn: require.Nil accepts a
	// typed nil through reflection, so compare the interface against nil.
	require.Truef(t, mc == nil, "%s: a refused waiter was also handed a conn", site)
	require.Zerof(t, left, "%s: waiters = %d after every one was answered, want 0", site, left)
}

// TestPool_HandleTick_RefusesWaitersStrandedByTheLastConnGoing — H2, tick path.
func TestPool_HandleTick_RefusesWaitersStrandedByTheLastConnGoing(t *testing.T) {
	p := h2StrandPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := h2StrandWaiter(ctx)
	rs := &RunState{
		Conns:         []*managedConn{{C: h2DeadConn(t)}},
		Waiters:       []AcquireReq{w},
		LastDialErrAt: time.Now(), // a dial failed a moment ago: backoff is open
	}

	p.HandleTick(rs)

	var (
		resp     AcquireResp
		answered bool
	)
	select {
	case resp = <-w.Reply:
		answered = true
	default:
	}
	var got any
	if resp.Mc != nil {
		got = resp.Mc
	}
	assertRefusedWithBackoff(t, resp.Err, answered, got, len(rs.Waiters), "Pool.HandleTick")
}

// TestPool_HandleRelease_RefusesWaitersStrandedByTheLastConnGoing — H2, release
// path. HandleRelease is the pool's ONLY eviction site for a conn still
// carrying traffic (evictDead and evictDeadSilent both defer to it), so it is
// where a GOAWAY'd conn is finally reaped after RFC 7540 §6.8's drain — the
// ordinary way this pool loses its last conn, not an error path.
func TestPool_HandleRelease_RefusesWaitersStrandedByTheLastConnGoing(t *testing.T) {
	p := h2StrandPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mc := &managedConn{C: h2DeadConn(t), Active: 1}
	w := h2StrandWaiter(ctx)
	rs := &RunState{
		Conns:         []*managedConn{mc},
		Waiters:       []AcquireReq{w},
		LastDialErrAt: time.Now(),
	}

	p.HandleRelease(rs, ReleaseMsg{Mc: mc})

	var (
		resp     AcquireResp
		answered bool
	)
	select {
	case resp = <-w.Reply:
		answered = true
	default:
	}
	var got any
	if resp.Mc != nil {
		got = resp.Mc
	}
	assertRefusedWithBackoff(t, resp.Err, answered, got, len(rs.Waiters), "Pool.HandleRelease")
}

// TestH3Pool_HandleTick_RefusesWaitersStrandedByTheLastConnGoing — H3, tick path.
func TestH3Pool_HandleTick_RefusesWaitersStrandedByTheLastConnGoing(t *testing.T) {
	p := inertH3Pool(PoolOptions{
		MaxConnsPerHost:   4,
		HealthCheckPeriod: time.Hour,
		DialBackoff:       time.Hour,
	}, nil)
	// No t.Cleanup(p.Close): inertH3Pool never starts the actor, so Close would
	// block forever on closedCh. The existing h3 white-box tests do the same.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dead := &barrierH3Client{}
	atomic.StoreInt32(&dead.dead, 1) // Alive() == false, so evictDead takes it
	w := h3AcquireReq{ctx: ctx, reply: make(chan h3AcquireResp, 1)}
	rs := &h3RunState{
		conns:         []*h3ManagedConn{{cl: dead, streamCap: 10}},
		waiters:       []h3AcquireReq{w},
		lastDialErrAt: time.Now(),
	}

	p.handleTick(rs)

	var (
		resp     h3AcquireResp
		answered bool
	)
	select {
	case resp = <-w.reply:
		answered = true
	default:
	}
	var got any
	if resp.mc != nil {
		got = resp.mc
	}
	assertRefusedWithBackoff(t, resp.err, answered, got, len(rs.waiters), "h3Pool.handleTick")
}

// TestH3Pool_HandleRelease_RefusesWaitersStrandedByAGoAwayDrain — H3, release
// path, driven the way RFC 9114 §5.2 says it happens: the conn is GOAWAY'd and
// draining, PickLeastLoaded has already stopped offering it, and its last
// in-flight exchange releasing is what finally retires it.
//
// This is also the case where h3CountLive == 0 while len(conns) == 1 on entry —
// a GOAWAY'd conn is not live, deliberately, because it can never become
// capacity for a waiter.
func TestH3Pool_HandleRelease_RefusesWaitersStrandedByAGoAwayDrain(t *testing.T) {
	p := inertH3Pool(PoolOptions{
		MaxConnsPerHost:   4,
		HealthCheckPeriod: time.Hour,
		DialBackoff:       time.Hour,
	}, nil)
	// No t.Cleanup(p.Close): inertH3Pool never starts the actor, so Close would
	// block forever on closedCh. The existing h3 white-box tests do the same.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gone := &barrierH3Client{}
	atomic.StoreInt32(&gone.goaway, 1)
	mc := &h3ManagedConn{cl: gone, active: 1, streamCap: 10}
	w := h3AcquireReq{ctx: ctx, reply: make(chan h3AcquireResp, 1)}
	rs := &h3RunState{
		conns:         []*h3ManagedConn{mc},
		waiters:       []h3AcquireReq{w},
		lastDialErrAt: time.Now(),
	}

	p.handleRelease(rs, h3ReleaseMsg{mc: mc})

	var (
		resp     h3AcquireResp
		answered bool
	)
	select {
	case resp = <-w.reply:
		answered = true
	default:
	}
	var got any
	if resp.mc != nil {
		got = resp.mc
	}
	assertRefusedWithBackoff(t, resp.err, answered, got, len(rs.waiters), "h3Pool.handleRelease")
}

// TestPool_HandleTick_FlushesWithADrainingConnStillInTheSlice pins the choice of
// CountLive over len(conns) on the H2 pool.
//
// evictDead defers a dead conn that still has streams to HandleRelease, so the
// tick can and does leave one in the slice. It is not capacity: draining streams
// can never serve a waiter, and the conn is never picked again. A flush keyed on
// len(conns) would sit on the queue until that last stream happened to end.
func TestPool_HandleTick_FlushesWithADrainingConnStillInTheSlice(t *testing.T) {
	p := h2StrandPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	draining := &managedConn{C: h2DeadConn(t), Active: 1}
	w := h2StrandWaiter(ctx)
	rs := &RunState{
		Conns:         []*managedConn{draining},
		Waiters:       []AcquireReq{w},
		LastDialErrAt: time.Now(),
	}

	p.HandleTick(rs)

	// Without this the test could pass for the wrong reason — an empty slice
	// makes CountLive and len(conns) agree, and the distinction goes untested.
	require.Lenf(t, rs.Conns, 1,
		"conns = %d after the tick, want the draining conn still there; "+
			"evictDead's active == 0 guard is what puts this test in the CountLive-only case",
		len(rs.Conns))
	require.Zerof(t, CountLive(rs.Conns),
		"CountLive = %d, want 0 — the draining conn must not read as capacity",
		CountLive(rs.Conns))
	var (
		resp     AcquireResp
		answered bool
	)
	select {
	case resp = <-w.Reply:
		answered = true
	default:
	}
	var got any
	if resp.Mc != nil {
		got = resp.Mc
	}
	assertRefusedWithBackoff(t, resp.Err, answered, got, len(rs.Waiters),
		"Pool.HandleTick with a draining conn")
}

// TestH3Pool_HandleTick_FlushesWithADrainingConnStillInTheSlice is the H3 twin.
// Here the conn is GOAWAY'd rather than dead: h3CountLive excludes it by RFC
// 9114 §5.2, for the same reason and with the same consequence.
func TestH3Pool_HandleTick_FlushesWithADrainingConnStillInTheSlice(t *testing.T) {
	p := inertH3Pool(PoolOptions{
		MaxConnsPerHost:   4,
		HealthCheckPeriod: time.Hour,
		DialBackoff:       time.Hour,
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gone := &barrierH3Client{}
	atomic.StoreInt32(&gone.goaway, 1)
	draining := &h3ManagedConn{cl: gone, active: 1, streamCap: 10}
	w := h3AcquireReq{ctx: ctx, reply: make(chan h3AcquireResp, 1)}
	rs := &h3RunState{
		conns:         []*h3ManagedConn{draining},
		waiters:       []h3AcquireReq{w},
		lastDialErrAt: time.Now(),
	}

	p.handleTick(rs)

	require.Lenf(t, rs.conns, 1,
		"conns = %d after the tick, want the draining conn still there", len(rs.conns))
	require.Zerof(t, h3CountLive(rs.conns),
		"h3CountLive = %d, want 0 — a GOAWAY'd conn must not read as capacity",
		h3CountLive(rs.conns))
	var (
		resp     h3AcquireResp
		answered bool
	)
	select {
	case resp = <-w.reply:
		answered = true
	default:
	}
	var got any
	if resp.mc != nil {
		got = resp.mc
	}
	assertRefusedWithBackoff(t, resp.err, answered, got, len(rs.waiters),
		"h3Pool.handleTick with a draining conn")
}

// TestFlushStrandedWaiters_KeepsTheQueueWhenTheBackoffIsClosed pins the guard
// that both pools' flush carries for a reason the handlers cannot demonstrate.
//
// Through any call site the state is unreachable: EnsureDialForWaiters /
// dialForWaiters would have started a dial (nothing live and nothing in flight
// is below any cap, and the backoff is what stops it), so inFlightDials would be
// non-zero and the flush would return one check earlier. Deleting the guard is
// therefore an EQUIVALENT MUTANT through the handlers — measured, both "drop the
// backoff guard" mutations survive the whole suite.
//
// It is still load-bearing as a contract. Nothing-live with the backoff closed
// means a dial is owed, not that the caller should be refused, and a future call
// site reaching this state must not get the opposite answer. Calling it directly
// is the only way to say so — and it works: replacing the guard's condition with
// `false` is CAUGHT 2/2 here while surviving every handler-level test.
func TestFlushStrandedWaiters_KeepsTheQueueWhenTheBackoffIsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two hours since the last dial error against a one-hour backoff: closed.
	expired := time.Now().Add(-2 * time.Hour)

	t.Run("h2", func(t *testing.T) {
		p := h2StrandPool(t)
		w := h2StrandWaiter(ctx)
		rs := &RunState{Waiters: []AcquireReq{w}, LastDialErrAt: expired}

		p.FlushStrandedWaiters(rs, ErrDialBackoff)

		require.Lenf(t, rs.Waiters, 1,
			"waiters = %d, want 1 — with the backoff closed this waiter is owed a "+
				"dial, not a refusal", len(rs.Waiters))
		select {
		case resp := <-w.Reply:
			require.Failf(t, "waiter refused while the pool was free to dial",
				"waiter refused with %v while the pool was free to dial for it", resp.Err)
		default:
		}
	})

	t.Run("h3", func(t *testing.T) {
		p := inertH3Pool(PoolOptions{MaxConnsPerHost: 4, DialBackoff: time.Hour}, nil)
		w := h3AcquireReq{ctx: ctx, reply: make(chan h3AcquireResp, 1)}
		rs := &h3RunState{waiters: []h3AcquireReq{w}, lastDialErrAt: expired}

		p.flushStrandedWaiters(rs, ErrDialBackoff)

		require.Lenf(t, rs.waiters, 1,
			"waiters = %d, want 1 — with the backoff closed this waiter is owed a "+
				"dial, not a refusal", len(rs.waiters))
		select {
		case resp := <-w.reply:
			require.Failf(t, "waiter refused while the pool was free to dial",
				"waiter refused with %v while the pool was free to dial for it", resp.err)
		default:
		}
	})
}

// TestPool_HandleDialDone_RefusesTheWaitersBehindTheFirst gives the H2 pool the
// deterministic gate its H3 sibling already had (the behind_the_first subtest of
// TestH3Pool_DialFailureDoesNotStrandQueuedWaiters), and that this file's hoist
// otherwise leans on a racy one.
//
// TestPool_DialFailureRefusesEveryQueuedWaiter is the existing cover, and it is
// a coin flip: refusingDialer fails instantly, so whether waiters 2 and 3 reach
// acquireCh before HandleDialDone runs decides whether they are refused by the
// flush under test or by handleAcquire's fast-refuse on arrival. Measured —
// deleting the flush from HandleDialDone made it FAIL on one run and PASS on the
// next, same code both times.
func TestPool_HandleDialDone_RefusesTheWaitersBehindTheFirst(t *testing.T) {
	p := h2StrandPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sentinel := errors.New("connect refused (synthetic)")
	a, b, c := h2StrandWaiter(ctx), h2StrandWaiter(ctx), h2StrandWaiter(ctx)
	rs := &RunState{Waiters: []AcquireReq{a, b, c}, InFlightDials: 1}

	p.HandleDialDone(rs, DialResult{Err: sentinel})

	for i, w := range []AcquireReq{a, b, c} {
		select {
		case resp := <-w.Reply:
			require.Errorf(t, resp.Err, "waiter %d got a conn from a pool with nothing live", i)
			require.ErrorIsf(t, resp.Err, sentinel,
				"waiter %d got %v, want the dial error", i, resp.Err)
		default:
			require.Failf(t, "waiter left queued",
				"waiter %d left queued with nothing live and no dial in flight; it waits a "+
					"full HealthCheckPeriod while a fresh acquire is refused instantly", i)
		}
	}
	require.Emptyf(t, rs.Waiters,
		"%d Waiters still queued after every one was answered", len(rs.Waiters))
}
