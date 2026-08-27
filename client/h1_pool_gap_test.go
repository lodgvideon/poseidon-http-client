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
// Four properties of the HTTP/1.1 pool that the suite named but never pinned
// (#870, #882, #883, #884). Each one had a mutation that survived the whole
// package, and in three of the four the reason is the same: the observable the
// test polled could be produced by a SECOND site, so the two covered for each
// other.
// ————————————————————————————————————————————————————————————————

// h1CloseTally records OnConnClose events under a mutex. The hook fires on the
// pool's actor goroutine, so an unguarded slice here is a data race the test
// itself would own.
type h1CloseTally struct {
	mu      sync.Mutex
	events  []ConnCloseEvent
	metrics *Metrics
	ref     *atomic.Pointer[Hooks]
}

func newH1CloseTally() *h1CloseTally {
	tl := &h1CloseTally{metrics: &Metrics{}}
	tl.ref = &atomic.Pointer[Hooks]{}
	tl.ref.Store(&Hooks{OnConnClose: func(e ConnCloseEvent) {
		tl.mu.Lock()
		defer tl.mu.Unlock()
		tl.events = append(tl.events, e)
	}})
	return tl
}

func (tl *h1CloseTally) snapshot() []ConnCloseEvent {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return append([]ConnCloseEvent(nil), tl.events...)
}

// h1GapPool builds a pool whose every timing knob is out of reach of the test,
// so nothing below depends on a clock, and whose close events are recorded.
func h1GapPool(t *testing.T, tl *h1CloseTally) *h1Pool {
	t.Helper()
	p := newH1Pool("gap.test:80", newH1FakeDialer(), PoolOptions{
		MaxConnsPerHost:   4,
		HealthCheckPeriod: time.Hour,
		DialBackoff:       time.Hour,
	}, tl.ref, tl.metrics)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// TestH1Pool_HandleRelease_EvictsADeadConnWithoutAskingForStats pins the
// release-path eviction on its own (#870).
//
// TestH1Pool_DeadConn_EvictedOnRelease polls p.Stats() for the verdict, and
// handleStats runs evictDeadSilent inside that very call — so the observable it
// waits for is produced by EITHER handleRelease's dead-conn branch OR the Stats
// path, and each covers for the other. Both mutations survive it 2/2.
//
// The release path is the one that matters operationally: it is what stops a
// dead socket being handed to the NEXT request. A production caller may never
// ask for PoolStats at all. So this calls handleRelease directly and reads
// rs.conns, which no eviction site but this one can change, and checks the
// reason too — evictDeadSilent fires no hook, so a CloseDead event is proof the
// release path did the work.
func TestH1Pool_HandleRelease_EvictsADeadConnWithoutAskingForStats(t *testing.T) {
	tl := newH1CloseTally()
	p := h1GapPool(t, tl)
	dead := h1SettleConn(t)
	require.NoError(t, dead.Close(), "close the conn under the pool")
	require.Falsef(t, dead.IsAlive(),
		"the fixture conn is still alive, so the dead-conn branch cannot be reached at all")
	mc := &h1ManagedConn{p: p, c: dead, active: 1}
	rs := &h1RunState{conns: []*h1ManagedConn{mc}}

	p.handleRelease(rs, h1ReleaseMsg{mc: mc, keepAlive: true})

	assert.Emptyf(t, rs.conns,
		"a conn the peer closed under us survived release with a keep-alive verdict; "+
			"the next acquire is handed a dead socket, and Stats() is not called on the "+
			"request path that would tidy it up")
	ev := tl.snapshot()
	require.Lenf(t, ev, 1,
		"want exactly one OnConnClose for the evicted conn, got %d — the Stats path evicts "+
			"silently, so a missing event means the release path did not do the eviction", len(ev))
	assert.Equalf(t, CloseDead, ev[0].Reason,
		"release-path eviction of a dead socket must be attributed CloseDead, got %v; "+
			"CloseManual is what the !keepAlive branch means and conflating them makes "+
			"peer-initiated churn indistinguishable from our own", ev[0].Reason)
}

// TestH1Pool_HandleRelease_KeepAliveLiveConnStaysPooled is the other direction:
// eviction on release must be conditional, not unconditional. Without it the
// test above is satisfied by a handleRelease that evicts everything.
func TestH1Pool_HandleRelease_KeepAliveLiveConnStaysPooled(t *testing.T) {
	tl := newH1CloseTally()
	p := h1GapPool(t, tl)
	live := h1SettleConn(t)
	require.Truef(t, live.IsAlive(), "the fixture conn must start alive")
	mc := &h1ManagedConn{p: p, c: live, active: 1}
	rs := &h1RunState{conns: []*h1ManagedConn{mc}}

	p.handleRelease(rs, h1ReleaseMsg{mc: mc, keepAlive: true})

	assert.Lenf(t, rs.conns, 1,
		"a live conn released with a keep-alive verdict was dropped; the pool re-dials for "+
			"every request and connection reuse — the point of the pool — stops happening")
	assert.Emptyf(t, tl.snapshot(),
		"a conn that was not evicted still fired OnConnClose, which double-counts churn")
}

// TestH1Pool_DoubleRelease_WithAWaiterQueued_ServesExactlyOne covers the double
// release against a pool that has a caller PARKED (#882).
//
// TestH1PoolTransport_ReleaseIsIdempotent covers the double release against an
// IDLE pool. With a waiter queued it is a different program path: handleRelease
// runs serveWaiters a second time on the same conn, so the second release both
// drives active to -1 and hands a second exchange the one HTTP/1.1 socket —
// which has no multiplexing, so that is two requests interleaved on one wire.
//
// The assertion is InFlightStreams, not "a third acquire blocks": the pool's own
// count of checked-out conns is what goes wrong first, and it names the
// mechanism. After the second (no-op) release the waiter still holds the conn,
// so the count must be 1. Without the guard the second release decrements it to
// 0 while the waiter is still using it.
func TestH1Pool_DoubleRelease_WithAWaiterQueued_ServesExactlyOne(t *testing.T) {
	p := newH1Pool("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	pt := &h1PoolTransport{p: p}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ex1, _, _, _, err := pt.openExchange(ctx)
	require.NoError(t, err, "first openExchange")
	parked := make(chan protoStream, 1)
	go func() {
		ex2, _, _, _, oerr := pt.openExchange(ctx)
		if oerr != nil {
			close(parked)
			return
		}
		parked <- ex2
	}()
	require.Eventuallyf(t, func() bool { return p.Stats().Waiters == 1 },
		5*time.Second, time.Millisecond,
		"no caller ever parked on the pool, so the double release never runs serveWaiters "+
			"twice and the path under test is not exercised")

	ex1.(*h1Exchange).release(true)
	ex1.(*h1Exchange).release(true) // the second must be a no-op

	var ex2 protoStream
	select {
	case ex2 = <-parked:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the parked caller was never served", "release did not free the slot")
	}
	require.NotNilf(t, ex2, "the parked caller was refused rather than served")
	t.Cleanup(func() { ex2.(*h1Exchange).release(true) })
	st := p.Stats()
	assert.Equalf(t, 1, st.InFlightStreams,
		"the pool counts %d exchanges in flight while the served waiter holds the only "+
			"connection; a second release decremented active for an exchange that had already "+
			"given the conn back, so the next caller is handed a socket that is still in use",
		st.InFlightStreams)
	assert.Equalf(t, 0, st.Waiters,
		"exactly one waiter may be served by one release, got %d still queued", st.Waiters)
}

// TestH1Pool_ServeWaiters_IsStrictlyFIFO pins the service half of the waiter
// rule (#884).
//
// handleDialDone refuses a dial ERROR from the back of the queue on purpose, and
// says why: "This perturbs FIFO for ERRORS ONLY. Successful service stays
// strictly in arrival order through serveWaiters, which is what the rest of this
// pool assumes of the waiter queue." The error half is pinned in both
// directions. The service half was not asserted anywhere — no test called
// serveWaiters or observed the order parked callers were served in.
//
// It matters because the FRONT of this queue is where health-sweep reservations
// land: handleAcquire parks a caller with no dial of its own while
// len(waiters) < h1CountReservedIdle. A serveWaiters that drained from the back
// would starve exactly the caller the pool promised to serve first.
func TestH1Pool_ServeWaiters_IsStrictlyFIFO(t *testing.T) {
	p := h1SettlePool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b, c := h1SettleWaiter(ctx), h1SettleWaiter(ctx), h1SettleWaiter(ctx)
	mc := &h1ManagedConn{p: p, c: h1SettleConn(t)}
	rs := &h1RunState{conns: []*h1ManagedConn{mc}, waiters: []h1AcquireReq{a, b, c}}

	served := make([]h1AcquireReq, 0, 3)
	for range 3 {
		rs.waiters = p.serveWaiters(rs.conns, rs.waiters)
		for _, w := range []h1AcquireReq{a, b, c} {
			if _, got := h1SettleAnswer(w); got {
				served = append(served, w)
			}
		}
		mc.active = 0 // the served exchange gives the one conn back
	}

	require.Lenf(t, served, 3,
		"only %d of three parked callers were ever served from one conn released between "+
			"rounds; a waiter the pool has capacity for must not be left queued", len(served))
	assert.Truef(t, served[0].reply == a.reply,
		"round 1 served the wrong caller: the oldest waiter must go first, or the front of "+
			"the queue — where health-sweep reservations park with no dial of their own — starves")
	assert.Truef(t, served[1].reply == b.reply, "round 2 broke arrival order")
	assert.Truef(t, served[2].reply == c.reply, "round 3 broke arrival order")
	assert.Emptyf(t, rs.waiters, "waiters remained queued after every one was served")
}

// TestH1Pool_ServeWaiters_ServesNobodyWithoutACapableConn is the negative arm:
// FIFO order is only half the rule, and a serveWaiters that answered everybody
// unconditionally would satisfy the test above.
func TestH1Pool_ServeWaiters_ServesNobodyWithoutACapableConn(t *testing.T) {
	p := h1SettlePool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := h1SettleWaiter(ctx), h1SettleWaiter(ctx)
	busy := &h1ManagedConn{p: p, c: h1SettleConn(t), active: h1ConnStreamCap}
	rs := &h1RunState{conns: []*h1ManagedConn{busy}, waiters: []h1AcquireReq{a, b}}

	rs.waiters = p.serveWaiters(rs.conns, rs.waiters)

	assert.Lenf(t, rs.waiters, 2,
		"waiters were served from a pool whose only conn is at its exclusive-checkout cap; "+
			"HTTP/1.1 has no multiplexing, so that is two requests on one socket")
	_, gotA := h1SettleAnswer(a)
	_, gotB := h1SettleAnswer(b)
	assert.Falsef(t, gotA || gotB, "a parked caller was answered with no capacity to give it")
}

// TestH1Pool_CloseDuringHealthSweep_ClosesEachConnExactlyOnce pins the shutdown
// race runHealthSweep documents and nothing exercised (#883).
//
// runHealthSweep's last act is a select between reporting its result and
// noticing the pool closed; evictDead's doc spells out the consequence — "if
// Close lands mid-sweep the result is dropped, so handleClose reports that conn
// CloseManual where a serialised tick would have said CloseDead — the same
// number of events, one different reason, in the shutdown race only."
//
// The count half is the one that hurts: the sweep holds a reservation on a conn
// that handleClose is closing, so a second close or a lost event is exactly what
// a race here would produce. h1GatedConn suspends the probe on a channel rather
// than a clock, so the state is built deliberately.
func TestH1Pool_CloseDuringHealthSweep_ClosesEachConnExactlyOnce(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	defer func() { _ = ln.Close() }()
	var srvMu sync.Mutex
	var srv []net.Conn
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			srvMu.Lock()
			srv = append(srv, c)
			srvMu.Unlock()
		}
	}()
	defer func() {
		srvMu.Lock()
		defer srvMu.Unlock()
		for _, c := range srv {
			_ = c.Close()
		}
	}()
	d := &h1GatedDialer{
		addr:    ln.Addr().String(),
		gate:    make(chan struct{}),
		entered: make(chan struct{}),
	}
	tl := newH1CloseTally()
	p := newH1Pool("closerace.test:80", d, PoolOptions{
		MaxConnsPerHost:   2,
		HealthCheckPeriod: 20 * time.Millisecond,
	}, tl.ref, tl.metrics)
	mc, err := p.Acquire(context.Background())
	require.NoError(t, err, "seed acquire")
	p.release(mc, true)
	select {
	case <-d.entered:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the health sweep never probed the idle conn",
			"no probe is suspended, so Close cannot land mid-sweep and the race under "+
				"test is not expressible by this fixture")
	}
	require.Positivef(t, d.probes.Load(),
		"the gate opened without a probe ever reaching the conn")

	done := make(chan error, 1)
	go func() { done <- p.Close() }()
	select {
	case cerr := <-done:
		require.NoError(t, cerr, "Close")
	case <-time.After(5 * time.Second):
		close(d.gate)
		require.FailNow(t, "Close blocked while a probe was suspended",
			"the actor waited for a sweep goroutine that only the test can release, so a "+
				"peer that stops reading wedges Close for as long as it likes")
	}
	close(d.gate) // the probe now finishes into a pool that is already gone

	// The dropped sweep result must not produce a second close: exactly one
	// event per conn, and the reason is the shutdown's, not the probe's.
	require.Eventuallyf(t, func() bool { return len(tl.snapshot()) >= 1 },
		5*time.Second, time.Millisecond, "no conn was ever reported closed at shutdown")
	time.Sleep(50 * time.Millisecond) // give a spurious second event time to arrive
	ev := tl.snapshot()
	assert.Lenf(t, ev, 1,
		"want exactly one OnConnClose for the pool's one conn, got %d — a sweep result "+
			"applied after handleClose closes and reports the same conn twice, and "+
			"double-counted churn is precisely what this dashboard exists to rule out", len(ev))
	for i, e := range ev {
		assert.Equalf(t, CloseManual, e.Reason,
			"event[%d] reason %v: a conn closed by Close is CloseManual even when a probe "+
				"had already found it dead — the sweep result is dropped, not applied", i, e.Reason)
	}
	assert.EqualValuesf(t, len(ev), tl.metrics.Counters.ConnsClosed.Load(),
		"ConnsClosed (%d) and OnConnClose events (%d) disagree; the counter and the hook "+
			"are the same observation and an operator reading one of them would be wrong",
		tl.metrics.Counters.ConnsClosed.Load(), len(ev))
}
