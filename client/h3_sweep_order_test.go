package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ————————————————————————————————————————————————————————————————
// Two h3 sweep decisions that nothing pinned.
//
// Both were found by mutating the pool and running the whole client suite:
// each mutation passed. They are deliberate — the H2 sibling documents the
// first at length and h3RetireReason documents the second — but a deliberate
// decision no test defends is one a future unification will quietly reverse.
//
// Neither test may call Stats(). h3Pool.handleStats runs evictDeadSilent,
// which retires on the SAME predicate as evictDead, so polling stats performs
// the eviction under test and both tests would pass for the wrong reason. The
// OnConnClose hook is the only safe observer here.
// ————————————————————————————————————————————————————————————————

// closeWatch collects OnConnClose reasons without racing the actor.
type closeWatch struct{ ch chan CloseReason }

func newCloseWatch() (*closeWatch, *atomic.Pointer[Hooks]) {
	w := &closeWatch{ch: make(chan CloseReason, 8)}
	hp := new(atomic.Pointer[Hooks])
	hp.Store(&Hooks{OnConnClose: func(e ConnCloseEvent) {
		select {
		case w.ch <- e.Reason:
		default:
		}
	}})
	return w, hp
}

func (w *closeWatch) next(t *testing.T, d time.Duration, what string) CloseReason {
	t.Helper()
	select {
	case r := <-w.ch:
		return r
	case <-time.After(d):
		require.FailNowf(t, "no OnConnClose arrived", "within %v: %s", d, what)
		return 0
	}
}

// TestH3Pool_Tick_AttributesGoAwayBeforeIdle pins the sweep ORDER.
//
// A GOAWAY'd conn is very often also idle — a peer draining for a rolling
// restart stops being given work. Whichever sweep reaches it first decides its
// CloseReason, so handleTick runs evictDead before evictIdle. Reverse them and
// a rolling restart is reported as local inactivity and GoAwaysReceived never
// moves, which is exactly the signal an operator would be watching.
func TestH3Pool_Tick_AttributesGoAwayBeforeIdle(t *testing.T) {
	t.Parallel()

	w, hp := newCloseWatch()
	m := &Metrics{}
	d := newH3FakeDialer()
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 1,
		HealthCheckPeriod: 20 * time.Millisecond,
		// Short enough that the conn is idle-eligible by the time the tick
		// runs. That collision is the whole point of the test.
		IdleTimeout: time.Millisecond,
	}, d.dial, hp, m)
	defer func() { _ = p.Close() }()
	mc, err := p.acquire(context.Background())
	require.NoError(t, err, "acquire")
	p.release(mc)
	// Wait for the actor to RECORD the release before arming the GOAWAY, and do
	// it while the flag is still clear so this poll cannot evict anything.
	//
	// release only queues a message. Arming the flag first lets handleRelease
	// itself retire the conn through h3RetireEvict — as CloseGoAway, which is
	// the answer this test is looking for. Mutation-checking caught exactly
	// that: with the sweeps reversed the test still passed, because the tick
	// was never the evictor.
	for p.Stats().InFlightStreams != 0 {
	}
	conns := d.all()
	require.Len(t, conns, 1, "one capped stream must have opened exactly one conn")

	// The peer sends GOAWAY on a conn that is also about to qualify as idle.
	// From here on nothing may call Stats: evictDeadSilent retires on the same
	// predicate and would do the tick's job for it.
	atomic.StoreInt32(&conns[0].goawayFlag, 1)
	time.Sleep(5 * time.Millisecond) // past IdleTimeout

	got := w.next(t, 3*time.Second, "the tick never evicted the conn")
	assert.Equalf(t, CloseGoAway, got,
		"conn reported %v, want CloseGoAway;\n"+
			"a peer draining for a restart was attributed to local inactivity — "+
			"evictIdle ran before evictDead", got)
	assert.EqualValues(t, 1, m.Counters.GoAwaysReceived.Load(),
		"GoAwaysReceived did not move: the signal an operator watches for a rolling "+
			"restart never fired")
}

// TestH3Pool_Tick_EvictsDeadConnStillCarryingStreams pins that h3's Dead arm is
// NOT guarded on active == 0.
//
// h2 guards its whole liveness test that way, because its IsAlive folds GOAWAY
// in and evicting there would tear down an RFC 7540 §6.8 drain under its own
// in-flight streams. h3 does not need that guard on the Dead arm and must not
// have it: Alive() goes false only once the QUIC reader goroutine is gone, so
// the streams on that conn are already doomed and holding it keeps a corpse in
// the pool. Copying h2's shape here is the plausible-looking merge this pins.
func TestH3Pool_Tick_EvictsDeadConnStillCarryingStreams(t *testing.T) {
	t.Parallel()

	w, hp := newCloseWatch()
	m := &Metrics{}
	d := newH3FakeDialer()
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 4,
		HealthCheckPeriod: 20 * time.Millisecond,
	}, d.dial, hp, m)
	defer func() { _ = p.Close() }()
	// Hold the stream: the conn is checked out (active > 0) for the whole test,
	// which is the state a mistaken active == 0 guard would protect.
	_, err := p.acquire(context.Background())
	require.NoError(t, err, "acquire")
	conns := d.all()
	require.Len(t, conns, 1, "one acquire must have opened exactly one conn")

	conns[0].kill() // the QUIC reader goroutine is gone

	got := w.next(t, 3*time.Second, "a dead conn with a live stream was kept in the pool")
	assert.Equalf(t, CloseDead, got,
		"conn reported %v, want CloseDead: a conn whose QUIC reader is gone must be "+
			"evicted even while streams are still attached to it", got)
}
