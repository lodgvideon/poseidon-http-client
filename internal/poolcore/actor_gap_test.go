package poolcore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// testPool builds a pool over the in-process fake server, with the sweep parked
// so the actor cannot race an assertion about state the test set up by hand.
func testPool(t *testing.T, opts PoolOptions) *Pool {
	t.Helper()
	if opts.HealthCheckPeriod == 0 {
		opts.HealthCheckPeriod = time.Hour
	}
	p := New("actor.test:443", conn.ConnOptions{Dialer: &fakeDialer{}}, opts, nil, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// TestEnsureDialForWaiters_DialsTheShortfallAndNoMore is the arithmetic the pool
// budgets connections with. Every case below is a boundary of one term: the
// waiter count against the headroom existing conns already have, the rounding
// when a shortfall does not fill a whole connection, the in-flight dials already
// promised to those waiters, and the pool's own conn ceiling.
//
// Both directions matter. Dialing one too many opens a connection nothing needs
// and pays a handshake for it; dialing one too few leaves a waiter parked until
// the next tick, a whole HealthCheckPeriod away.
func TestEnsureDialForWaiters_DialsTheShortfallAndNoMore(t *testing.T) {
	t.Parallel()
	live := dialFakeConn(t)

	cases := []struct {
		name    string
		opts    PoolOptions
		rs      RunState
		want    int
		explain string
	}{
		{
			name:    "no waiters, no dial",
			opts:    PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 10},
			rs:      RunState{},
			want:    0,
			explain: "a pool with nothing queued must not dial speculatively",
		},
		{
			name: "waiters fully covered by spare capacity",
			opts: PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 10},
			rs: RunState{
				Conns:   []*ManagedConn{{C: live, StreamCap: 10}},
				Waiters: make([]AcquireReq, 10),
			},
			want:    0,
			explain: "ten waiters against ten free streams need no new connection",
		},
		{
			name: "one waiter past capacity dials exactly one",
			opts: PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 10},
			rs: RunState{
				Conns:   []*ManagedConn{{C: live, StreamCap: 10}},
				Waiters: make([]AcquireReq, 11),
			},
			want:    1,
			explain: "a partial batch still needs a whole connection",
		},
		{
			name:    "a shortfall of one full conn plus one dials two",
			opts:    PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 10},
			rs:      RunState{Waiters: make([]AcquireReq, 11)},
			want:    2,
			explain: "eleven waiters over a cap of ten round up to two connections",
		},
		{
			name:    "in-flight dials already cover the waiters",
			opts:    PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 10},
			rs:      RunState{Waiters: make([]AcquireReq, 8), InFlightDials: 1},
			want:    1,
			explain: "a dial already in flight is capacity promised to these waiters",
		},
		{
			name:    "the conn ceiling clamps the batch",
			opts:    PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 1},
			rs:      RunState{Waiters: make([]AcquireReq, 9)},
			want:    2,
			explain: "nine waiters want nine conns; MaxConnsPerHost allows two",
		},
		{
			name: "no room left, no dial",
			opts: PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 1},
			rs: RunState{
				Conns:   []*ManagedConn{{C: live, StreamCap: 1, Active: 1}},
				Waiters: make([]AcquireReq, 4),
			},
			want:    0,
			explain: "the pool is already at MaxConnsPerHost",
		},
		{
			name:    "an open dial backoff refuses the batch",
			opts:    PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 10, DialBackoff: time.Hour},
			rs:      RunState{Waiters: make([]AcquireReq, 4), LastDialErrAt: time.Now()},
			want:    0,
			explain: "a dial that just failed must not be retried inside its backoff window",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := testPool(t, tc.opts)
			rs := tc.rs

			p.EnsureDialForWaiters(&rs)

			assert.Equalf(t, tc.want, rs.InFlightDials,
				"InFlightDials = %d, want %d — %s", rs.InFlightDials, tc.want, tc.explain)
		})
	}
}

// TestFlushStrandedWaiters_OnlyWhenNothingCanRescueThem pins the guard, not just
// the flush. Answering a waiter that a live conn or an in-flight dial could
// still serve turns a recoverable wait into a failed request; not answering one
// that nothing can serve parks it until the next tick.
func TestFlushStrandedWaiters_OnlyWhenNothingCanRescueThem(t *testing.T) {
	t.Parallel()
	live := dialFakeConn(t)
	backoffOpen := PoolOptions{MaxConnsPerHost: 2, DialBackoff: time.Hour}

	cases := []struct {
		name      string
		opts      PoolOptions
		rs        RunState
		wantFlush bool
		explain   string
	}{
		{
			name:      "nothing live, nothing in flight, backoff open: flush",
			opts:      backoffOpen,
			rs:        RunState{LastDialErrAt: time.Now()},
			wantFlush: true,
			explain:   "no conn, no dial and a closed door is the definition of stranded",
		},
		{
			name:      "a dial is in flight: wait for it",
			opts:      backoffOpen,
			rs:        RunState{InFlightDials: 1, LastDialErrAt: time.Now()},
			wantFlush: false,
			explain:   "the dial in flight is what these waiters are waiting for",
		},
		{
			name:      "a live conn exists: it can still serve them",
			opts:      backoffOpen,
			rs:        RunState{Conns: []*ManagedConn{{C: live, StreamCap: 4}}, LastDialErrAt: time.Now()},
			wantFlush: false,
			explain:   "a live conn releasing a stream serves the queue without any dial",
		},
		{
			name:      "backoff closed: a dial can still be started",
			opts:      PoolOptions{MaxConnsPerHost: 2, DialBackoff: time.Millisecond},
			rs:        RunState{},
			wantFlush: false,
			explain:   "with the backoff closed the caller's next EnsureDialForWaiters rescues them",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := testPool(t, tc.opts)
			rs := tc.rs
			reply := make(chan AcquireResp, 1)
			rs.Waiters = []AcquireReq{{Ctx: context.Background(), Reply: reply}}

			p.FlushStrandedWaiters(&rs, ErrDialBackoff)

			if !tc.wantFlush {
				assert.Lenf(t, rs.Waiters, 1, "the waiter was answered even though %s", tc.explain)
				assert.Empty(t, reply, "no reply should have been sent")
				return
			}
			assert.Emptyf(t, rs.Waiters, "the queue was not cleared — %s", tc.explain)
			select {
			case got := <-reply:
				assert.ErrorIs(t, got.Err, ErrDialBackoff,
					"a stranded waiter must be told why, not just released")
			default:
				assert.Fail(t, "the waiter was dropped from the queue without a reply; it would "+
					"block until its own context expired")
			}
		})
	}
}

// TestMapAcquireErr_TellsThePoolsTimeoutFromTheCallers is a decision table over
// the two inputs. The distinction is the point: a caller whose own deadline
// passed must not be told the POOL timed out, or a retry policy reading the
// error re-sends a request whose deadline has already gone.
func TestMapAcquireErr_TellsThePoolsTimeoutFromTheCallers(t *testing.T) {
	t.Parallel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	cancelled, cancelIt := context.WithCancel(context.Background())
	cancelIt()

	cases := []struct {
		name          string
		ctx           context.Context
		timeoutActive bool
		want          error
	}{
		{"pool timeout armed and the deadline passed", expired, true, ErrAcquireTimeout},
		{"deadline passed but no pool timeout: it was the caller's", expired, false, context.DeadlineExceeded},
		{"cancellation is never a pool timeout", cancelled, true, context.Canceled},
		{"live context yields no error", context.Background(), true, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := MapAcquireErr(tc.ctx, tc.timeoutActive)

			if tc.want == nil {
				assert.NoError(t, got, "a context that is still live has no acquire error")
				return
			}
			assert.ErrorIsf(t, got, tc.want,
				"MapAcquireErr = %v, want %v — conflating the pool's AcquireTimeout with the "+
					"caller's own deadline makes a retry policy replay an expired request", got, tc.want)
		})
	}
}

// TestEvictIdle_ClosesOnlyConnsPastTheirIdleWindow covers the sweep's guard and
// what it spares. A zero IdleTimeout is documented as "never close on idle", and
// a conn with an in-flight stream is never idle however long ago it was handed
// out.
func TestEvictIdle_ClosesOnlyConnsPastTheirIdleWindow(t *testing.T) {
	t.Parallel()

	t.Run("a zero IdleTimeout disables the sweep entirely", func(t *testing.T) {
		t.Parallel()
		p := testPool(t, PoolOptions{MaxConnsPerHost: 2})
		mc := &ManagedConn{C: dialFakeConn(t), LastUsed: time.Now().Add(-time.Hour)}

		got := p.EvictIdle([]*ManagedConn{mc})

		assert.Lenf(t, got, 1,
			"a conn idle for an hour was evicted with IdleTimeout unset; zero is documented as "+
				"'never close on idle', and a pool that reaps anyway churns connections a load "+
				"test deliberately kept warm")
	})

	t.Run("busy, recent and stale conns are told apart", func(t *testing.T) {
		t.Parallel()
		p := testPool(t, PoolOptions{MaxConnsPerHost: 4, IdleTimeout: 50 * time.Millisecond})
		busy := &ManagedConn{C: dialFakeConn(t), Active: 1, LastUsed: time.Now().Add(-time.Hour)}
		recent := &ManagedConn{C: dialFakeConn(t), LastUsed: time.Now()}
		stale := &ManagedConn{C: dialFakeConn(t), LastUsed: time.Now().Add(-time.Hour)}

		got := p.EvictIdle([]*ManagedConn{busy, recent, stale})

		assert.Contains(t, got, busy,
			"a conn with an in-flight stream was evicted; its streams would fail mid-request")
		assert.Contains(t, got, recent, "a conn used just now was evicted as idle")
		assert.NotContains(t, got, stale, "a conn idle past the window was kept")
	})
}
