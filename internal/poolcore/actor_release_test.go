package poolcore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// deadConn returns a conn that has been closed, so IsAlive reports false. It is
// dialled rather than zero-valued: a hand-built conn.Conn reads as alive and
// panics on Close.
func deadConn(t *testing.T) *conn.Conn {
	t.Helper()
	c := dialFakeConn(t)
	require.NoError(t, c.Close(), "close the conn standing in for a dead one")
	require.False(t, c.IsAlive(), "the dead fixture still reads as alive")
	return c
}

// TestHandleRelease_ReapsAConnOnlyOnceItsLastStreamIsBack pins the pair of
// conditions the release path evicts on. Both halves are load-bearing in
// opposite directions: reaping a dead conn that still carries streams kills
// those requests mid-flight, and keeping a dead conn that has gone quiet leaves
// the pool handing out a corpse until the next health tick.
func TestHandleRelease_ReapsAConnOnlyOnceItsLastStreamIsBack(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		activeAfter int // Active count after the release this test performs
		wantEvicted bool
		explain     string
	}{
		{
			name:        "dead and now idle: reaped",
			activeAfter: 0,
			wantEvicted: true,
			explain:     "a dead conn whose last stream has returned is of no further use",
		},
		{
			name:        "dead but still carrying a stream: kept",
			activeAfter: 1,
			wantEvicted: false,
			explain:     "evicting it closes the conn under a request that is still running",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := testPool(t, PoolOptions{MaxConnsPerHost: 2})
			mc := &ManagedConn{C: deadConn(t), Active: tc.activeAfter + 1}
			rs := RunState{Conns: []*ManagedConn{mc}}

			p.HandleRelease(&rs, ReleaseMsg{Mc: mc})

			if tc.wantEvicted {
				assert.Emptyf(t, rs.Conns, "the conn was kept — %s", tc.explain)
				return
			}
			assert.Lenf(t, rs.Conns, 1, "the conn was evicted — %s", tc.explain)
		})
	}
}

// TestHandleRelease_CountsAGoAwayAsItsOwnReason covers the classification, not
// just the eviction. A peer's graceful shutdown reported as an ordinary dead
// conn makes a rolling restart indistinguishable from local churn on the one
// counter an operator watches.
func TestHandleRelease_CountsAGoAwayAsItsOwnReason(t *testing.T) {
	t.Parallel()
	rec := &countingRecorder{}
	obs := &recordingObserver{}
	p := New("goaway.test:443", conn.ConnOptions{Dialer: &fakeDialer{}},
		PoolOptions{MaxConnsPerHost: 2, HealthCheckPeriod: time.Hour}, obs, rec)
	t.Cleanup(func() { _ = p.Close() })
	mc := &ManagedConn{C: deadConn(t), Active: 1}
	rs := RunState{Conns: []*ManagedConn{mc}}

	p.HandleRelease(&rs, ReleaseMsg{Mc: mc})

	require.Empty(t, rs.Conns, "the dead conn was not reaped")
	assert.Equalf(t, int64(1), rec.ConnsClosedN.Load(),
		"ConnsClosed = %d, want 1 — an eviction the counter misses makes connection churn "+
			"invisible", rec.ConnsClosedN.Load())
	require.Len(t, obs.closes, 1, "OnConnClose must fire for an evicted conn")
	assert.Equalf(t, CloseDead, obs.closes[0].Reason,
		"reason = %v, want CloseDead — this conn was closed locally, not by a peer GOAWAY, and "+
			"the two must not be conflated", obs.closes[0].Reason)
}

// TestReclaim_ReturnsAConnTheAbandonedAcquireStillOwed covers the leak guard. An
// acquire that gave up after the actor took its request is still owed exactly
// one reply, and that reply may carry a connection nobody is going to use. Drop
// it and the pool leaks a live conn for the life of the process; the nil arm is
// the ordinary case where the actor answered with an error instead.
func TestReclaim_ReturnsAConnTheAbandonedAcquireStillOwed(t *testing.T) {
	t.Parallel()

	t.Run("a conn in the abandoned reply is released", func(t *testing.T) {
		t.Parallel()
		p := testPool(t, PoolOptions{MaxConnsPerHost: 2})
		mc := &ManagedConn{C: dialFakeConn(t), Active: 1, StreamCap: 4}
		reply := make(chan AcquireResp, 1)
		reply <- AcquireResp{Mc: mc}

		p.reclaim(reply)

		assert.Eventuallyf(t, func() bool { return p.Stats().InFlightStreams == 0 }, 3*time.Second,
			10*time.Millisecond,
			"the abandoned reply's conn was never released; its stream slot stays charged "+
				"forever, and the pool eventually refuses acquires against capacity nothing holds")
	})

	t.Run("an error reply releases nothing", func(t *testing.T) {
		t.Parallel()
		p := testPool(t, PoolOptions{MaxConnsPerHost: 2})
		reply := make(chan AcquireResp, 1)
		reply <- AcquireResp{Err: errors.New("dial failed")}

		assert.NotPanics(t, func() { p.reclaim(reply) },
			"reclaim dereferenced a nil conn on the error reply, which is the shape every "+
				"failed acquire produces")
	})
}

// TestEvictDeadSilent_ReapsWithoutFiringHooks covers the Stats-path sweep. It is
// silent in the sense of not calling a user callback from inside a metrics read
// — but it must still COUNT, or a conn killed out of band and first noticed by a
// scrape is closed with the counter staying at zero forever.
func TestEvictDeadSilent_ReapsWithoutFiringHooks(t *testing.T) {
	t.Parallel()
	rec := &countingRecorder{}
	obs := &recordingObserver{}
	p := New("silent.test:443", conn.ConnOptions{Dialer: &fakeDialer{}},
		PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Hour}, obs, rec)
	t.Cleanup(func() { _ = p.Close() })
	live := &ManagedConn{C: dialFakeConn(t)}
	busy := &ManagedConn{C: deadConn(t), Active: 1}
	gone := &ManagedConn{C: deadConn(t)}

	got := p.evictDeadSilent([]*ManagedConn{live, busy, gone})

	assert.Contains(t, got, live, "a live conn was reaped")
	assert.Containsf(t, got, busy,
		"a dead conn with an in-flight stream was reaped from the metrics path; observability "+
			"must never be able to fail a request")
	assert.NotContains(t, got, gone, "a dead, idle conn survived the sweep")
	assert.Equalf(t, int64(1), rec.ConnsClosedN.Load(),
		"ConnsClosed = %d, want 1 — silent means no user callback, not no record",
		rec.ConnsClosedN.Load())
	assert.Empty(t, obs.closes,
		"OnConnClose fired from inside a metrics read; that is the callback this sweep exists "+
			"to suppress")
}

// TestAcquire_TimesOnlyTheAcquiresThatSucceeded covers the recording arm. A
// failed acquire folded into the latency histogram reports the pool as fast
// whenever it is refusing work, which is exactly backwards.
func TestAcquire_TimesOnlyTheAcquiresThatSucceeded(t *testing.T) {
	t.Parallel()

	t.Run("a successful acquire is timed", func(t *testing.T) {
		t.Parallel()
		rec := &countingRecorder{}
		p := New("acq.test:443", conn.ConnOptions{Dialer: &fakeDialer{}},
			PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}, nil, rec)
		t.Cleanup(func() { _ = p.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		mc, err := p.Acquire(ctx)

		require.NoError(t, err, "acquire against the in-process fake server")
		p.Release(mc)
		assert.Equalf(t, int64(1), rec.AcquiresObserved.Load(),
			"AcquiresObserved = %d, want 1 — the acquire latency is how a load test sees the "+
				"pool queueing behind capacity", rec.AcquiresObserved.Load())
	})

	t.Run("a failed acquire is not timed", func(t *testing.T) {
		t.Parallel()
		rec := &countingRecorder{}
		p := New("acqfail.test:443", conn.ConnOptions{Dialer: &failingDialer{err: errors.New("refused")}},
			PoolOptions{MaxConnsPerHost: 1, DialTimeout: time.Second, HealthCheckPeriod: time.Hour},
			nil, rec)
		t.Cleanup(func() { _ = p.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := p.Acquire(ctx)

		require.Error(t, err, "acquire against a refusing dialer succeeded")
		assert.Zerof(t, rec.AcquiresObserved.Load(),
			"AcquiresObserved = %d, want 0 — timing a refusal alongside real acquires makes the "+
				"pool look fastest exactly when it is serving nothing", rec.AcquiresObserved.Load())
	})
}

// TestDialAttempt_CountsAFailureOnlyWhenTheDialFailed covers the pool-side dial
// wrapper's counting arm, the sibling of DialObserved's. Counting a success as a
// failure inverts the one ratio a dial-failure alert is built on.
func TestDialAttempt_CountsAFailureOnlyWhenTheDialFailed(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("connection refused")

	cases := []struct {
		name       string
		dialErr    error
		wantFailed int64
	}{
		{"successful dial", nil, 0},
		{"failed dial", wantErr, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &countingRecorder{}
			obs := &recordingObserver{}
			env := DialEnv{
				ClosedCh: make(chan struct{}),
				Timeout:  2 * time.Second,
				Addr:     "10.0.0.6:443",
				Rec:      rec,
				Obs:      obs,
			}

			got, err := DialAttempt(env, func(context.Context) (int, error) {
				if tc.dialErr != nil {
					return 0, tc.dialErr
				}
				return 3, nil
			})

			if tc.dialErr != nil {
				require.ErrorIs(t, err, tc.dialErr, "DialAttempt must return the dial's own error")
			} else {
				require.NoError(t, err, "DialAttempt on a succeeding dial")
				assert.Equal(t, 3, got, "DialAttempt must return the dial's value unchanged")
			}
			assert.Equalf(t, int64(1), rec.DialsAttempted.Load(),
				"DialsAttempted = %d, want 1", rec.DialsAttempted.Load())
			assert.Equalf(t, tc.wantFailed, rec.DialsFailed.Load(),
				"DialsFailed = %d, want %d — this ratio is what a dial-failure alert fires on",
				rec.DialsFailed.Load(), tc.wantFailed)
			require.Len(t, obs.dials, 1, "OnDial must fire once per attempt, whatever the outcome")
		})
	}
}
