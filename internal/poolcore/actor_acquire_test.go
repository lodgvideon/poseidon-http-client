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

// TestHandleAcquire_ChoosesBetweenServingDialingAndRefusing is the decision the
// actor makes on every acquire. All four outcomes are here because the wrong one
// is silent in three directions: dialling past MaxConnsPerHost ignores the
// caller's ceiling, queueing when a dial could start adds a whole
// HealthCheckPeriod of latency, and refusing while a dial is in flight fails a
// request that was about to be served.
func TestHandleAcquire_ChoosesBetweenServingDialingAndRefusing(t *testing.T) {
	t.Parallel()

	type outcome struct {
		served     bool
		dialed     bool
		queued     bool
		refusedErr error
	}
	cases := []struct {
		name    string
		opts    PoolOptions
		rs      func(t *testing.T) RunState
		want    outcome
		explain string
	}{
		{
			name: "a conn with headroom serves the caller at once",
			opts: PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 4},
			rs: func(t *testing.T) RunState {
				return RunState{Conns: []*ManagedConn{{C: dialFakeConn(t), StreamCap: 4}}}
			},
			want:    outcome{served: true},
			explain: "an idle conn is the whole point of a pool",
		},
		{
			name: "nothing live and room to grow: dial and queue",
			opts: PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 4},
			rs:   func(*testing.T) RunState { return RunState{} },
			want: outcome{dialed: true, queued: true},
			explain: "the caller waits for a dial that was started for it, not for the next " +
				"health tick",
		},
		{
			name: "at the conn ceiling: queue without dialling",
			opts: PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 1},
			rs: func(t *testing.T) RunState {
				return RunState{Conns: []*ManagedConn{{C: dialFakeConn(t), StreamCap: 1, Active: 1}}}
			},
			want:    outcome{queued: true},
			explain: "MaxConnsPerHost is a ceiling the pool must not step over to serve a queue",
		},
		{
			name: "in-flight dials count against the ceiling",
			opts: PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 1},
			rs:   func(*testing.T) RunState { return RunState{InFlightDials: 1} },
			want: outcome{queued: true},
			explain: "a dial already in flight occupies the slot; counting only established " +
				"conns opens one connection per queued waiter",
		},
		{
			name: "backoff open with nothing to wait for: refuse",
			opts: PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 4, DialBackoff: time.Hour},
			rs:   func(*testing.T) RunState { return RunState{LastDialErrAt: time.Now()} },
			want: outcome{refusedErr: ErrDialBackoff},
			explain: "queueing here parks the caller behind a dial that will not be attempted " +
				"until the backoff expires",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := testPool(t, tc.opts)
			rs := tc.rs(t)
			reply := make(chan AcquireResp, 1)
			req := AcquireReq{Ctx: context.Background(), Reply: reply}

			before := rs.InFlightDials

			p.handleAcquire(&rs, req)

			// The DELTA, not the count: one case starts with a dial already in
			// flight, and "InFlightDials > 0" cannot tell that apart from a dial
			// this call started.
			started := rs.InFlightDials - before
			assert.Equalf(t, tc.want.dialed, started > 0,
				"dials started = %d, want %v — %s", started, tc.want.dialed, tc.explain)
			assert.Equalf(t, tc.want.queued, len(rs.Waiters) > 0,
				"queued = %v, want %v — %s", len(rs.Waiters) > 0, tc.want.queued, tc.explain)
			switch {
			case tc.want.served:
				got := <-reply
				require.NoError(t, got.Err, "an available conn must be handed over, not refused")
				assert.NotNil(t, got.Mc, "the reply carried no conn")
				assert.Equalf(t, 1, got.Mc.Active,
					"the served conn's stream was not charged; the pool would hand the same "+
						"capacity out twice")
			case tc.want.refusedErr != nil:
				got := <-reply
				assert.ErrorIsf(t, got.Err, tc.want.refusedErr,
					"err = %v, want %v — %s", got.Err, tc.want.refusedErr, tc.explain)
			default:
				assert.Emptyf(t, reply, "the caller was answered when it should have waited — %s",
					tc.explain)
			}
		})
	}
}

// TestEvictDead_ReapsAndAttributes covers the health sweep's reaping arm and the
// reason it reports. It is the sibling of the silent sweep: this one fires the
// caller's hook, and the reason it carries is what tells a rolling restart apart
// from a connection that simply died.
func TestEvictDead_ReapsAndAttributes(t *testing.T) {
	t.Parallel()
	rec := &countingRecorder{}
	obs := &recordingObserver{}
	p := New("evict.test:443", conn.ConnOptions{Dialer: &fakeDialer{}},
		PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Hour}, obs, rec)
	t.Cleanup(func() { _ = p.Close() })
	live := &ManagedConn{C: dialFakeConn(t)}
	busy := &ManagedConn{C: deadConn(t), Active: 1}
	gone := &ManagedConn{C: deadConn(t)}

	got := p.evictDead([]*ManagedConn{live, busy, gone})

	assert.Contains(t, got, live, "a live conn was reaped")
	assert.Containsf(t, got, busy,
		"a dead conn still carrying a stream was reaped; RFC 9113 §6.8 lets those streams "+
			"finish, and closing under them fails requests the peer was still answering")
	assert.NotContains(t, got, gone, "a dead, idle conn survived the health sweep")
	require.Lenf(t, obs.closes, 1, "OnConnClose fired %d times, want 1", len(obs.closes))
	assert.Equalf(t, CloseDead, obs.closes[0].Reason,
		"reason = %v, want CloseDead — this conn died without a GOAWAY, and reporting it as "+
			"one would invent a peer shutdown that never happened", obs.closes[0].Reason)
	assert.Zerof(t, rec.GoAwaysReceived.Load(),
		"GoAwaysReceived = %d, want 0 — no peer sent one", rec.GoAwaysReceived.Load())
}

// partialResolver answers with an address set AND an error, which is what a
// resolver does when one of several sources failed.
type partialResolver struct {
	addrs []Address
	err   error
}

func (r *partialResolver) Resolve(context.Context) ([]Address, error) { return r.addrs, r.err }

func (r *partialResolver) Watch(context.Context) (<-chan []Address, error) {
	return nil, ErrWatchUnsupported
}

// TestNewCore_DefaultsAndThePartialResolveRule covers the constructor's two
// jobs. The nil substitutions are what the whole observability seam rests on —
// the reporting call sites carry no nil check of their own — and the resolve rule
// decides whether a half-broken resolver yields a working pool or none at all.
func TestNewCore_DefaultsAndThePartialResolveRule(t *testing.T) {
	t.Parallel()
	addr := Address{Host: "10.0.0.1", Port: 443}
	co := conn.ConnOptions{Dialer: &fakeDialer{}}
	po := PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}

	t.Run("nil selector, observer and recorder are all substituted", func(t *testing.T) {
		t.Parallel()

		mp, err := BuildManagedPool(StaticResolver(addr), nil, DrainGraceful, co, po, nil, nil)

		require.NoError(t, err, "BuildManagedPool with everything optional left nil")
		t.Cleanup(func() { _ = mp.Close() })
		assert.NotNil(t, mp.selector,
			"a nil Selector was not defaulted; the acquire path calls Pick without a nil check")
		assert.NotNil(t, mp.obs,
			"a nil Observer was not defaulted; every resolver update would panic")
		assert.NotNil(t, mp.rec,
			"a nil Recorder was not defaulted; every dial would panic")
	})

	t.Run("a resolver that answered despite an error still yields a pool", func(t *testing.T) {
		t.Parallel()
		res := &partialResolver{addrs: []Address{addr}, err: errors.New("one source failed")}

		mp, err := BuildManagedPool(res, RoundRobin(), DrainGraceful, co, po, nil, nil)

		require.NoError(t, err,
			"a resolver that returned addresses alongside an error must still build a pool; "+
				"failing here takes a service down because one of its discovery sources did")
		t.Cleanup(func() { _ = mp.Close() })
		assert.Len(t, mp.SnapshotActive(), 1, "the addresses it did return were dropped")
	})

	t.Run("a resolver that answered nothing reports its error", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("every source failed")
		res := &partialResolver{err: wantErr}

		_, err := BuildManagedPool(res, RoundRobin(), DrainGraceful, co, po, nil, nil)

		assert.ErrorIsf(t, err, wantErr,
			"err = %v, want the resolver's own error — a pool with no addresses and no "+
				"explanation gives the caller nothing to act on", err)
	})
}

// TestNewCore_SubstitutesNilObservabilityItself pins NewCore's OWN guards
// rather than BuildManagedPool's. The HTTP/2 constructor substitutes the nops
// before it calls NewCore, so going through it leaves these branches
// unreachable — and a constructor other callers reach directly (the HTTP/1.1
// and HTTP/3 managed pools do, and the gRPC channel will) must not depend on
// its caller having done the work.
func TestNewCore_SubstitutesNilObservabilityItself(t *testing.T) {
	t.Parallel()
	co := conn.ConnOptions{Dialer: &fakeDialer{}}
	po := PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}

	mp, err := NewCore(CoreConfig[*Pool, *ManagedConn, *conn.Conn, func()]{
		Resolver: StaticResolver(Address{Host: "10.0.0.2", Port: 443}),
		PoolOpts: po,
		NewSub:   func(key string) *Pool { return New(key, co, po, nil, nil) },
		ConnOf:   func(mc *ManagedConn) *conn.Conn { return mc.C },
		MkRelease: func(p *Pool, mc *ManagedConn) func() {
			return func() { p.Release(mc) }
		},
	})

	require.NoError(t, err, "NewCore with a nil Selector, Observer and Recorder")
	t.Cleanup(func() { _ = mp.Close() })
	require.NotNil(t, mp.obs, "NewCore left Obs nil; its own reporting path has no nil check")
	require.NotNil(t, mp.rec, "NewCore left Rec nil; its own reporting path has no nil check")
	// NotNil alone would accept an interface holding a typed nil, so the
	// substitutes are exercised rather than merely inspected.
	assert.NotPanics(t, func() {
		mp.obs.OnResolverUpdate(ResolverUpdateEvent{Total: 1})
		mp.rec.ConnClosed()
	}, "the substituted observability panicked when called")
	assert.NotNil(t, mp.selector, "NewCore left Selector nil; Acquire calls Pick unguarded")
}

// TestDefaultsAreTheDocumentedThirtySeconds pins the two timing constants to
// their values rather than to themselves. Every transport floors its dial
// timeout at the first and every managed pool sweeps on the second, so a change
// here is a change to how long a black-hole host hangs a request and to how fast
// a drained backend is noticed — worth failing a build over, not discovering in
// a load report.
func TestDefaultsAreTheDocumentedThirtySeconds(t *testing.T) {
	t.Parallel()

	assert.Equalf(t, 30*time.Second, DefaultDialTimeout,
		"DefaultDialTimeout = %v, want 30s", DefaultDialTimeout)
	assert.Equalf(t, 30*time.Second, defaultManagedPoolTickerPeriod,
		"defaultManagedPoolTickerPeriod = %v, want 30s", defaultManagedPoolTickerPeriod)
}
