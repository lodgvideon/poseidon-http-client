package poolcore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestAddrString_ZeroAddressRendersEmpty covers the distinction AddrString
// exists for. Address.String is net.JoinHostPort, so the zero Address renders
// as ":0" — a plausible-looking address that names no backend. The acquire path
// returns a zero Address whenever no backend was ever picked (an empty set, or
// a Selector that refused), and an exchange record saying ":0" claims a backend
// was chosen. Only the both-fields-zero case is the "nothing was picked"
// signal; a zero port with a real host is a real, if unusual, address.
func TestAddrString_ZeroAddressRendersEmpty(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		addr Address
		want string
	}{
		{"zero value means no backend was picked", Address{}, ""},
		{"host and port", Address{Host: "10.0.0.1", Port: 443}, "10.0.0.1:443"},
		{"host with zero port is still an address", Address{Host: "10.0.0.1"}, "10.0.0.1:0"},
		{"port with no host is still an address", Address{Port: 443}, ":443"},
		{"IPv6 literal keeps its brackets", Address{Host: "::1", Port: 8443}, "[::1]:8443"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := AddrString(tc.addr)

			assert.Equalf(t, tc.want, got,
				"AddrString(%#v) = %q, want %q — a record naming ':0' claims a backend was "+
					"chosen and names one that does not exist", tc.addr, got, tc.want)
		})
	}
}

// TestDialTimeoutOrDefault_NonPositiveMeansDefault pins the boundary. A
// non-positive timeout means "unset", not "already expired": these pools are
// also constructed directly, and a zero field turning every dial into an
// instant deadline is the failure mode the default exists to make unreachable.
func TestDialTimeoutOrDefault_NonPositiveMeansDefault(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"negative is unset", -time.Second, DefaultDialTimeout},
		{"zero is unset", 0, DefaultDialTimeout},
		{"one nanosecond is a real timeout", time.Nanosecond, time.Nanosecond},
		{"an explicit value is kept", 5 * time.Second, 5 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := DialTimeoutOrDefault(tc.in)

			assert.Equalf(t, tc.want, got,
				"DialTimeoutOrDefault(%v) = %v, want %v", tc.in, got, tc.want)
		})
	}
}

// TestDialCtx_AppliesTheDefaultAndKeepsTheParent covers both halves of DialCtx:
// the deadline it installs, and that it derives from the caller's context so a
// cancelled request still aborts the dial rather than waiting out the timeout.
func TestDialCtx_AppliesTheDefaultAndKeepsTheParent(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := DialCtx(parent, 0)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok, "DialCtx must install a deadline; without one a black-hole host hangs the dial")
	assert.WithinDurationf(t, time.Now().Add(DefaultDialTimeout), deadline, 5*time.Second,
		"a zero timeout must mean the %v default, not an expired deadline", DefaultDialTimeout)

	cancelParent()

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		assert.Fail(t, "cancelling the parent did not cancel the dial context; a cancelled "+
			"request would keep its dial alive for the whole timeout")
	}
}

// TestIsDialOnlyErr_ClassifiesFailoverCandidates pins which errors mean "this
// backend could not be reached" — the only class worth trying the next address
// for. A request-level failure must NOT qualify: replaying it against another
// backend would repeat work the first one may already have done.
func TestIsDialOnlyErr_ClassifiesFailoverCandidates(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"dial backoff", ErrDialBackoff, true},
		{"pool closed", ErrPoolClosed, true},
		{"dial error", &DialError{Addr: "10.0.0.1:443", Err: errors.New("refused")}, true},
		{"wrapped dial error", fmt.Errorf("acquire: %w", &DialError{Addr: "a:1", Err: errors.New("x")}), true},
		{"wrapped pool closed", fmt.Errorf("acquire: %w", ErrPoolClosed), true},
		{"acquire timeout is not a dial failure", ErrAcquireTimeout, false},
		{"a plain error is not a dial failure", errors.New("stream reset"), false},
		{"context cancellation is the caller giving up", context.Canceled, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := IsDialOnlyErr(tc.err)

			assert.Equalf(t, tc.want, got,
				"IsDialOnlyErr(%v) = %v, want %v — a wrong answer either strands a reachable "+
					"backend set or replays a request the server may have processed", tc.err, got, tc.want)
		})
	}
}

// TestPoolRelease_AfterCloseClosesTheConn covers the branch that exists to
// avoid a leak: once the actor is gone there is nobody to process a release, so
// Release must close the connection itself rather than drop it.
func TestPoolRelease_AfterCloseClosesTheConn(t *testing.T) {
	t.Parallel()

	p := New("release.test:443", conn.ConnOptions{Dialer: &fakeDialer{}}, PoolOptions{
		MaxConnsPerHost:   1,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	require.NoError(t, p.Close(), "Close on a pool that never dialled")

	p.Release(&ManagedConn{}) // C is nil: the nil guard inside must hold
	p.Release(nil)

	assert.Equal(t, Stats{}, p.Stats(),
		"a release after Close must not resurrect pool state")
}

// TestManagedCoreAccessors_ReportWhatTheCoreWasBuiltWith covers the read seams
// the per-protocol pools' tests use, including the one that answers under the
// lock.
func TestManagedCoreAccessors_ReportWhatTheCoreWasBuiltWith(t *testing.T) {
	t.Parallel()
	addr := Address{Host: "10.0.0.9", Port: 443}
	res := StaticResolver(addr)

	mp, err := BuildManagedPool(res, RoundRobin(), DrainHard, conn.ConnOptions{Dialer: &fakeDialer{}},
		PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}, nil, nil)
	require.NoError(t, err, "BuildManagedPool against a static resolver")
	t.Cleanup(func() { _ = mp.Close() })
	mp.SetTickerPeriod(time.Hour)

	assert.Same(t, res, mp.Resolver(),
		"Resolver() must hand back the resolver the core was built with; the sibling tests "+
			"reach through it to script address-set changes")
	assert.Equalf(t, DrainHard, mp.DrainMode(),
		"DrainMode() = %v, want DrainHard", mp.DrainMode())
	assert.False(t, mp.HasSubPool(addr.String()),
		"HasSubPool reported a sub-pool before any acquire created one")

	require.NotNil(t, mp.GetOrCreateSubPool(addr), "GetOrCreateSubPool for a resolved address")

	assert.True(t, mp.HasSubPool(addr.String()),
		"HasSubPool did not see the sub-pool GetOrCreateSubPool just registered; the drain "+
			"tests use it to decide when an address has really been evicted")
	assert.Len(t, mp.SnapshotActive(), 1,
		"SnapshotActive must report the resolver's set")
}

// recordingObserver captures the connection-lifecycle events a pool raises.
type recordingObserver struct {
	dials   []DialEvent
	closes  []ConnCloseEvent
	updates []ResolverUpdateEvent
}

func (o *recordingObserver) OnDial(e DialEvent)                     { o.dials = append(o.dials, e) }
func (o *recordingObserver) OnConnClose(e ConnCloseEvent)           { o.closes = append(o.closes, e) }
func (o *recordingObserver) OnResolverUpdate(e ResolverUpdateEvent) { o.updates = append(o.updates, e) }

// TestDialObserved_ReportsBothOutcomes covers the observability seam itself: the
// counts and the event a single-connection transport's dial produces. Both arms
// matter — a dial that fails must still be counted as ATTEMPTED and must still
// reach OnDial, because a hook that only fires on success cannot measure the
// thing a load test is asking about.
func TestDialObserved_ReportsBothOutcomes(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("connection refused")
	cases := []struct {
		name        string
		dialErr     error
		wantFailed  int64
		wantEventOK bool
	}{
		{"successful dial", nil, 0, true},
		{"failed dial", wantErr, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &countingRecorder{}
			obs := &recordingObserver{}

			got, err := DialObserved(context.Background(), "10.0.0.4:443", time.Second, rec, obs,
				func(context.Context) (int, error) {
					if tc.dialErr != nil {
						return 0, tc.dialErr
					}
					return 7, nil
				})

			if tc.dialErr != nil {
				require.ErrorIs(t, err, tc.dialErr, "DialObserved must return the dial's own error unchanged")
			} else {
				require.NoError(t, err, "DialObserved on a succeeding dial")
				assert.Equal(t, 7, got, "DialObserved must return the dial's value unchanged")
			}
			assert.Equalf(t, int64(1), rec.DialsAttempted.Load(),
				"DialsAttempted = %d, want 1 — every dial is counted before its outcome is known",
				rec.DialsAttempted.Load())
			assert.Equalf(t, tc.wantFailed, rec.DialsFailed.Load(),
				"DialsFailed = %d, want %d", rec.DialsFailed.Load(), tc.wantFailed)
			assert.Equalf(t, int64(1), rec.DialsObserved.Load(),
				"the dial latency must be observed on both outcomes; timing only successes hides "+
					"the slow-failure case a load test is looking for")
			require.Lenf(t, obs.dials, 1, "OnDial fired %d times, want exactly 1", len(obs.dials))
			assert.Equal(t, "10.0.0.4:443", obs.dials[0].Addr, "OnDial must carry the dialled address")
			assert.ErrorIs(t, obs.dials[0].Err, tc.dialErr, "OnDial must carry the dial's outcome")
		})
	}
}

// TestNotifyConnClose_CountsAndReports pins the other half of the seam. The
// hook's contract is "every close this client performs", so the count and the
// event must both fire for a reason the caller chose.
func TestNotifyConnClose_CountsAndReports(t *testing.T) {
	t.Parallel()
	rec := &countingRecorder{}
	obs := &recordingObserver{}

	NotifyConnClose("10.0.0.5:443", CloseGoAway, obs, rec)

	assert.Equalf(t, int64(1), rec.ConnsClosedN.Load(),
		"ConnsClosed = %d, want 1 — a close the counter misses makes connection churn "+
			"invisible on the one dashboard an operator reads", rec.ConnsClosedN.Load())
	require.Len(t, obs.closes, 1, "OnConnClose must fire once per close")
	assert.Equal(t, ConnCloseEvent{Addr: "10.0.0.5:443", Reason: CloseGoAway}, obs.closes[0],
		"the event must carry the address and the reason the caller classified it with")
}
