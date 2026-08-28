package poolcore

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// startH2Servers boots n httptest+h2 TLS servers each backed by an
// independent counter; returns parsed Addresses, counts, and cleanup.
// counts[i] is incremented each time a new TCP connection reaches the
// StateNew state on server i (i.e. each new dial), which is observable
// without sending an HTTP request.
func startH2Servers(t *testing.T, n int) ([]Address, []*atomic.Int32, func()) {
	t.Helper()
	addrs := make([]Address, n)
	counts := make([]*atomic.Int32, n)
	servers := make([]*httptest.Server, n)
	for i := 0; i < n; i++ {
		c := &atomic.Int32{}
		counts[i] = c
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
		}))
		srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				c.Add(1)
			}
		}
		require.NoError(t, http2.ConfigureServer(srv.Config, &http2.Server{}), "ConfigureServer")
		srv.EnableHTTP2 = true
		srv.StartTLS()
		servers[i] = srv
		host, port := splitHostPortInt(t, srv.Listener.Addr().String())
		addrs[i] = Address{Host: host, Port: port}
	}
	cleanup := func() {
		for _, s := range servers {
			s.Close()
		}
	}
	return addrs, counts, cleanup
}

func splitHostPortInt(t *testing.T, hp string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(hp)
	require.NoErrorf(t, err, "SplitHostPort(%q)", hp)
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi(%q)", portStr)
	return host, port
}

func newConnOpts() conn.ConnOptions {
	return conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}}
}

func TestManagedPool_StaticResolver_RoundRobin_DistributesDials(t *testing.T) {
	t.Parallel()

	addrs, counts, cleanup := startH2Servers(t, 3)
	defer cleanup()
	mp, err := NewManagedPool(
		StaticResolver(addrs...),
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		nil, nil,
	)
	require.NoError(t, err, "NewManagedPool")
	defer mp.Close()

	// 9 sequential acquires — RoundRobin distributes 3-3-3.
	for i := 0; i < 9; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, release, _, err := mp.Acquire(ctx)
		cancel()
		require.NoErrorf(t, err, "acquire[%d]", i)
		require.True(t, c.IsAlive(), "conn not alive after acquire")
		release()
	}

	for i, cnt := range counts {
		assert.Greaterf(t, cnt.Load(), int32(0),
			"server[%d] hits = %d, want > 0 — RoundRobin left an address unused", i, cnt.Load())
	}
}

func TestManagedPool_NoAddresses_ReturnsErrNoAddresses(t *testing.T) {
	t.Parallel()

	mp, err := NewManagedPool(
		StaticResolver(), // empty
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1},
		nil, nil,
	)
	require.NoError(t, err, "NewManagedPool")
	defer mp.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, _, err = mp.Acquire(ctx)

	assert.ErrorIsf(t, err, ErrNoAddresses,
		"acquire err = %v, want ErrNoAddresses — an empty resolver set must be reported as such, "+
			"not as a dial failure", err)
}

// scriptedResolver: a Resolver whose Watch channel is driven by the test.
type scriptedResolver struct {
	initial []Address
	updates chan []Address
}

func newScriptedResolver(initial []Address) *scriptedResolver {
	return &scriptedResolver{
		initial: initial,
		updates: make(chan []Address, 8),
	}
}

func (s *scriptedResolver) Resolve(_ context.Context) ([]Address, error) {
	return s.initial, nil
}

func (s *scriptedResolver) Watch(ctx context.Context) (<-chan []Address, error) {
	out := make(chan []Address, 1)
	out <- s.initial
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case set, ok := <-s.updates:
				if !ok {
					return
				}
				select {
				case out <- set:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (s *scriptedResolver) push(set []Address) { s.updates <- set }

func TestManagedPool_Watch_AddedAddress_PickedUp(t *testing.T) {
	t.Parallel()

	addrs, _, cleanup := startH2Servers(t, 3)
	defer cleanup()
	res := newScriptedResolver([]Address{addrs[0]})
	mp, err := NewManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
	require.NoError(t, err, "NewManagedPool")
	defer mp.Close()

	// Push expanded set; ManagedPool's Watch consumer must pick it up.
	res.push([]Address{addrs[0], addrs[1], addrs[2]})
	// Wait briefly for the Watch goroutine to apply the update.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(mp.SnapshotActive()) != 3 {
		time.Sleep(10 * time.Millisecond)
	}

	assert.Lenf(t, mp.SnapshotActive(), 3,
		"active set never grew to 3; got %d — a Watch update that adds addresses was dropped",
		len(mp.SnapshotActive()))
}

func TestManagedPool_DrainGraceful_RemovedAddress_KeepsInFlight(t *testing.T) {
	t.Parallel()

	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()
	res := newScriptedResolver(addrs)
	mp, err := NewManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
	require.NoError(t, err, "NewManagedPool")
	defer mp.Close()
	// Acquire a conn for addr[0].
	c0, rel0, _, err := mp.Acquire(context.Background())
	require.NoError(t, err, "acquire 0")
	require.True(t, c0.IsAlive(), "conn 0 not alive")

	// Remove addr[0] from the resolver set.
	res.push([]Address{addrs[1]})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(mp.SnapshotActive()) != 1 {
		time.Sleep(10 * time.Millisecond)
	}

	// In-flight conn must still be alive (graceful).
	assert.True(t, c0.IsAlive(),
		"conn 0 closed during graceful drain — expected alive until release")
	// New acquire must pick addr[1] only.
	c1, rel1, _, err := mp.Acquire(context.Background())
	require.NoError(t, err, "acquire after remove")
	defer rel1()
	require.NotNil(t, c1, "acquire after remove returned a nil conn")
	// Release in-flight conn → sub-pool should drain and be removed.
	rel0()
	deadline = time.Now().Add(2 * time.Second)
	present := true
	for time.Now().Before(deadline) {
		mp.Mu.RLock()
		_, present = mp.SubPools[addrs[0].String()]
		mp.Mu.RUnlock()
		if !present {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.False(t, present,
		"sub-pool for drained address still present after release; expected close+evict")
}

func TestManagedPool_DrainHard_RemovedAddress_ClosesImmediately(t *testing.T) {
	t.Parallel()

	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()
	res := newScriptedResolver(addrs)
	mp, err := NewManagedPool(res, RoundRobin(), DrainHard, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
	require.NoError(t, err, "NewManagedPool")
	defer mp.Close()
	c0, rel0, _, err := mp.Acquire(context.Background())
	require.NoError(t, err, "acquire 0")

	res.push([]Address{addrs[1]})
	// DrainHard closes the sub-pool synchronously inside applySet/beginDrain.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && c0.IsAlive() {
		time.Sleep(20 * time.Millisecond)
	}

	assert.False(t, c0.IsAlive(),
		"conn 0 still alive after DrainHard removal — DrainHard must close at once, "+
			"that is the whole difference from DrainLazy")
	rel0()
}

func TestManagedPool_DrainLazy_RemovedAddress_RetainsSubPool(t *testing.T) {
	t.Parallel()

	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()
	res := newScriptedResolver(addrs)
	mp, err := NewManagedPool(res, RoundRobin(), DrainLazy, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: 100 * time.Millisecond}, nil, nil)
	require.NoError(t, err, "NewManagedPool")
	defer mp.Close()
	// Seed both sub-pools by acquiring one conn each.
	for i := 0; i < 2; i++ {
		_, rel, _, err := mp.Acquire(context.Background())
		require.NoErrorf(t, err, "seed acquire %d", i)
		rel()
	}

	res.push([]Address{addrs[1]})
	// Wait for applySet to run.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(mp.SnapshotActive()) != 1 {
		time.Sleep(10 * time.Millisecond)
	}

	// DrainLazy: sub-pool must still be in map (not dropped immediately).
	mp.Mu.RLock()
	_, present := mp.SubPools[addrs[0].String()]
	mp.Mu.RUnlock()
	assert.True(t, present, "DrainLazy: sub-pool dropped immediately, expected retained")
	// New acquires pick addr[1] only.
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, rel, _, err := mp.Acquire(ctx)
		cancel()
		require.NoErrorf(t, err, "post-drain acquire %d", i)
		rel()
	}
}

// noWatchResolver satisfies Resolver with working Resolve but
// Watch always returns ErrWatchUnsupported.
type noWatchResolver struct {
	mu    sync.Mutex
	addrs []Address
}

func (r *noWatchResolver) Resolve(_ context.Context) ([]Address, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Address, len(r.addrs))
	copy(out, r.addrs)
	return out, nil
}

func (r *noWatchResolver) Watch(_ context.Context) (<-chan []Address, error) {
	return nil, ErrWatchUnsupported
}

func (r *noWatchResolver) set(addrs []Address) {
	r.mu.Lock()
	r.addrs = addrs
	r.mu.Unlock()
}

func TestManagedPool_WatchUnsupported_FallsBackToTicker(t *testing.T) {
	t.Parallel()

	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()
	res := &noWatchResolver{}
	res.set([]Address{addrs[0]})
	// Use BuildManagedPool so we can set the test seam before the
	// background goroutine starts reading tickerPeriod.
	mp, err := BuildManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
	require.NoError(t, err, "BuildManagedPool")
	mp.tickerPeriod.Store(int64(25 * time.Millisecond)) // test seam set before run
	go mp.Run()
	defer mp.Close()

	res.set(addrs) // expand set
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(mp.SnapshotActive()) != 2 {
		time.Sleep(10 * time.Millisecond)
	}

	assert.Lenf(t, mp.SnapshotActive(), 2,
		"ticker mode never picked up the new address; active = %d — a Resolver without Watch "+
			"must still track the address set", len(mp.SnapshotActive()))
}

func TestManagedPool_StatsAggregation_SumsAcrossSubPools(t *testing.T) {
	t.Parallel()

	addrs, _, cleanup := startH2Servers(t, 3)
	defer cleanup()
	mp, err := NewManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
	require.NoError(t, err, "NewManagedPool")
	defer mp.Close()
	// Seed each sub-pool with one conn.
	holds := make([]func(), 0, 3)
	for i := 0; i < 3; i++ {
		_, rel, _, err := mp.Acquire(context.Background())
		require.NoErrorf(t, err, "acquire %d", i)
		holds = append(holds, rel)
	}

	st := mp.Stats()

	assert.Equalf(t, 3, st.ActiveConns,
		"ActiveConns = %d, want 3 — stats must SUM across sub-pools, not report one of them",
		st.ActiveConns)
	assert.Equalf(t, 3, st.Addresses,
		"Addresses = %d, want 3 — stats must report every resolved address", st.Addresses)
	for _, rel := range holds {
		rel()
	}
}

// TestManagedPool_Close_NoGoroutineLeak closes N pools rather than one.
//
// This test previously created a single pool and allowed `after <= before+2`.
// That tolerance was exactly the size of the leak it exists to catch: making
// ManagedCore.run's cancel goroutine never fire strands run() plus its watcher
// — about two goroutines — and the test passed 2/2 under that mutation. Scaling
// to poolCount pools scales the leak with it (~2*poolCount) while the noise
// from other t.Parallel tests in this package stays constant, so the tolerance
// can absorb scheduler noise without also absorbing the defect.
func TestManagedPool_Close_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	const (
		poolCount = 8 // leak scales with this; scheduler noise does not
		tolerance = 4 // must stay well under 2*poolCount for the test to discriminate
	)
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()
	before := runtime.NumGoroutine()

	for i := 0; i < poolCount; i++ {
		mp, err := NewManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful, newConnOpts(),
			PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
		require.NoErrorf(t, err, "NewManagedPool %d", i)
		// Acquire once to force sub-pool creation and its background goroutines.
		_, rel, _, err := mp.Acquire(context.Background())
		require.NoErrorf(t, err, "acquire %d", i)
		rel()
		require.NoErrorf(t, mp.Close(), "close %d", i)
	}

	// Allow goroutines up to 2s to wind down after close.
	deadline := time.Now().Add(2 * time.Second)
	after := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		after = runtime.NumGoroutine()
		if after <= before+tolerance {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.LessOrEqualf(t, after, before+tolerance,
		"goroutine leak: before=%d after=%d (want <= %d) across %d closed pools — a pool that "+
			"does not stop its own goroutines on close leaks one set per pool",
		before, after, before+tolerance, poolCount)
}
