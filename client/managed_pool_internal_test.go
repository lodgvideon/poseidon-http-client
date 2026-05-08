package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		if err := http2.ConfigureServer(srv.Config, &http2.Server{}); err != nil {
			t.Fatalf("ConfigureServer: %v", err)
		}
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
	for i := len(hp) - 1; i >= 0; i-- {
		if hp[i] == ':' {
			port := 0
			for _, c := range hp[i+1:] {
				port = port*10 + int(c-'0')
			}
			return hp[:i], port
		}
	}
	t.Fatalf("malformed host:port %q", hp)
	return "", 0
}

func newConnOpts() conn.ConnOptions {
	return conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}}
}

func TestManagedPool_StaticResolver_RoundRobin_DistributesDials(t *testing.T) {
	t.Parallel()
	addrs, counts, cleanup := startH2Servers(t, 3)
	defer cleanup()

	mp, err := newManagedPool(
		StaticResolver(addrs...),
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
	)
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// 9 sequential acquires — RoundRobin distributes 3-3-3.
	for i := 0; i < 9; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, release, err := mp.acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("acquire[%d] = %v", i, err)
		}
		if !c.IsAlive() {
			t.Fatal("conn not alive after acquire")
		}
		release()
	}
	for i, cnt := range counts {
		if got := cnt.Load(); got < 1 {
			t.Errorf("server[%d] hits = %d, want > 0", i, got)
		}
	}
}

func TestManagedPool_NoAddresses_ReturnsErrNoAddresses(t *testing.T) {
	t.Parallel()
	mp, err := newManagedPool(
		StaticResolver(), // empty
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1},
	)
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = mp.acquire(ctx)
	if err != ErrNoAddresses {
		t.Errorf("acquire err = %v, want ErrNoAddresses", err)
	}
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
	mp, err := newManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// Push expanded set; managedPool's Watch consumer must pick it up.
	res.push([]Address{addrs[0], addrs[1], addrs[2]})

	// Wait briefly for the Watch goroutine to apply the update.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("active set never grew to 3; got %d", len(mp.snapshotActive()))
}

func TestManagedPool_DrainGraceful_RemovedAddress_KeepsInFlight(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()

	res := newScriptedResolver(addrs)
	mp, err := newManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// Acquire a conn for addr[0].
	c0, rel0, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 0: %v", err)
	}
	if !c0.IsAlive() {
		t.Fatal("conn 0 not alive")
	}

	// Remove addr[0] from the resolver set.
	res.push([]Address{addrs[1]})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// In-flight conn must still be alive (graceful).
	if !c0.IsAlive() {
		t.Error("conn 0 closed during graceful drain — expected alive until release")
	}

	// New acquire must pick addr[1] only.
	c1, rel1, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after remove: %v", err)
	}
	defer rel1()
	_ = c1

	// Release in-flight conn → sub-pool should drain and be removed.
	rel0()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mp.mu.RLock()
		_, present := mp.subPools[addrs[0].String()]
		mp.mu.RUnlock()
		if !present {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("sub-pool for drained address still present after release; expected close+evict")
}

func TestManagedPool_DrainHard_RemovedAddress_ClosesImmediately(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()

	res := newScriptedResolver(addrs)
	mp, err := newManagedPool(res, RoundRobin(), DrainHard, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	c0, rel0, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 0: %v", err)
	}

	res.push([]Address{addrs[1]})

	// DrainHard closes the sub-pool synchronously inside applySet/beginDrain.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !c0.IsAlive() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if c0.IsAlive() {
		t.Error("conn 0 still alive after DrainHard removal")
	}
	rel0()
}

func TestManagedPool_DrainLazy_RemovedAddress_RetainsSubPool(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()

	res := newScriptedResolver(addrs)
	mp, err := newManagedPool(res, RoundRobin(), DrainLazy, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// Seed both sub-pools by acquiring one conn each.
	for i := 0; i < 2; i++ {
		_, rel, err := mp.acquire(context.Background())
		if err != nil {
			t.Fatalf("seed acquire %d: %v", i, err)
		}
		rel()
	}

	res.push([]Address{addrs[1]})

	// Wait for applySet to run.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// DrainLazy: sub-pool must still be in map (not dropped immediately).
	mp.mu.RLock()
	_, present := mp.subPools[addrs[0].String()]
	mp.mu.RUnlock()
	if !present {
		t.Error("DrainLazy: sub-pool dropped immediately, expected retained")
	}

	// New acquires pick addr[1] only.
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, rel, err := mp.acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("post-drain acquire %d: %v", i, err)
		}
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
	// Use buildManagedPool so we can set the test seam before the
	// background goroutine starts reading tickerPeriod.
	mp, err := buildManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("buildManagedPool: %v", err)
	}
	mp.tickerPeriod.Store(int64(25 * time.Millisecond)) // test seam set before run
	go mp.run()
	defer mp.close()

	res.set(addrs) // expand set

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("ticker mode never picked up the new address; active = %d", len(mp.snapshotActive()))
}

func TestManagedPool_StatsAggregation_SumsAcrossSubPools(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 3)
	defer cleanup()

	mp, err := newManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// Seed each sub-pool with one conn.
	holds := make([]func(), 0, 3)
	for i := 0; i < 3; i++ {
		_, rel, err := mp.acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		holds = append(holds, rel)
	}

	st := mp.stats()
	if st.ActiveConns != 3 {
		t.Errorf("ActiveConns = %d, want 3", st.ActiveConns)
	}
	if st.Addresses != 3 {
		t.Errorf("Addresses = %d, want 3", st.Addresses)
	}
	for _, rel := range holds {
		rel()
	}
}

func TestManagedPool_Close_NoGoroutineLeak(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()

	before := runtime.NumGoroutine()
	mp, err := newManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	// Acquire once to force sub-pool creation and its background goroutines.
	_, rel, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	rel()

	_ = mp.close()

	// Allow goroutines up to 500 ms to wind down after close.
	deadline := time.Now().Add(500 * time.Millisecond)
	var after int
	for time.Now().Before(deadline) {
		after = runtime.NumGoroutine()
		if after <= before+2 { // +2: tolerance for test runner scheduler
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("goroutine leak: before=%d after=%d (want <= %d)", before, after, before+2)
}
