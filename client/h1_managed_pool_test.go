package client

import (
	"context"
	"testing"
	"time"
)

// h1Addrs builds n distinct fake Addresses (no live servers — the HTTP/1.1
// managed pool tests dial fakes). Distinct ports keep Address.String() unique.
func h1Addrs(n int) []Address {
	out := make([]Address, n)
	for i := 0; i < n; i++ {
		out[i] = Address{Host: "10.0.0.1", Port: 8080 + i}
	}
	return out
}

// h1ManagedPoolOpts is the shared sub-pool config for these tests: one conn per
// address so fan-out is observable per-address, with a fast health tick.
func h1ManagedPoolOpts() PoolOptions {
	return PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Second}
}

func TestH1ManagedPool_RoundRobin_DistributesAcrossAddresses(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(3)
	d := newH1FakeDialer()
	mp, err := newH1ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		d, h1ManagedPoolOpts(), nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	// 9 sequential acquires — RoundRobin distributes 3-3-3 across the addresses.
	for i := 0; i < 9; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		c, release, _, err := mp.acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("acquire[%d] = %v", i, err)
		}
		if !c.IsAlive() {
			t.Fatalf("acquire[%d] conn not alive", i)
		}
		release(true)
	}

	// Each address's sub-pool must have dialed a conn — proof of fan-out.
	for _, a := range addrs {
		if got := d.count(a.String()); got < 1 {
			t.Errorf("address %s dialed %d conns, want >= 1", a, got)
		}
	}
}

func TestH1ManagedPool_NoAddresses_ReturnsErrNoAddresses(t *testing.T) {
	t.Parallel()
	mp, err := newH1ManagedPool(StaticResolver(), RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, _, err := mp.acquire(ctx); err != ErrNoAddresses {
		t.Errorf("acquire err = %v, want ErrNoAddresses", err)
	}
}

func TestH1ManagedPool_Watch_AddedAddress_PickedUp(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(3)
	res := newScriptedResolver([]Address{addrs[0]})
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	res.push([]Address{addrs[0], addrs[1], addrs[2]})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("active set never grew to 3; got %d", len(mp.snapshotActive()))
}

func TestH1ManagedPool_DrainGraceful_RemovedAddress_KeepsInFlight(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(2)
	res := newScriptedResolver(addrs)
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	// The first RoundRobin pick is deterministic (counter starts at zero), so
	// this conn belongs to addrs[0] — the address removed below.
	c0, rel0, _, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 0: %v", err)
	}

	res.push([]Address{addrs[1]})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The in-flight conn must survive a graceful drain until it is released.
	if !c0.IsAlive() {
		t.Error("in-flight conn closed during graceful drain — expected alive until release")
	}

	// A new acquire must pick the surviving address only.
	_, rel1, _, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after remove: %v", err)
	}
	defer rel1(true)

	// Releasing the held conn lets the drained sub-pool close and drop out.
	rel0(true)
	deadline = time.Now().Add(3 * time.Second)
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

func TestH1ManagedPool_DrainHard_RemovedAddress_ClosesImmediately(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(2)
	res := newScriptedResolver(addrs)
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainHard,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	c0, rel0, _, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 0: %v", err)
	}

	// Remove addrs[0] (the deterministic first RoundRobin pick); DrainHard closes
	// the removed sub-pool synchronously, taking its conn down with it.
	res.push([]Address{addrs[1]})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !c0.IsAlive() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if c0.IsAlive() {
		t.Error("conn still alive after DrainHard removal")
	}
	rel0(false)
}

func TestH1ManagedPool_StatsAggregation_SumsAcrossSubPools(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(3)
	mp, err := newH1ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	holds := make([]func(bool), 0, 3)
	for i := 0; i < 3; i++ {
		_, rel, _, err := mp.acquire(context.Background())
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
		rel(true)
	}
}

func TestH1ManagedPool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	mp, err := newH1ManagedPool(StaticResolver(h1Addrs(2)...), RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	if err := mp.close(); err != nil {
		t.Fatalf("first close = %v", err)
	}
	if err := mp.close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
}

// --- managed transport ---

// TestH1ManagedTransport_EndToEnd drives real requests through the managed
// transport and proves the resolver's addresses are all exercised.
func TestH1ManagedTransport_EndToEnd(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(2)
	d := newH1FakeDialer()
	c, err := NewManagedH1Client(StaticResolver(addrs...), d, WithDefaultScheme("http"))
	if err != nil {
		t.Fatalf("NewManagedH1Client: %v", err)
	}
	defer func() { _ = c.Close() }()

	for i := 0; i < 4; i++ {
		resp, err := h1Get(context.Background(), c)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status != 200 {
			t.Fatalf("request %d: status = %d, want 200", i, resp.Status)
		}
	}
	for _, a := range addrs {
		if got := d.count(a.String()); got < 1 {
			t.Errorf("address %s never dialed — selector did not spread requests", a)
		}
	}
}

func TestNewManagedH1Client_Construction(t *testing.T) {
	t.Parallel()
	c, err := NewManagedH1Client(StaticResolver(h1Addrs(2)...), newH1FakeDialer(), WithDrainMode(DrainHard))
	if err != nil {
		t.Fatalf("NewManagedH1Client: %v", err)
	}
	defer func() { _ = c.Close() }()
	mt, ok := c.tr.(*h1ManagedTransport)
	if !ok {
		t.Fatalf("transport is %T, want *h1ManagedTransport", c.tr)
	}
	if mt.mp.drainMode != DrainHard {
		t.Fatalf("drainMode = %v, want DrainHard", mt.mp.drainMode)
	}
}
