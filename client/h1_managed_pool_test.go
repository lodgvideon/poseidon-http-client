package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.close() }()

	// 9 sequential acquires — RoundRobin distributes 3-3-3 across the addresses.
	for i := 0; i < 9; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		c, release, aerr := mp.acquire(ctx)
		cancel()

		require.NoErrorf(t, aerr, "acquire[%d]", i)
		require.Truef(t, c.IsAlive(), "acquire[%d] handed back a dead conn", i)
		release(true)
	}

	// Each address's sub-pool must have dialed a conn — proof of fan-out.
	for _, a := range addrs {
		assert.GreaterOrEqualf(t, d.count(a.String()), 1,
			"address %s was never dialed — RoundRobin did not fan out across the set", a)
	}
}

func TestH1ManagedPool_NoAddresses_ReturnsErrNoAddresses(t *testing.T) {
	t.Parallel()
	mp, err := newH1ManagedPool(StaticResolver(), RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _, aerr := mp.acquire(ctx)

	// assert.Equal, not ErrorIs: the original compared with ==, and ErrorIs would
	// also accept a wrapped value. Widening a sentinel check while "just" moving
	// it to testify removes a capability the caller relies on.
	assert.Equal(t, ErrNoAddresses, aerr,
		"an empty resolver result must surface as exactly ErrNoAddresses")
}

func TestH1ManagedPool_Watch_AddedAddress_PickedUp(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(3)
	res := newScriptedResolver([]Address{addrs[0]})
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.close() }()

	res.push([]Address{addrs[0], addrs[1], addrs[2]})

	assert.Eventually(t, func() bool { return len(mp.snapshotActive()) == 3 },
		2*time.Second, 10*time.Millisecond,
		"active set never grew to 3 — a resolver update that adds an address was not picked up")
}

func TestH1ManagedPool_DrainGraceful_RemovedAddress_KeepsInFlight(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(2)
	res := newScriptedResolver(addrs)
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.close() }()
	// The first RoundRobin pick is deterministic (counter starts at zero), so
	// this conn belongs to addrs[0] — the address removed below.
	c0, rel0, err := mp.acquire(context.Background())
	require.NoError(t, err, "acquire 0")

	res.push([]Address{addrs[1]})

	// CONTROL: the removal must actually have landed, or "the conn is still
	// alive" below is the trivially-true state of a pool nothing happened to.
	require.Eventually(t, func() bool { return len(mp.snapshotActive()) == 1 },
		2*time.Second, 10*time.Millisecond,
		"the removed address never left the active set, so no drain was ever started")
	// The in-flight conn must survive a graceful drain until it is released.
	assert.True(t, c0.IsAlive(),
		"in-flight conn closed during graceful drain — expected alive until release")
	// A new acquire must pick the surviving address only.
	_, rel1, err := mp.acquire(context.Background())
	require.NoError(t, err, "acquire after remove")
	defer rel1(true)
	// Releasing the held conn lets the drained sub-pool close and drop out.
	rel0(true)
	assert.Eventually(t, func() bool {
		mp.mu.RLock()
		_, present := mp.subPools[addrs[0].String()]
		mp.mu.RUnlock()
		return !present
	}, 3*time.Second, 20*time.Millisecond,
		"sub-pool for the drained address is still present after release; expected close+evict")
}

func TestH1ManagedPool_DrainHard_RemovedAddress_ClosesImmediately(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(2)
	res := newScriptedResolver(addrs)
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainHard,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.close() }()
	c0, rel0, err := mp.acquire(context.Background())
	require.NoError(t, err, "acquire 0")

	// Remove addrs[0] (the deterministic first RoundRobin pick); DrainHard closes
	// the removed sub-pool synchronously, taking its conn down with it.
	res.push([]Address{addrs[1]})

	assert.Eventually(t, func() bool { return !c0.IsAlive() },
		3*time.Second, 20*time.Millisecond,
		"conn still alive after DrainHard removal — hard drain degraded to graceful")
	rel0(false)
}

func TestH1ManagedPool_StatsAggregation_SumsAcrossSubPools(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(3)
	mp, err := newH1ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.close() }()
	holds := make([]func(bool), 0, 3)
	for i := 0; i < 3; i++ {
		_, rel, aerr := mp.acquire(context.Background())
		require.NoErrorf(t, aerr, "acquire %d", i)
		holds = append(holds, rel)
	}

	st := mp.stats()

	assert.Equal(t, 3, st.ActiveConns,
		"stats must sum ActiveConns across every sub-pool, not report one of them")
	assert.Equal(t, 3, st.Addresses,
		"stats must report every resolved address, not only the ones dialed")
	for _, rel := range holds {
		rel(true)
	}
}

func TestH1ManagedPool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	mp, err := newH1ManagedPool(StaticResolver(h1Addrs(2)...), RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")

	err1 := mp.close()
	err2 := mp.close()

	require.NoError(t, err1, "first close")
	require.NoError(t, err2, "second close must be a no-op, not an error")
}

// --- managed transport ---

// TestH1ManagedTransport_EndToEnd drives real requests through the managed
// transport and proves the resolver's addresses are all exercised.
func TestH1ManagedTransport_EndToEnd(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(2)
	d := newH1FakeDialer()
	c, err := NewManagedH1Client(StaticResolver(addrs...), d, WithDefaultScheme("http"))
	require.NoError(t, err, "NewManagedH1Client")
	defer func() { _ = c.Close() }()

	for i := 0; i < 4; i++ {
		resp, rerr := h1Get(context.Background(), c)

		require.NoErrorf(t, rerr, "request %d", i)
		require.Equalf(t, 200, resp.Status, "request %d", i)
	}

	for _, a := range addrs {
		assert.GreaterOrEqualf(t, d.count(a.String()), 1,
			"address %s never dialed — the selector did not spread requests", a)
	}
}

func TestNewManagedH1Client_Construction(t *testing.T) {
	t.Parallel()
	c, err := NewManagedH1Client(StaticResolver(h1Addrs(2)...), newH1FakeDialer(), WithDrainMode(DrainHard))
	require.NoError(t, err, "NewManagedH1Client")
	defer func() { _ = c.Close() }()

	mt, ok := c.tr.(*h1ManagedTransport)

	require.Truef(t, ok, "transport is %T, want *h1ManagedTransport", c.tr)
	assert.Equal(t, DrainHard, mt.mp.drainMode,
		"WithDrainMode must reach the managed pool, or the option is silently ignored")
}
