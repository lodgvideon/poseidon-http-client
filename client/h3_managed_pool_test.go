package client

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// h3Addrs builds n distinct fake Addresses (no live servers — the HTTP/3 managed
// pool tests dial fakes). Distinct ports keep Address.String() unique.
func h3Addrs(n int) []Address {
	out := make([]Address, n)
	for i := 0; i < n; i++ {
		out[i] = Address{Host: "10.0.0.1", Port: 4430 + i}
	}
	return out
}

func TestH3ManagedPool_RoundRobin_DistributesAcrossAddresses(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(3)
	d := newH3FakeDialer()
	mp, err := newH3ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		d.dial, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.close() }()

	// 9 sequential acquires — RoundRobin distributes 3-3-3 across the addresses.
	for i := 0; i < 9; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		cl, release, _, aerr := mp.acquire(ctx)
		cancel()
		require.NoErrorf(t, aerr, "acquire[%d]", i)
		require.Truef(t, cl.Alive(), "acquire[%d] handed out a conn that is not alive", i)
		release()
	}

	// Each address's sub-pool must have dialed at least one QUIC conn — proof of
	// fan-out across the resolver set.
	for _, a := range addrs {
		assert.GreaterOrEqualf(t, d.count(a.String()), 1,
			"address %s was never dialed: the selector did not fan out across the resolver set", a)
	}
}

func TestH3ManagedPool_NoAddresses_ReturnsErrNoAddresses(t *testing.T) {
	t.Parallel()
	mp, err := newH3ManagedPool(StaticResolver(), RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1}, newH3FakeDialer().dial, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _, _, err = mp.acquire(ctx)

	assert.Equal(t, ErrNoAddresses, err,
		"an empty resolver set must be reported as ErrNoAddresses, not as a dial or "+
			"timeout failure a caller cannot act on")
}

func TestH3ManagedPool_Watch_AddedAddress_PickedUp(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(3)
	res := newScriptedResolver([]Address{addrs[0]})
	mp, err := newH3ManagedPool(res, RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		newH3FakeDialer().dial, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.close() }()

	res.push([]Address{addrs[0], addrs[1], addrs[2]})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(mp.snapshotActive()) != 3 {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Len(t, mp.snapshotActive(), 3,
		"the active set never grew to match the resolver: a watch update was not applied")
}

func TestH3ManagedPool_DrainGraceful_RemovedAddress_KeepsInFlight(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(2)
	res := newScriptedResolver(addrs)
	d := newH3FakeDialer()
	mp, err := newH3ManagedPool(res, RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		d.dial, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.close() }()
	// Acquire a conn for addr[0] and hold it in-flight.
	cl0, rel0, _, err := mp.acquire(context.Background())
	require.NoError(t, err, "acquire 0")
	require.True(t, cl0.Alive(), "conn 0 must be alive before the drain begins")

	// Remove addr[0] from the resolver set.
	res.push([]Address{addrs[1]})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(mp.snapshotActive()) != 1 {
		time.Sleep(10 * time.Millisecond)
	}
	// The in-flight conn must stay alive during a graceful drain.
	assert.True(t, cl0.Alive(),
		"conn 0 closed during a graceful drain — a graceful drain must keep an in-flight "+
			"conn until its holder releases it")
	// New acquire must pick addr[1] only.
	_, rel1, _, err := mp.acquire(context.Background())
	require.NoError(t, err, "acquire after remove")
	defer rel1()

	// Release the in-flight conn → the drained sub-pool should close and be removed.
	rel0()

	present := true
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mp.mu.RLock()
		_, present = mp.subPools[addrs[0].String()]
		mp.mu.RUnlock()
		if !present {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.False(t, present,
		"the sub-pool for the drained address is still present after its last holder "+
			"released; expected close+evict")
}

func TestH3ManagedPool_DrainHard_RemovedAddress_ClosesImmediately(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(2)
	res := newScriptedResolver(addrs)
	d := newH3FakeDialer()
	mp, err := newH3ManagedPool(res, RoundRobin(), DrainHard,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		d.dial, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.close() }()
	cl0, rel0, _, err := mp.acquire(context.Background())
	require.NoError(t, err, "acquire 0")

	res.push([]Address{addrs[1]})

	// DrainHard closes the removed sub-pool synchronously; its conn becomes dead.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cl0.Alive() {
		time.Sleep(20 * time.Millisecond)
	}
	assert.False(t, cl0.Alive(),
		"conn 0 is still alive after a DrainHard removal — a hard drain must not wait "+
			"for the in-flight holder")
	rel0()
}

func TestH3ManagedPool_StatsAggregation_SumsAcrossSubPools(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(3)
	d := newH3FakeDialer()
	mp, err := newH3ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		d.dial, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.close() }()
	holds := make([]func(), 0, 3)
	for i := 0; i < 3; i++ {
		_, rel, _, aerr := mp.acquire(context.Background())
		require.NoErrorf(t, aerr, "acquire %d", i)
		holds = append(holds, rel)
	}

	st := mp.stats()

	assert.Equal(t, 3, st.ActiveConns,
		"aggregated ActiveConns must sum every sub-pool, not report one of them")
	assert.Equal(t, 3, st.Addresses,
		"aggregated Addresses must report the whole resolver set")
	for _, rel := range holds {
		rel()
	}
}

// --- constructor + validation ---

func TestNewManagedH3Client_Construction(t *testing.T) {
	t.Parallel()

	c, err := NewManagedH3Client(StaticResolver(h3Addrs(2)...), &tls.Config{ServerName: "h"}, WithDrainMode(DrainHard))

	require.NoError(t, err, "NewManagedH3Client")
	defer func() { _ = c.Close() }()
	mt, ok := c.tr.(*h3ManagedTransport)
	require.Truef(t, ok, "transport is %T, want *h3ManagedTransport", c.tr)
	assert.Equal(t, DrainHard, mt.mp.drainMode,
		"WithDrainMode did not reach the managed pool")
}

func TestNewClient_H3Managed_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opts ClientOptions
		why  string
	}{
		{
			name: "missing Resolver",
			opts: ClientOptions{Transport: TransportH3Managed, TLSConfig: &tls.Config{ServerName: "h"}},
			why:  "a managed transport has no addressing at all without a Resolver",
		},
		{
			name: "Addr set alongside a Resolver",
			opts: ClientOptions{
				Addr: "h:443", Transport: TransportH3Managed, TLSConfig: &tls.Config{ServerName: "h"},
				Resolver: StaticResolver(h3Addrs(1)...),
			},
			why: "Resolver owns addressing on a managed transport, so Addr is ambiguous",
		},
		{
			name: "missing TLSConfig",
			opts: ClientOptions{Transport: TransportH3Managed, Resolver: StaticResolver(h3Addrs(1)...)},
			why:  "HTTP/3 cannot be dialled without TLS",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClient(tc.opts)

			assert.Errorf(t, err, "NewClient accepted %s: %s", tc.name, tc.why)
		})
	}
}
