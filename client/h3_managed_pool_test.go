package client

import (
	"context"
	"crypto/tls"
	"testing"
	"time"
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
	if err != nil {
		t.Fatalf("newH3ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	// 9 sequential acquires — RoundRobin distributes 3-3-3 across the addresses.
	for i := 0; i < 9; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		cl, release, err := mp.acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("acquire[%d] = %v", i, err)
		}
		if !cl.Alive() {
			t.Fatalf("acquire[%d] conn not alive", i)
		}
		release()
	}

	// Each address's sub-pool must have dialed at least one QUIC conn — proof of
	// fan-out across the resolver set.
	for _, a := range addrs {
		if got := d.count(a.String()); got < 1 {
			t.Errorf("address %s dialed %d conns, want >= 1", a, got)
		}
	}
}

func TestH3ManagedPool_NoAddresses_ReturnsErrNoAddresses(t *testing.T) {
	t.Parallel()
	mp, err := newH3ManagedPool(StaticResolver(), RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1}, newH3FakeDialer().dial, nil, nil)
	if err != nil {
		t.Fatalf("newH3ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := mp.acquire(ctx); err != ErrNoAddresses {
		t.Errorf("acquire err = %v, want ErrNoAddresses", err)
	}
}

func TestH3ManagedPool_Watch_AddedAddress_PickedUp(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(3)
	res := newScriptedResolver([]Address{addrs[0]})
	mp, err := newH3ManagedPool(res, RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		newH3FakeDialer().dial, nil, nil)
	if err != nil {
		t.Fatalf("newH3ManagedPool: %v", err)
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

func TestH3ManagedPool_DrainGraceful_RemovedAddress_KeepsInFlight(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(2)
	res := newScriptedResolver(addrs)
	d := newH3FakeDialer()
	mp, err := newH3ManagedPool(res, RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		d.dial, nil, nil)
	if err != nil {
		t.Fatalf("newH3ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	// Acquire a conn for addr[0] and hold it in-flight.
	cl0, rel0, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 0: %v", err)
	}
	if !cl0.Alive() {
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

	// The in-flight conn must stay alive during a graceful drain.
	if !cl0.Alive() {
		t.Error("conn 0 closed during graceful drain — expected alive until release")
	}

	// New acquire must pick addr[1] only.
	_, rel1, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after remove: %v", err)
	}
	defer rel1()

	// Release the in-flight conn → the drained sub-pool should close and be removed.
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

func TestH3ManagedPool_DrainHard_RemovedAddress_ClosesImmediately(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(2)
	res := newScriptedResolver(addrs)
	d := newH3FakeDialer()
	mp, err := newH3ManagedPool(res, RoundRobin(), DrainHard,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		d.dial, nil, nil)
	if err != nil {
		t.Fatalf("newH3ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	cl0, rel0, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 0: %v", err)
	}

	res.push([]Address{addrs[1]})

	// DrainHard closes the removed sub-pool synchronously; its conn becomes dead.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !cl0.Alive() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if cl0.Alive() {
		t.Error("conn 0 still alive after DrainHard removal")
	}
	rel0()
}

func TestH3ManagedPool_StatsAggregation_SumsAcrossSubPools(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(3)
	d := newH3FakeDialer()
	mp, err := newH3ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"}, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		d.dial, nil, nil)
	if err != nil {
		t.Fatalf("newH3ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

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

// --- constructor + validation ---

func TestNewManagedH3Client_Construction(t *testing.T) {
	t.Parallel()
	c, err := NewManagedH3Client(StaticResolver(h3Addrs(2)...), &tls.Config{ServerName: "h"}, WithDrainMode(DrainHard))
	if err != nil {
		t.Fatalf("NewManagedH3Client: %v", err)
	}
	defer func() { _ = c.Close() }()
	mt, ok := c.tr.(*h3ManagedTransport)
	if !ok {
		t.Fatalf("transport is %T, want *h3ManagedTransport", c.tr)
	}
	if mt.mp.drainMode != DrainHard {
		t.Fatalf("drainMode = %v, want DrainHard", mt.mp.drainMode)
	}
}

func TestNewClient_H3Managed_Validation(t *testing.T) {
	t.Parallel()
	// Missing Resolver → rejected.
	if _, err := NewClient(ClientOptions{
		Transport: TransportH3Managed, TLSConfig: &tls.Config{ServerName: "h"},
	}); err == nil {
		t.Fatal("missing Resolver = nil error, want failure")
	}
	// Addr set on a managed transport → rejected (Resolver owns addressing).
	if _, err := NewClient(ClientOptions{
		Addr: "h:443", Transport: TransportH3Managed, TLSConfig: &tls.Config{ServerName: "h"},
		Resolver: StaticResolver(h3Addrs(1)...),
	}); err == nil {
		t.Fatal("Addr with TransportH3Managed = nil error, want failure")
	}
	// Missing TLSConfig → rejected.
	if _, err := NewClient(ClientOptions{
		Transport: TransportH3Managed, Resolver: StaticResolver(h3Addrs(1)...),
	}); err == nil {
		t.Fatal("missing TLSConfig = nil error, want failure")
	}
}
