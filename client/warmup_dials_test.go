package client

import (
	"testing"
	"time"
)

// TestWarmup_ManagedPool_PreDialsBeforeAnyRequest is the regression test for the
// half of #399 that opened nothing at all.
//
// managedPool.warmup iterated mp.subPools, which getOrCreateSubPool fills lazily
// on the first acquire — so on a freshly constructed pool it was empty, warmup
// took its len(subs)==0 early return, and Warmup pre-dialled zero connections.
// The whole point of a warmup is to run before the first request, which is
// exactly the state the old code could not handle.
func TestWarmup_ManagedPool_PreDialsBeforeAnyRequest(t *testing.T) {
	addrs, counts, cleanup := startH2Servers(t, 2)
	defer cleanup()

	res := newScriptedResolver(addrs)
	mp, err := newManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 8, HealthCheckPeriod: time.Second}, nil, nil)
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	if got := waitActive(mp, 2, 2*time.Second); got != 2 {
		t.Fatalf("resolved %d addresses, want 2", got)
	}

	// No request has been issued. This is the state the bug lived in.
	mp.warmup(4)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if int(counts[0].Load())+int(counts[1].Load()) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	d0, d1 := int(counts[0].Load()), int(counts[1].Load())
	if d0+d1 == 0 {
		t.Fatalf("Warmup opened nothing (server dials: %d, %d) — it pre-dialled zero connections", d0, d1)
	}
	if d0 == 0 || d1 == 0 {
		t.Fatalf("Warmup skipped an address: dials = %d, %d; both resolved addresses must be warmed", d0, d1)
	}
}

// TestWarmup_Pool_OpensExactlyN pins the other half: a multiplexed pool opened
// exactly ONE connection regardless of n.
//
// The old warmup was a loop of acquire+release. pickLeastLoaded returns any conn
// with a free stream slot, so the second acquire reused the conn the first had
// just released. Holding them instead does not help either — a held conn still
// has free slots — which is why h1Pool's fix (hold, then release together) does
// not port to a multiplexed pool. The dials now come from the actor directly.
func TestWarmup_Pool_OpensExactlyN(t *testing.T) {
	addrs, counts, cleanup := startH2Servers(t, 1)
	defer cleanup()

	p := newPool(addrs[0].String(), newConnOpts(),
		PoolOptions{MaxConnsPerHost: 3, MaxStreamsPerConn: 100, HealthCheckPeriod: time.Second}, nil, nil)
	defer p.Close()

	p.warmup(3)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if int(counts[0].Load()) >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := int(counts[0].Load()); got != 3 {
		t.Fatalf("warmup(3) opened %d conns, want 3 — a multiplexed pool cannot warm up via acquire+release", got)
	}

	// Idempotent: already at target, so no further dials.
	p.warmup(3)
	time.Sleep(300 * time.Millisecond)
	if got := int(counts[0].Load()); got != 3 {
		t.Fatalf("second warmup(3) opened more conns (%d); it must be a no-op at target", got)
	}
}

// TestWarmup_Pool_RespectsMaxConnsPerHost pins that warmup cannot exceed the cap
// the pool was configured with, which is the invariant the old acquire-based
// loop got for free and a direct dial path must reassert.
func TestWarmup_Pool_RespectsMaxConnsPerHost(t *testing.T) {
	addrs, counts, cleanup := startH2Servers(t, 1)
	defer cleanup()

	p := newPool(addrs[0].String(), newConnOpts(),
		PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 100, HealthCheckPeriod: time.Second}, nil, nil)
	defer p.Close()

	p.warmup(10)
	time.Sleep(1500 * time.Millisecond)
	if got := int(counts[0].Load()); got != 2 {
		t.Fatalf("warmup(10) opened %d conns with MaxConnsPerHost=2, want 2", got)
	}
}
