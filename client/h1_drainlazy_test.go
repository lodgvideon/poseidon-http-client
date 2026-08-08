package client

import (
	"context"
	"testing"
	"time"
)

// TestH1ManagedPool_DrainLazy_RemovedAddress_RetainsSubPool is the H1 sibling of
// TestManagedPool_DrainLazy_RemovedAddress_RetainsSubPool.
//
// DrainLazy's contract is that a removed address keeps its sub-pool — closure is
// left to idle eviction — as opposed to DrainHard, which drops it at once. That
// distinction was pinned on H2 and H3 only: making h1ManagedPool.beginDrain call
// dropSubPool on the DrainLazy arm left the whole ./client suite green, while the
// same mutation on the H2 and H3 pools went red.
//
// The re-add tables in managed_readd_siblings_test.go do drive DrainLazy on all
// three, but they assert that a re-added address heals — which is a different
// property and is satisfied whether or not the sub-pool was dropped in between.
func TestH1ManagedPool_DrainLazy_RemovedAddress_RetainsSubPool(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(2)
	res := newScriptedResolver(addrs)
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainLazy,
		newH1FakeDialer(),
		PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: 100 * time.Millisecond},
		nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	// Seed both sub-pools so the removal has a live one to mark draining.
	for i := 0; i < 2; i++ {
		_, rel, err := mp.acquire(context.Background())
		if err != nil {
			t.Fatalf("seed acquire %d: %v", i, err)
		}
		rel(true)
	}

	res.push([]Address{addrs[1]})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(mp.snapshotActive()); got != 1 {
		t.Fatalf("active set = %d after removal, want 1", got)
	}

	// The point: DrainLazy retains. DrainHard is what drops immediately.
	mp.mu.RLock()
	_, present := mp.subPools[addrs[0].String()]
	mp.mu.RUnlock()
	if !present {
		t.Error("DrainLazy dropped the sub-pool immediately; that is DrainHard's behaviour")
	}
}
