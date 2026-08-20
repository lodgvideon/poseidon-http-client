package client

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.close() }()
	// Seed both sub-pools so the removal has a live one to mark draining.
	for i := 0; i < 2; i++ {
		_, rel, err := mp.acquire(context.Background())
		require.NoErrorf(t, err, "seed acquire %d", i)
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

	require.Lenf(t, mp.snapshotActive(), 1,
		"active set = %d after removal, want 1", len(mp.snapshotActive()))
	// The point: DrainLazy retains. DrainHard is what drops immediately.
	mp.mu.RLock()
	_, present := mp.subPools[addrs[0].String()]
	mp.mu.RUnlock()
	assert.True(t, present,
		"DrainLazy dropped the sub-pool immediately; that is DrainHard's behaviour")
}

// TestH3ManagedPool_DrainLazy_RemovedAddress_RetainsSubPool is the H3 sibling.
//
// This one was nearly missed. A first, ad-hoc mutation run reported H3 as
// already covered, and that result was recorded in the PR. Re-run through the
// scripted gate it SURVIVES — twice, whole-suite, deterministically. The earlier
// "caught" was a flake in an unrelated timing-sensitive test that happened to
// fail in the same run, taken as evidence because it arrived in the right shape.
// Single-run mutation results are not evidence in a suite with timing tests.
func TestH3ManagedPool_DrainLazy_RemovedAddress_RetainsSubPool(t *testing.T) {
	t.Parallel()

	addrs := h3Addrs(2)
	d := newH3FakeDialer()
	res := newScriptedResolver(addrs)
	mp, err := newH3ManagedPool(res, RoundRobin(), DrainLazy,
		&tls.Config{ServerName: "h"},
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: 100 * time.Millisecond},
		d.dial, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.close() }()
	for i := 0; i < 2; i++ {
		_, rel, err := mp.acquire(context.Background())
		require.NoErrorf(t, err, "seed acquire %d", i)
		rel()
	}

	res.push([]Address{addrs[1]})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.Lenf(t, mp.snapshotActive(), 1,
		"active set = %d after removal, want 1", len(mp.snapshotActive()))
	mp.mu.RLock()
	_, present := mp.subPools[addrs[0].String()]
	mp.mu.RUnlock()
	assert.True(t, present,
		"DrainLazy dropped the sub-pool immediately; that is DrainHard's behaviour")
}
