package client

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The address-revive fix (#385) shipped to all three managed pools, but only the
// HTTP/2 one had tests. Deleting `s.draining = false` from h1_managed_pool.go or
// h3_managed_pool.go left the entire ./client suite green — so the exact bug
// that fix closed (a resolver flap blackholing a re-added address forever under
// DrainLazy) could regress on two of three protocols unnoticed.
//
// These are the h1/h3 counterparts of client/managed_readd_test.go. They use the
// fake dialers, so they exercise the pool's own bookkeeping rather than a socket.

func waitActiveH1(mp *h1ManagedPool, want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	got := -1
	for time.Now().Before(deadline) {
		if got = len(mp.SnapshotActive()); got == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return got
}

func waitActiveH3(mp *h3ManagedPool, want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	got := -1
	for time.Now().Before(deadline) {
		if got = len(mp.SnapshotActive()); got == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return got
}

// TestH1ManagedPool_AddressReAddedAfterRemoval pins the revive on the HTTP/1.1
// managed pool across every drain mode. Under DrainLazy beginDrain is an
// explicit no-op, so without the revive the sub-pool keeps draining == true
// forever and both snapshotActive and getOrCreateSubPool exclude the address for
// the life of the pool.
func TestH1ManagedPool_AddressReAddedAfterRemoval(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode DrainMode
	}{
		{"DrainLazy", DrainLazy},
		{"DrainGraceful", DrainGraceful},
		{"DrainHard", DrainHard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addrs := h1Addrs(2)
			res := newScriptedResolver(addrs)
			mp, err := newH1ManagedPool(res, RoundRobin(), tc.mode,
				newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
			require.NoError(t, err, "newH1ManagedPool")
			defer func() { _ = mp.Close() }()
			require.Equal(t, 2, waitActiveH1(mp, 2, 2*time.Second), "initial active set")
			// Materialise the sub-pool so the removal has something to mark.
			require.NotNil(t, mp.GetOrCreateSubPool(addrs[0]),
				"getOrCreateSubPool nil for a live address")

			res.push([]Address{addrs[1]})
			require.Equal(t, 1, waitActiveH1(mp, 1, 2*time.Second), "active set after removal")
			res.push(addrs)

			require.Equalf(t, 2, waitActiveH1(mp, 2, 3*time.Second),
				"after re-add active = %d, want 2 — the address is blackholed",
				len(mp.SnapshotActive()))
			require.NotNil(t, mp.GetOrCreateSubPool(addrs[0]),
				"getOrCreateSubPool still refuses the re-added address")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, rel, _, err := mp.Acquire(ctx)
			require.NoError(t, err, "acquire after re-add")
			require.NotNil(t, c, "acquire after re-add returned a nil conn")
			rel(true)
		})
	}
}

// TestH1ManagedPool_SingleAddressFlapDoesNotBlackhole is the sharpest form: one
// address, removed and re-added. Under the bug this pool could never serve
// another request.
func TestH1ManagedPool_SingleAddressFlapDoesNotBlackhole(t *testing.T) {
	addrs := h1Addrs(1)
	res := newScriptedResolver(addrs)
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainLazy,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.Close() }()
	require.Equal(t, 1, waitActiveH1(mp, 1, 2*time.Second), "initial active set")
	require.NotNil(t, mp.GetOrCreateSubPool(addrs[0]),
		"getOrCreateSubPool nil for the only address")

	res.push([]Address{})
	require.Equal(t, 0, waitActiveH1(mp, 0, 2*time.Second), "active set after removal")
	res.push(addrs)

	require.Equalf(t, 1, waitActiveH1(mp, 1, 3*time.Second),
		"after re-add active = %d, want 1 — a DNS flap blackholed the only address",
		len(mp.SnapshotActive()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, rel, _, err := mp.Acquire(ctx)
	require.NoError(t, err, "acquire after a single-address flap")
	require.NotNil(t, c, "acquire after a flap returned a nil conn")
	rel(true)
}

// TestH3ManagedPool_AddressReAddedAfterRemoval is the HTTP/3 counterpart.
func TestH3ManagedPool_AddressReAddedAfterRemoval(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode DrainMode
	}{
		{"DrainLazy", DrainLazy},
		{"DrainGraceful", DrainGraceful},
		{"DrainHard", DrainHard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addrs := h3Addrs(2)
			d := newH3FakeDialer()
			mp, err := newH3ManagedPool(newScriptedResolver(addrs), RoundRobin(), tc.mode,
				&tls.Config{ServerName: "h"},
				PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
				d.dial, nil, nil)
			require.NoError(t, err, "newH3ManagedPool")
			defer func() { _ = mp.Close() }()
			res := mp.Resolver().(*scriptedResolver)
			require.Equal(t, 2, waitActiveH3(mp, 2, 2*time.Second), "initial active set")
			require.NotNil(t, mp.GetOrCreateSubPool(addrs[0]),
				"getOrCreateSubPool nil for a live address")

			res.push([]Address{addrs[1]})
			require.Equal(t, 1, waitActiveH3(mp, 1, 2*time.Second), "active set after removal")
			res.push(addrs)

			require.Equalf(t, 2, waitActiveH3(mp, 2, 3*time.Second),
				"after re-add active = %d, want 2 — the address is blackholed",
				len(mp.SnapshotActive()))
			require.NotNil(t, mp.GetOrCreateSubPool(addrs[0]),
				"getOrCreateSubPool still refuses the re-added address")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, rel, _, err := mp.Acquire(ctx)
			require.NoError(t, err, "acquire after re-add")
			// c is an interface (h3Client): require.NotNil would accept a typed
			// nil through reflection, so compare against nil directly.
			require.Truef(t, c != nil, "acquire after re-add returned a nil conn")
			rel()
		})
	}
}

// TestH1ManagedPool_ReviveBeatsDrainWatcher and its h3 twin guard the hazard the
// revive itself introduces: under DrainGraceful a watchDrain goroutine is
// already polling the removed sub-pool, and clearing draining in place makes its
// next idle poll want to close a sub-pool whose address is live again.
//
// The held conn is load-bearing. Without it the sub-pool is already idle at
// removal, so the watcher drops it on its first 20ms poll and is gone before the
// address returns — the guard never executes and the test passes with it
// deleted. Identity, not liveness, is the assertion: a wrongly dropped sub-pool
// is silently recreated, so only the pointer shows the damage (a needless
// reconnect for an address that never went away).
func TestH1ManagedPool_ReviveBeatsDrainWatcher(t *testing.T) {
	addrs := h1Addrs(1)
	res := newScriptedResolver(addrs)
	mp, err := newH1ManagedPool(res, RoundRobin(), DrainGraceful,
		newH1FakeDialer(), h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.Close() }()
	require.Equal(t, 1, waitActiveH1(mp, 1, 2*time.Second), "initial active set")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, relHeld, _, err := mp.Acquire(ctx) // hold it: the sub-pool must NOT be idle
	require.NoError(t, err, "initial acquire")
	before := mp.GetOrCreateSubPool(addrs[0])
	require.NotNil(t, before, "no sub-pool for the live address")

	res.push([]Address{})
	require.Equal(t, 0, waitActiveH1(mp, 0, 2*time.Second), "active set after removal")
	time.Sleep(80 * time.Millisecond) // let the watcher poll and see the in-flight
	res.push(addrs)
	require.Equal(t, 1, waitActiveH1(mp, 1, 3*time.Second), "active set after re-add")
	relHeld(true) // now it goes idle, with the address live again
	time.Sleep(600 * time.Millisecond)

	after := mp.GetOrCreateSubPool(addrs[0])
	require.NotNil(t, after, "drain watcher left the revived address unusable")
	require.Same(t, before, after,
		"drain watcher closed the revived sub-pool; the live address paid for a needless reconnect")
}

func TestH3ManagedPool_ReviveBeatsDrainWatcher(t *testing.T) {
	addrs := h3Addrs(1)
	d := newH3FakeDialer()
	mp, err := newH3ManagedPool(newScriptedResolver(addrs), RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"},
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
		d.dial, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.Close() }()
	res := mp.Resolver().(*scriptedResolver)
	require.Equal(t, 1, waitActiveH3(mp, 1, 2*time.Second), "initial active set")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, relHeld, _, err := mp.Acquire(ctx)
	require.NoError(t, err, "initial acquire")
	before := mp.GetOrCreateSubPool(addrs[0])
	require.NotNil(t, before, "no sub-pool for the live address")

	res.push([]Address{})
	require.Equal(t, 0, waitActiveH3(mp, 0, 2*time.Second), "active set after removal")
	time.Sleep(80 * time.Millisecond)
	res.push(addrs)
	require.Equal(t, 1, waitActiveH3(mp, 1, 3*time.Second), "active set after re-add")
	relHeld()
	time.Sleep(600 * time.Millisecond)

	after := mp.GetOrCreateSubPool(addrs[0])
	require.NotNil(t, after, "drain watcher left the revived address unusable")
	require.Same(t, before, after,
		"drain watcher closed the revived sub-pool; the live address paid for a needless reconnect")
}
