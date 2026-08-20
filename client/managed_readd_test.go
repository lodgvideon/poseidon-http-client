package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitActive polls snapshotActive until it holds want addresses or the deadline
// passes, returning the last snapshot length seen.
func waitActive(mp *managedPool, want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	got := -1
	for time.Now().Before(deadline) {
		got = len(mp.snapshotActive())
		if got == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return got
}

// TestManagedPool_AddressReAddedAfterRemoval is the regression test for the
// resolver-flap blackhole. subPoolState.draining was set on removal and never
// cleared, so an address that came back stayed excluded by snapshotActive and
// refused by getOrCreateSubPool.
//
// Under DrainHard and DrainGraceful the sub-pool is eventually removed from the
// registry, so a re-add heals once that happens. Under DrainLazy beginDrain is
// an explicit no-op and nothing else ever removes it — the address was excluded
// for the life of the pool. With a single resolved address, which is the
// ordinary case, that is a permanent ErrNoAddresses from a transient DNS flap.
func TestManagedPool_AddressReAddedAfterRemoval(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode DrainMode
	}{
		{"DrainLazy", DrainLazy},
		{"DrainGraceful", DrainGraceful},
		{"DrainHard", DrainHard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addrs, _, cleanup := startH2Servers(t, 2)
			defer cleanup()
			res := newScriptedResolver([]Address{addrs[0], addrs[1]})
			mp, err := newManagedPool(res, RoundRobin(), tc.mode, newConnOpts(),
				PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
			require.NoError(t, err, "newManagedPool")
			defer mp.close()
			require.Equal(t, 2, waitActive(mp, 2, 2*time.Second), "initial active set")
			// Materialise a sub-pool for addrs[0] so the removal has something to
			// mark draining — without this the registry entry never exists and the
			// bug cannot show.
			require.NotNil(t, mp.getOrCreateSubPool(addrs[0]),
				"getOrCreateSubPool returned nil for a live address")

			// Flap: drop addrs[0], then bring it back.
			res.push([]Address{addrs[1]})
			require.Equal(t, 1, waitActive(mp, 1, 2*time.Second), "active set after removal")
			res.push([]Address{addrs[0], addrs[1]})

			require.Equalf(t, 2, waitActive(mp, 2, 3*time.Second),
				"after re-add active = %d, want 2 — the re-added address is blackholed",
				len(mp.snapshotActive()))
			require.NotNil(t, mp.getOrCreateSubPool(addrs[0]),
				"getOrCreateSubPool still refuses the re-added address")
			// And it must actually serve.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, release, err := mp.acquire(ctx)
			require.NoError(t, err, "acquire after re-add")
			require.NotNil(t, c, "acquire returned a nil conn")
			release()
		})
	}
}

// TestManagedPool_SingleAddressFlapDoesNotBlackhole is the sharpest form: one
// address, removed and re-added. Under the bug this pool could never serve
// another request under DrainLazy.
func TestManagedPool_SingleAddressFlapDoesNotBlackhole(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()
	res := newScriptedResolver([]Address{addrs[0]})
	mp, err := newManagedPool(res, RoundRobin(), DrainLazy, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
	require.NoError(t, err, "newManagedPool")
	defer mp.close()
	require.Equal(t, 1, waitActive(mp, 1, 2*time.Second), "initial active set")
	require.NotNil(t, mp.getOrCreateSubPool(addrs[0]),
		"getOrCreateSubPool nil for the only address")

	res.push([]Address{})
	require.Equal(t, 0, waitActive(mp, 0, 2*time.Second), "active set after removal")
	res.push([]Address{addrs[0]})

	require.Equalf(t, 1, waitActive(mp, 1, 3*time.Second),
		"after re-add active = %d, want 1 — a DNS flap blackholed the only address",
		len(mp.snapshotActive()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, release, err := mp.acquire(ctx)
	require.NoError(t, err, "acquire after single-address flap")
	require.NotNil(t, c, "acquire after a flap returned a nil conn")
	release()
}

// TestManagedPool_ReviveBeatsDrainWatcher guards the hazard the fix itself
// introduces. Under DrainGraceful a watchDrain goroutine polls the removed
// sub-pool until it goes idle. Clearing draining in place makes that
// goroutine's next idle poll want to close a sub-pool whose address is live
// again — it must not, and dropSubPool must not delete a registry entry
// belonging to a different sub-pool for the same address.
//
// The in-flight conn is load-bearing: it is held across the removal so the
// watcher's first poll sees InFlightStreams != 0 and backs off instead of
// dropping the sub-pool immediately. Without it the watcher is already gone by
// the time the address comes back, and both guards go unexercised — the test
// passes with either of them deleted.
func TestManagedPool_ReviveBeatsDrainWatcher(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()
	res := newScriptedResolver([]Address{addrs[0]})
	mp, err := newManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, nil, nil)
	require.NoError(t, err, "newManagedPool")
	defer mp.close()
	require.Equal(t, 1, waitActive(mp, 1, 2*time.Second), "initial active set")
	// Hold a conn so the sub-pool is NOT idle while it drains.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	held, releaseHeld, err := mp.acquire(ctx)
	require.NoError(t, err, "initial acquire")
	require.NotNil(t, held, "initial acquire returned nil conn")
	before := mp.getOrCreateSubPool(addrs[0])
	require.NotNil(t, before, "no sub-pool for the live address")

	res.push([]Address{})
	require.Equal(t, 0, waitActive(mp, 0, 2*time.Second), "active set after removal")
	// Let the watcher poll at least once and observe the in-flight stream.
	time.Sleep(80 * time.Millisecond)
	res.push([]Address{addrs[0]})
	require.Equal(t, 1, waitActive(mp, 1, 3*time.Second), "active set after re-add")
	// Now let it go idle. The watcher is still polling and will next see
	// InFlightStreams == 0 — on a sub-pool whose address is live again.
	releaseHeld()
	time.Sleep(600 * time.Millisecond)

	require.Lenf(t, mp.snapshotActive(), 1,
		"drain watcher tore down a revived address: active = %d, want 1", len(mp.snapshotActive()))
	// The sharp assertion. Liveness alone self-heals — a dropped sub-pool is
	// silently recreated on the next getOrCreateSubPool — so it cannot see the
	// damage. Identity can: the watcher must leave the revived sub-pool in
	// place, not close it and force a fresh dial and TLS handshake against an
	// address that never went away.
	after := mp.getOrCreateSubPool(addrs[0])
	require.NotNil(t, after, "drain watcher left the revived address unusable")
	require.Same(t, before, after,
		"drain watcher closed the revived sub-pool; the live address paid for a needless reconnect")
	c, release, err := mp.acquire(ctx)
	require.NoError(t, err, "acquire after revive")
	require.NotNil(t, c, "acquire after revive returned a nil conn")
	release()
}
