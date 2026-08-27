package client

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ————————————————————————————————————————————————————————————————
// h1 sweeps idle BEFORE dead, the reverse of its siblings.
//
// This is the divergence the base-tier survey flagged as most likely to be
// "fixed" by a unification, and nothing pinned it: swapping the two lines
// passed the whole client suite. The package doc for this pool used to assert
// its sweeps MATCHED the other two, while pool.go spent five lines explaining
// that they deliberately do not.
//
// The order buys two things.
//
// A conn about to be discarded for idleness is never probed. startHealthSweep
// reserves every checked-in conn and reads its socket; running it over a conn
// evictIdle is about to drop is pure waste, and before that probe moved off
// the actor it was waste measured in milliseconds of whole-pool stall.
//
// And a conn that is BOTH idle-expired and locally closed is attributed to
// idleness. h2 and h3 make the opposite choice for a reason their files give —
// a GOAWAY'd conn is usually also idle, and dead-first is what keeps a rolling
// restart from being reported as local inactivity. HTTP/1.1 has no GOAWAY, so
// there is no attribution to protect and the cheaper sweep goes first.
// ————————————————————————————————————————————————————————————————

func TestH1Pool_Tick_SweepsIdleBeforeDead(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	reasons := map[CloseReason]int{}
	hp := new(atomic.Pointer[Hooks])
	hp.Store(&Hooks{OnConnClose: func(e ConnCloseEvent) {
		mu.Lock()
		reasons[e.Reason]++
		mu.Unlock()
	}})
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   1,
		HealthCheckPeriod: 20 * time.Millisecond,
		// Short enough that the conn qualifies as idle by the time the tick
		// runs. The collision between "idle" and "dead" is the whole test.
		IdleTimeout: time.Millisecond,
	}, hp, nil)
	defer func() { _ = p.Close() }()
	mc, err := p.Acquire(context.Background())
	require.NoError(t, err, "acquire")
	p.release(mc, true)
	// Wait for the actor to record the release BEFORE killing the conn. Doing
	// it the other way round lets handleRelease's own IsAlive check evict the
	// conn as CloseDead, so the tick would never be the evictor and the test
	// would pass whatever order handleTick used.
	for p.Stats().InFlightStreams != 0 {
	}

	// Now make it idle-expired AND locally dead at the same instant. From here
	// nothing may call Stats: handleStats runs evictDeadSilent, which would
	// remove the conn before the tick gets to it.
	time.Sleep(5 * time.Millisecond)
	_ = mc.c.Close()

	var idle, dead int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		idle, dead = reasons[CloseIdle], reasons[CloseDead]
		mu.Unlock()
		if idle > 0 || dead > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	require.Zerof(t, dead,
		"a conn that was both idle-expired and closed was reported CloseDead (idle=%d dead=%d);\n"+
			"evictDead ran before evictIdle, so this pool now attributes like its siblings "+
			"and probes conns it is about to discard", idle, dead)
	require.Equalf(t, 1, idle,
		"no idle eviction observed within 3s (idle=%d dead=%d)", idle, dead)
}
