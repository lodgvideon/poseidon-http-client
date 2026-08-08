package client

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ————————————————————————————————————————————————————————————————
// A warmup must not defeat the backoff a failing peer earned.
//
// handleWarmup carries an inDialBackoff check with that comment against it,
// and deleting the check passed the entire client suite. Without it, warming a
// pool whose peer is down fans out MaxConnsPerHost dials at once — the exact
// stampede the backoff exists to prevent. warmup is also the entry point a
// caller reaches for right after a deploy, when the peer is most likely to
// still be coming up.
//
// The observable is Stats().InFlightDials, not DialsAttempted. handleWarmup
// increments inFlightDials on the actor goroutine BEFORE spawning `go
// p.dialOne()`, so it is settled by the time the Stats round-trip is served;
// DialsAttempted is bumped inside the spawned goroutine, so reading it right
// after the barrier races the scheduler. A first version of this test did read
// it, and mutation-checking showed the mutation went unnoticed.
// ————————————————————————————————————————————————————————————————

// blockingRefusingDialer fails every dial, and while blocking is set it parks
// in the dial until released. Parking is what makes inFlightDials stand still
// long enough to be read: a dial that fails instantly can be counted and
// uncounted between the actor spawning it and the test asking.
type blockingRefusingDialer struct {
	blocking atomic.Bool
	release  chan struct{}
}

func (d *blockingRefusingDialer) Dial(ctx context.Context, _ string) (net.Conn, error) {
	if d.blocking.Load() {
		select {
		case <-d.release:
		case <-ctx.Done():
		}
	}
	return nil, errors.New("blockingRefusingDialer: connection refused")
}

func TestPool_Warmup_DoesNotDefeatDialBackoff(t *testing.T) {
	t.Parallel()

	const backoff = 200 * time.Millisecond
	m := &Metrics{}
	d := &blockingRefusingDialer{release: make(chan struct{})}
	p := newPool("warmup.test:443", conn.ConnOptions{Dialer: d}, PoolOptions{
		MaxConnsPerHost: 4,
		DialBackoff:     backoff,
		DialTimeout:     5 * time.Second,
		// Far away: a tick dials for waiters and would muddy the counts.
		HealthCheckPeriod: time.Hour,
	}, nil, m)
	defer func() { _ = p.Close() }()

	// One failed dial is what arms the backoff. Let it fail immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.acquire(ctx); err == nil {
		t.Fatal("acquire against a refusing dialer succeeded")
	}
	if got := m.Counters.DialsAttempted.Load(); got != 1 {
		t.Fatalf("DialsAttempted after the arming acquire = %d, want 1", got)
	}

	// From here a dial parks, so anything handleWarmup starts stays visible.
	d.blocking.Store(true)

	// warmupCh is unbuffered, so warmup returns once the actor has taken the
	// message; the actor is single-threaded, so the Stats round-trip after it
	// cannot be served until handleWarmup has returned. A barrier, not a sleep.
	p.warmup(4)
	if s := p.Stats(); s.InFlightDials != 0 {
		t.Fatalf("warmup during dial backoff started %d dials, want 0;\n"+
			"a warmup right after a deploy would stampede a peer that is still coming up",
			s.InFlightDials)
	}

	// The converse, so this pins "warmup respects the backoff" rather than the
	// far weaker "warmup never dials": once the window closes it must fan out.
	close(d.release)
	d.blocking.Store(false)
	time.Sleep(backoff + 100*time.Millisecond)

	p.warmup(4)
	_ = p.Stats()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m.Counters.DialsAttempted.Load() > 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("warmup started no dial after the backoff expired; DialsAttempted still %d",
		m.Counters.DialsAttempted.Load())
}
