package client

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
//
// The two tests below are each other's control. The refusal arm asserts zero
// dials; on its own, a fixture that had simply stopped dialling for any reason
// would satisfy it. The expiry arm runs the same fixture with the only
// difference being that the backoff window has closed, and requires dials to
// appear — so the zero is a refusal, not a dead fixture. Both print the counts.
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

const warmupBackoffWindow = 200 * time.Millisecond

// armBackoffPool builds a pool over a refusing dialer and spends one acquire on
// it, which is what arms the dial backoff. It returns with the dialer switched
// to blocking, so anything handleWarmup starts afterwards stays visible in
// InFlightDials. The returned count is the number of dials the arming step
// actually performed — one, or the backoff was never armed and neither test
// below is measuring what it claims.
func armBackoffPool(t *testing.T) (*Pool, *blockingRefusingDialer, *Metrics) {
	t.Helper()

	m := &Metrics{}
	d := &blockingRefusingDialer{release: make(chan struct{})}
	p := newPool("warmup.test:443", conn.ConnOptions{Dialer: d}, PoolOptions{
		MaxConnsPerHost: 4,
		DialBackoff:     warmupBackoffWindow,
		DialTimeout:     5 * time.Second,
		// Far away: a tick dials for waiters and would muddy the counts.
		HealthCheckPeriod: time.Hour,
	}, nil, m)
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.acquire(ctx)
	require.Error(t, err, "acquire against a refusing dialer succeeded")
	require.Equalf(t, int64(1), m.Counters.DialsAttempted.Load(),
		"DialsAttempted after the arming acquire = %d, want 1 — without exactly one "+
			"failed dial the backoff window is not open and neither arm measures it",
		m.Counters.DialsAttempted.Load())

	d.blocking.Store(true)
	return p, d, m
}

// TestPool_Warmup_DoesNotDefeatDialBackoff is the refusal arm: inside the
// window, warmup must start nothing.
func TestPool_Warmup_DoesNotDefeatDialBackoff(t *testing.T) {
	p, _, m := armBackoffPool(t)
	armed := m.Counters.DialsAttempted.Load()

	// warmupCh is unbuffered, so warmup returns once the actor has taken the
	// message; the actor is single-threaded, so the Stats round-trip after it
	// cannot be served until handleWarmup has returned. A barrier, not a sleep.
	p.warmup(4)
	s := p.Stats()

	t.Logf("injections: arming dials=%d, warmup requested=4, InFlightDials after warmup=%d",
		armed, s.InFlightDials)
	assert.Zerof(t, s.InFlightDials,
		"warmup during dial backoff started %d dials, want 0;\n"+
			"a warmup right after a deploy would stampede a peer that is still coming up",
		s.InFlightDials)
}

// TestPool_Warmup_DialsOnceBackoffExpires is the converse, and the control for
// the arm above: once the window closes the same warmup on the same fixture must
// fan out. Without it, "warmup respects the backoff" is indistinguishable from
// the far weaker "warmup never dials".
func TestPool_Warmup_DialsOnceBackoffExpires(t *testing.T) {
	p, d, m := armBackoffPool(t)
	armed := m.Counters.DialsAttempted.Load()
	close(d.release)
	d.blocking.Store(false)
	time.Sleep(warmupBackoffWindow + 100*time.Millisecond)

	p.warmup(4)
	_ = p.Stats() // barrier: handleWarmup has run by the time this returns

	var got int64
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got = m.Counters.DialsAttempted.Load(); got > armed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("injections: arming dials=%d, warmup requested=4, DialsAttempted after expiry=%d",
		armed, got)
	assert.Greaterf(t, got, armed,
		"warmup started no dial after the backoff expired; DialsAttempted still %d — "+
			"the refusal arm's zero would then prove nothing", got)
}
