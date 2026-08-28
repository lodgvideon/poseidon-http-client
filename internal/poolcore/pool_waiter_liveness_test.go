package poolcore

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPool_WaiterRescuedAfterLastConnEvicted pins the liveness property the H2
// pool was missing: a request already parked on the queue must be rescued by
// the pool itself, not by the arrival of an unrelated request.
//
// serveWaiters can only hand out capacity that exists, and eviction routinely
// removes the last conn. Before this, {no live conns, no in-flight dials,
// queued waiters} was terminal — release, dial-done and tick all passed through
// without re-entering the dial decision, which lived only in handleAcquire.
// Measured on the default options, that state survived ~30 health-check ticks
// against a server that was still dialable, and the waiter left only when
// another request happened to arrive. A pool whose every worker is parked never
// gets that request, so the worst case is a deadlock rather than a slow path.
//
// The fault is injected by hand (closing the conn out of band), so the test
// asserts the injection actually landed — IsAlive true before, false after.
// Without that, a run where Close silently no-ops passes exactly like a real
// rescue, because the waiter would then be served from the still-live conn.
func TestPool_WaiterRescuedAfterLastConnEvicted(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()
	p := New(addrs[0].String(), newConnOpts(), PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 1,
		// No tick: the rescue must come from the release path, not from
		// maintenance. A tick-only fix would still hang a pool configured
		// with a long HealthCheckPeriod.
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	held, err := p.Acquire(ctx)
	require.NoError(t, err, "first acquire against a live H2 server")
	// A second acquire parks: the one conn is at its one-stream cap.
	parked := make(chan error, 1)
	go func() {
		mc, aerr := p.Acquire(ctx)
		if aerr == nil {
			p.Release(mc)
		}
		parked <- aerr
	}()
	s := waitStats(p, func(s Stats) bool { return s.Waiters == 1 }, 5*time.Second)
	require.Equalf(t, 1, s.Waiters, "waiter never parked: %+v", s)
	require.True(t, held.C.IsAlive(),
		"the conn was already dead before the fault was injected, so the rescue "+
			"below would not be measuring the eviction path")

	// Kill the connection out of band, then release the held stream. The
	// release evicts the now-dead conn and the pool is left with a waiter and
	// nothing to serve it from.
	_ = held.C.Close()
	require.False(t, held.C.IsAlive(),
		"injection did not fire: the conn survived Close, so a pass here would only "+
			"show the waiter being served from the still-live conn")
	p.Release(held)

	select {
	case aerr := <-parked:
		assert.NoErrorf(t, aerr, "parked waiter = %v, want a rescued connection", aerr)
	case <-time.After(10 * time.Second):
		st := waitStats(p, func(Stats) bool { return true }, time.Second)
		require.Failf(t, "parked waiter never rescued",
			"pool sat at %+v with a dialable server", st)
	}
}

// refusingDialer fails every dial, which is what a downed host looks like. dials
// counts what it was actually asked to do, so a test can prove the injection
// fired rather than inferring it from an error that some other path produced.
type refusingDialer struct {
	err   error
	dials atomic.Int32
}

func (d *refusingDialer) Dial(context.Context, string) (net.Conn, error) {
	d.dials.Add(1)
	return nil, d.err
}

// TestPool_DialFailureRefusesEveryQueuedWaiter pins the second half: when a dial
// fails and the pool is in exactly the state that makes a NEW request
// fast-refuse — dial backoff, nothing live, nothing in flight — the requests
// already queued must be refused for the same reason.
//
// Failing only the first left the rest queued, which is a priority inversion:
// measured, a fresh acquire was refused with ErrDialBackoff in 0ms while two
// earlier waiters stayed parked past the end of the backoff window and left
// only when the pool closed. H1 drains one per health tick; H2 had no
// tick-path dial at all, so it drained none.
func TestPool_DialFailureRefusesEveryQueuedWaiter(t *testing.T) {
	sentinel := errors.New("connect refused (synthetic)")
	d := &refusingDialer{err: sentinel}
	p := New("203.0.113.1:1", conn.ConnOptions{Dialer: d}, PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 4,
		HealthCheckPeriod: time.Hour, // no tick: the refusal must come from dial-done
		DialBackoff:       time.Hour, // and the pool must stay in backoff afterwards
	}, nil, nil)
	defer func() { _ = p.Close() }()
	const waiters = 3
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	errs := make(chan error, waiters)
	var wg sync.WaitGroup

	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mc, aerr := p.Acquire(ctx)
			if aerr == nil {
				p.Release(mc)
			}
			errs <- aerr
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s := waitStats(p, func(Stats) bool { return true }, time.Second)
		require.Failf(t, "queued waiters were not refused", "pool sat at %+v", s)
	}
	close(errs)
	n := 0
	for aerr := range errs {
		n++
		require.Errorf(t, aerr, "an acquire succeeded against a dialer that refuses everything")
		assert.Truef(t, errors.Is(aerr, sentinel) || errors.Is(aerr, ErrDialBackoff),
			"acquire = %v, want the dial error or ErrDialBackoff — any other error means "+
				"the waiter was refused for a reason this test is not pinning", aerr)
	}
	assert.Equalf(t, waiters, n, "%d acquires returned, want %d", n, waiters)
	assert.Positivef(t, d.dials.Load(),
		"the refusing dialer was never called (%d dials), so the refusals came from "+
			"somewhere other than the dial failure this test injects", d.dials.Load())
}

// TestPool_WaiterRescueRespectsMaxConns is the bound on the rescue. Waiters are
// a reason to dial only while the pool is under its connection limit; at the
// limit they are waiting for a stream slot, which a release will hand them.
// Without this check the rescue path would open a connection on every release
// that leaves anyone queued — turning a bounded pool into an unbounded one
// under exactly the load that fills it.
func TestPool_WaiterRescueRespectsMaxConns(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()
	p := New(addrs[0].String(), newConnOpts(), PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 2,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	held := make([]*ManagedConn, 0, 2)
	for i := 0; i < 2; i++ {
		mc, err := p.Acquire(ctx)
		require.NoErrorf(t, err, "acquire %d", i)
		held = append(held, mc)
	}
	// Two more park: the single conn is at its stream cap.
	parked := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			mc, aerr := p.Acquire(ctx)
			if aerr == nil {
				p.Release(mc)
			}
			parked <- aerr
		}()
	}
	s := waitStats(p, func(s Stats) bool { return s.Waiters == 2 }, 5*time.Second)
	require.Equalf(t, 2, s.Waiters, "waiters never parked: %+v", s)

	// A release that frees a stream but evicts nothing: one waiter is served
	// from the existing conn, and the pool must not have dialled for the other.
	p.Release(held[0])

	select {
	case aerr := <-parked:
		require.NoErrorf(t, aerr, "waiter served by the freed slot = %v", aerr)
	case <-time.After(10 * time.Second):
		require.Fail(t, "freeing a stream did not serve a waiter")
	}
	after := waitStats(p, func(s Stats) bool { return s.InFlightDials == 0 }, 2*time.Second)
	assert.LessOrEqualf(t, after.ActiveConns, 1,
		"pool holds %d conns with MaxConnsPerHost=1: the rescue dial ignored the limit (%+v)",
		after.ActiveConns, after)
	p.Release(held[1])
	<-parked
}
