package client

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// These pin decisions inside the pools' periodic tick that no test detected.
// Each was found by mutation: deleting the guard or the call left the whole
// ./client suite green.
//
// They gate the #364 unification. A generic tick body has to either preserve
// each of these or drop it deliberately, and neither is possible while the
// suite cannot tell the difference.

// TestEvictIdle_KeepsConnWithActiveStreams pins evictIdle's `active == 0`
// guard on the HTTP/2 pool.
//
// Without it, a connection idle past IdleTimeout is closed even while it is
// carrying streams — the pool multiplexes, so lastUsed goes stale as soon as no
// NEW request picks the conn, regardless of what is still running on it. A
// long-lived response or a slow upload would be cut mid-flight.
//
// Deleting `mc.active == 0 &&` from pool.go left the suite green.
func TestEvictIdle_KeepsConnWithActiveStreams(t *testing.T) {
	// evictIdle closes what it evicts, so the idle conn needs a real *conn.Conn.
	cli, srv := net.Pipe()
	defer srv.Close()
	stopSrv := make(chan struct{})
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		runFakeH2Server(srv, func(*frame.Framer) { <-stopSrv })
	}()
	t.Cleanup(func() { close(stopSrv); <-srvDone })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := conn.NewClientConn(ctx, cli, conn.ConnOptions{})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	p := newPool("ignored:0", newConnOpts(), PoolOptions{
		MaxConnsPerHost: 4,
		IdleTimeout:     10 * time.Millisecond,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	stale := time.Now().Add(-time.Second) // well past IdleTimeout

	busy := &managedConn{c: c, active: 1, lastUsed: stale}
	idle := &managedConn{c: c, active: 0, lastUsed: stale}

	got := p.evictIdle([]*managedConn{busy, idle})

	if len(got) != 1 {
		t.Fatalf("evictIdle kept %d conns, want 1", len(got))
	}
	if got[0] != busy {
		t.Fatal("evictIdle evicted the conn that still had active streams and kept the idle one")
	}
}

// TestH3EvictIdle_KeepsConnWithActiveStreams is the HTTP/3 twin. Same guard,
// same surviving mutation, and the same consequence: QUIC streams on an
// otherwise-unpicked connection would be torn down.
func TestH3EvictIdle_KeepsConnWithActiveStreams(t *testing.T) {
	p := newH3Pool("ignored:0", &tls.Config{ServerName: "h"}, PoolOptions{
		MaxConnsPerHost: 4,
		IdleTimeout:     10 * time.Millisecond,
	}, newH3FakeDialer().dial, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	stale := time.Now().Add(-time.Second)

	busy := &h3ManagedConn{cl: &fakeH3Client{}, active: 1, lastUsed: stale, streamCap: 10}
	idle := &h3ManagedConn{cl: &fakeH3Client{}, active: 0, lastUsed: stale, streamCap: 10}

	got := p.evictIdle([]*h3ManagedConn{busy, idle})

	if len(got) != 1 {
		t.Fatalf("evictIdle kept %d conns, want 1", len(got))
	}
	if got[0] != busy {
		t.Fatal("evictIdle evicted the conn that still had active streams and kept the idle one")
	}
}

// TestHandleTick_DialsForStrandedWaiter pins ensureDialForWaiters inside the
// HTTP/2 tick. Deleting that call from Pool.handleTick left the suite green.
//
// The state it exists to escape is {no live conns, no in-flight dials, queued
// waiters}. pool.go records it as measured-terminal: release, dial-done and tick
// all pass through without acting, so nothing re-enters the dial decision and
// each waiter sits until its own context expires — for up to ~30 health-check
// ticks with the server still dialable. handleAcquire makes this decision for a
// NEW request; the point of the tick call is that the decision belongs to the
// state, not to the arrival of work, and a pool whose every worker is parked
// never receives that request.
//
// No test drove a tick into that state, so the call was free to delete.
func TestHandleTick_DialsForStrandedWaiter(t *testing.T) {
	p := newPool("127.0.0.1:1", newConnOpts(), PoolOptions{
		MaxConnsPerHost:   2,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := &runState{
		waiters: []acquireReq{{ctx: ctx, reply: make(chan acquireResp, 1)}},
	}

	p.handleTick(rs)

	if rs.inFlightDials != 1 {
		t.Fatalf("inFlightDials = %d after a tick with a stranded waiter, want 1 — "+
			"nothing re-enters the dial decision, so the waiter blocks until its ctx expires",
			rs.inFlightDials)
	}
}

// TestH1HandleTick_DialsForStrandedWaiter is the HTTP/1.1 twin. Same call, same
// surviving mutation, and h1_pool.go is where this rescue was written first.
func TestH1HandleTick_DialsForStrandedWaiter(t *testing.T) {
	p := newH1Pool("127.0.0.1:1", newH1FakeDialer(), PoolOptions{
		MaxConnsPerHost:   2,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := &h1RunState{
		waiters: []h1AcquireReq{{ctx: ctx, reply: make(chan h1AcquireResp, 1)}},
	}

	p.handleTick(rs)

	if rs.inFlightDials != 1 {
		t.Fatalf("inFlightDials = %d after a tick with a stranded waiter, want 1", rs.inFlightDials)
	}
}
