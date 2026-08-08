package client

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ————————————————————————————————————————————————————————————————
// A waiter queued against a reservation must not outlive it.
//
// startHealthSweep lets handleAcquire queue a caller with NO dial behind it, on
// the promise that handleSweepDone will serve it from a reserved conn. That is a
// new kind of waiter — every other queued caller either has a dial in flight or
// sits behind a busy conn — and the promise has to be kept even when the
// reservation turns out to be worthless.
//
// The case that breaks it: the reserved conns come back DEAD while the pool is
// in dial backoff. handleDialDone's terminal-state flush cannot cover it,
// because at the moment the dial fails those conns are still reserved and
// h1CountLive counts them as live, so its "nothing live" test is false. By the
// time they are evicted the dial is long finished, and only handleSweepDone is
// left to notice.
//
// Without a flush there the waiter sits until the next health tick — a full
// HealthCheckPeriod — while a FRESH acquire arriving an instant later gets an
// immediate ErrDialBackoff from handleAcquire's fast-refuse. That is the same
// priority inversion handleDialDone's own comment says was fixed for the
// dial-error path.
// ————————————————————————————————————————————————————————————————

// h1GatedConn wraps a real loopback TCP conn so the test, not the clock, decides
// when a probe finishes.
//
// Wrapping a REAL socket matters. HasResidue runs on every checkout and only
// skips reading when it can read the socket's receive queue via FIONREAD; for a
// transport that is not a syscall.Conn it falls back to a future-deadline Peek
// instead. Forwarding SyscallConn keeps that fast path, so the only read this
// conn ever sees is the health sweep's ProbeIdle.
type h1GatedConn struct {
	net.Conn
	gate    chan struct{} // closed by the test to let the probe finish
	entered chan struct{} // closed by the conn when the probe first reads
	once    sync.Once
}

func (c *h1GatedConn) Read(_ []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.gate
	return 0, io.EOF // a peer that closed: ProbeIdle calls this dead
}

// SetReadDeadline is deliberately inert so the gate alone decides when the probe
// returns. ProbeIdle's own bound is exactly what this test needs to suspend.
func (c *h1GatedConn) SetReadDeadline(_ time.Time) error { return nil }

// SyscallConn keeps HasResidue on its FIONREAD path — see the type comment.
func (c *h1GatedConn) SyscallConn() (syscall.RawConn, error) {
	sc, ok := c.Conn.(syscall.Conn)
	if !ok {
		return nil, errors.New("underlying conn is not a syscall.Conn")
	}
	return sc.SyscallConn()
}

// h1GatedDialer serves one gated conn and then fails every later dial, so the
// pool enters dial backoff while its only conn is still reserved by the sweep.
type h1GatedDialer struct {
	mu       sync.Mutex
	addr     string
	fail     bool
	held     []net.Conn
	gate     chan struct{}
	entered  chan struct{}
	dialFail atomic.Int32
}

func (d *h1GatedDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	d.mu.Lock()
	fail := d.fail
	d.mu.Unlock()
	if fail {
		d.dialFail.Add(1)
		return nil, errors.New("connection refused")
	}
	nc, err := net.Dial("tcp", d.addr)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.held = append(d.held, nc)
	d.mu.Unlock()
	return &h1GatedConn{Conn: nc, gate: d.gate, entered: d.entered}, nil
}

func (d *h1GatedDialer) failFromNowOn() {
	d.mu.Lock()
	d.fail = true
	d.mu.Unlock()
}

// TestH1Pool_DeadReservation_DoesNotStrandWaiter pins that a waiter queued
// against a reservation is answered when that reservation dies, rather than
// being left for the next health tick.
func TestH1Pool_DeadReservation_DoesNotStrandWaiter(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	// Accept and hold, so the sockets stay open and healthy. Nothing is ever
	// written: the gated Read, not the peer, decides what the probe sees.
	var srvMu sync.Mutex
	var srv []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			srvMu.Lock()
			srv = append(srv, c)
			srvMu.Unlock()
		}
	}()
	defer func() {
		srvMu.Lock()
		defer srvMu.Unlock()
		for _, c := range srv {
			_ = c.Close()
		}
	}()

	d := &h1GatedDialer{
		addr:    ln.Addr().String(),
		gate:    make(chan struct{}),
		entered: make(chan struct{}),
	}
	hp := new(atomic.Pointer[Hooks])
	hp.Store(&Hooks{})

	// The period is long on purpose: it is the interval the bug makes the waiter
	// wait, so it has to be far larger than the bound asserted below.
	const healthPeriod = 2 * time.Second
	p := newH1Pool("strand.test:80", d, PoolOptions{
		MaxConnsPerHost:   4,
		HealthCheckPeriod: healthPeriod,
	}, hp, nil)
	defer func() { _ = p.Close() }()

	ctx := context.Background()

	// One pooled conn, checked in, so the first sweep has something to reserve.
	mc, err := p.acquire(ctx)
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	p.release(mc, true)
	for p.Stats().InFlightStreams != 0 {
	}

	// Every later dial fails, so the pool is in backoff by the time the sweep
	// reports back.
	d.failFromNowOn()

	select {
	case <-d.entered:
	case <-time.After(healthPeriod + 5*time.Second):
		t.Fatal("the health sweep never probed the idle conn")
	}

	// A is queued against the reservation and gets NO dial of its own.
	aDone := make(chan error, 1)
	go func() {
		_, err := p.acquire(ctx)
		aDone <- err
	}()

	// B is beyond what the single reservation can cover, so it dials — and that
	// failing dial is what puts the pool into backoff.
	bDone := make(chan error, 1)
	go func() {
		_, err := p.acquire(ctx)
		bDone <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for d.dialFail.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if d.dialFail.Load() == 0 {
		t.Fatal("the second acquire never triggered a dial")
	}

	// The probe now finds the reserved conn dead. handleSweepDone evicts it,
	// leaving no live conn, no dial in flight, and a waiter with nothing left to
	// wait for.
	close(d.gate)

	// Far below healthPeriod: with the flush missing, A waits for the next tick.
	const bound = 500 * time.Millisecond
	for i, ch := range []chan error{aDone, bDone} {
		select {
		case err := <-ch:
			if err == nil {
				t.Fatalf("acquire[%d] returned a conn from a pool with no live conns", i)
			}
		case <-time.After(bound):
			t.Fatalf("acquire[%d] still queued %v after its reservation was evicted;\n"+
				"handleSweepDone left it for the next health tick (%v away) while a fresh "+
				"acquire would have been refused immediately", i, bound, healthPeriod)
		}
	}
}
