package conn

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestConn_Shutdown_NoInflightImmediate verifies that Shutdown on a
// conn with no in-flight streams closes the conn without waiting.
func TestConn_Shutdown_NoInflightImmediate(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}

	start := time.Now()
	if err := c.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 1*time.Second {
		t.Errorf("Shutdown took %v, expected < 1s with no inflight", elapsed)
	}
}

// TestConn_Shutdown_RejectsNewStreams verifies that after Shutdown
// is called, NewStream returns ErrConnDraining.
func TestConn_Shutdown_RejectsNewStreams(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	// Set draining without closing (simulate Shutdown in progress).
	c.draining.Store(true)

	if _, err := c.NewStream(context.Background()); err != ErrConnDraining {
		t.Errorf("NewStream after draining: err = %v, want ErrConnDraining", err)
	}
}

// TestConn_Shutdown_Idempotent verifies calling Shutdown twice is safe.
func TestConn_Shutdown_Idempotent(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}

	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Fatalf("Shutdown 1: %v", err)
	}
	// Second call should be a no-op.
	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Errorf("Shutdown 2: %v", err)
	}
}

// TestConn_Shutdown_WaitsForInflight verifies that Shutdown blocks
// until the in-flight count reaches zero (or the timeout fires).
func TestConn_Shutdown_WaitsForInflight(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}

	// Manually bump inflight to simulate an active stream.
	c.smu.Lock()
	c.inflight = 3
	c.draining.Store(true)
	c.smu.Unlock()

	// Shutdown should wait for the timeout (no one is draining).
	start := time.Now()
	_ = c.Shutdown(100 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 80*time.Millisecond {
		t.Errorf("Shutdown returned in %v, expected ~100ms wait", elapsed)
	}
	if elapsed > 1*time.Second {
		t.Errorf("Shutdown took %v, expected ~100ms", elapsed)
	}
}

// TestConn_Shutdown_WithInflightStream verifies the timer block in Shutdown
// (conn.go:325-330): when inflight > 0 at call time, Shutdown must create the
// timer, enter the select, and wait until either drainDone fires or the timer
// expires.
//
// Strategy: use a net.Pipe pair (no concurrent readerLoop modifying inflight).
// Set inflight = 1 synchronously BEFORE calling Shutdown, so the read of
// c.inflight at line 322 happens single-goroutine with no concurrent writer.
// Then let the 100ms graceful timer fire to exit the select, just like the
// existing TestConn_Shutdown_WaitsForInflight does for the other path — but
// this time draining starts as false so the CAS succeeds and we take the
// GOAWAY-sending branch first.
func TestConn_Shutdown_WithInflightStream(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}

	// Set inflight = 1 before calling Shutdown. Because the net.Pipe
	// pipeServer finishes the handshake and then returns (no concurrent
	// reader in flight at this point), no other goroutine writes to
	// c.inflight, so the read at conn.go:322 is race-free.
	c.smu.Lock()
	c.inflight = 1
	c.smu.Unlock()

	// Shutdown: draining is currently false, so CAS succeeds, GOAWAY is
	// written (best-effort, pipe peer is gone — error is ignored), then
	// inflight == 1 → falls into the timer/drainDone select (lines 325-330).
	// Nobody will close drainDone, so the 100ms timer fires.
	start := time.Now()
	_ = c.Shutdown(100 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 80*time.Millisecond {
		t.Errorf("Shutdown returned in %v, want ≥ 80ms (timer path)", elapsed)
	}
	if elapsed > 1*time.Second {
		t.Errorf("Shutdown took %v, want ≤ 1s", elapsed)
	}
}

// -- helpers (none currently needed; helpers were for h2 server tests
// that were simplified to in-process fakes).
