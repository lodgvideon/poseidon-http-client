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

// -- helpers (none currently needed; helpers were for h2 server tests
// that were simplified to in-process fakes).
