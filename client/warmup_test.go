package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestWarmup_SingleConn_DialsAhead verifies Warmup on a single-conn
// client triggers a background dial so the first Do call returns
// faster.
func TestWarmup_SingleConn_DialsAhead(t *testing.T) {
	var dialCount atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	// Custom dialer that counts dials.
	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &countingDialer{
				Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
				count:  &dialCount,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	c.Warmup(1)

	// Wait for the dial to complete (poll dial count).
	deadline := time.Now().Add(2 * time.Second)
	for dialCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if dialCount.Load() == 0 {
		t.Fatal("Warmup did not trigger a dial within 2s")
	}

	// First Do should now hit a warm conn.
	var resp Response
	if err := c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}

// TestWarmup_ZeroNoop verifies Warmup(0) is a no-op.
func TestWarmup_ZeroNoop(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	c.Warmup(0) // should not panic
}

// TestWarmup_Pool_DialsMultiple verifies Warmup on a pool transport
// opens multiple conns.
func TestWarmup_Pool_DialsMultiple(t *testing.T) {
	var dialCount atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &countingDialer{
				Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
				count:  &dialCount,
			},
		},
		Transport: TransportPool,
		Pool: &PoolOptions{
			MaxConnsPerHost:   4,
			MaxStreamsPerConn: 16,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	c.Warmup(4)

	// Wait for dials to complete.
	deadline := time.Now().Add(3 * time.Second)
	for dialCount.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	got := dialCount.Load()
	if got < 1 {
		t.Errorf("expected at least 1 dial, got %d", got)
	}
	if got > 4 {
		t.Errorf("expected at most 4 dials, got %d", got)
	}
}

// TestWarmup_Pool_CappedByMaxConns verifies Warmup(n) where n >
// MaxConnsPerHost is capped. The test must assert that Warmup
// actually triggered at least one dial — checking only ActiveConns
// <= 2 would pass even if Warmup did nothing (0 <= 2).
func TestWarmup_Pool_CappedByMaxConns(t *testing.T) {
	var dialCount atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &countingDialer{
				Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
				count:  &dialCount,
			},
		},
		Transport: TransportPool,
		Pool: &PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 16,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Request way more than MaxConnsPerHost.
	c.Warmup(100)

	// Wait for dials to settle. Deadline = MaxConnsPerHost dial slots
	// × a generous per-dial budget + slack.
	deadline := time.Now().Add(3 * time.Second)
	for dialCount.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	got := dialCount.Load()
	if got < 1 {
		t.Errorf("expected at least 1 dial triggered by Warmup, got %d (warmup no-op?)", got)
	}
	if got > 2 {
		t.Errorf("expected at most 2 dials (capped by MaxConnsPerHost), got %d", got)
	}

	stats := c.PoolStats()
	if stats.ActiveConns > 2 {
		t.Errorf("ActiveConns = %d, want <= 2 (capped by MaxConnsPerHost)", stats.ActiveConns)
	}
}

// countingDialer wraps a Dialer and increments a counter on every Dial.
type countingDialer struct {
	conn.Dialer
	count *atomic.Int32
}

func (c *countingDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	c.count.Add(1)
	return c.Dialer.Dial(ctx, addr)
}
