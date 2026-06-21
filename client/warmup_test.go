package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
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

	// Wait for the dial to complete. Budget = expectedDials ×
	// per-dial timeout + slack. 1 dial × 2s + 1s slack.
	deadline := time.Now().Add(3 * time.Second)
	for dialCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if dialCount.Load() == 0 {
		t.Fatal("Warmup did not trigger a dial within 3s")
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

	const (
		maxConns      = 4
		dialPerBudget = 1 * time.Second
		slack         = 1 * time.Second
	)

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
			MaxConnsPerHost:   maxConns,
			MaxStreamsPerConn: 16,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	c.Warmup(maxConns)

	// Wait for dials to complete. Budget = maxConns × dialPerBudget
	// + slack.
	deadline := time.Now().Add(time.Duration(maxConns)*dialPerBudget + slack)
	for dialCount.Load() < maxConns && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	got := dialCount.Load()
	if got < 1 {
		t.Errorf("expected at least 1 dial, got %d", got)
	}
	if got > maxConns {
		t.Errorf("expected at most %d dials, got %d", maxConns, got)
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

	const (
		maxConns      = 2
		dialPerBudget = 1 * time.Second
		slack         = 1 * time.Second
	)

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
			MaxConnsPerHost:   maxConns,
			MaxStreamsPerConn: 16,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Request way more than MaxConnsPerHost.
	c.Warmup(100)

	// Wait for dials to settle. Budget = maxConns × dialPerBudget
	// + slack. We poll until the cap is hit or the budget expires.
	deadline := time.Now().Add(time.Duration(maxConns)*dialPerBudget + slack)
	for dialCount.Load() < maxConns && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	got := dialCount.Load()
	if got < 1 {
		t.Errorf("expected at least 1 dial triggered by Warmup, got %d (warmup no-op?)", got)
	}
	if got > maxConns {
		t.Errorf("expected at most %d dials (capped by MaxConnsPerHost), got %d", maxConns, got)
	}

	stats := c.PoolStats()
	if stats.ActiveConns > maxConns {
		t.Errorf("ActiveConns = %d, want <= %d (capped by MaxConnsPerHost)", stats.ActiveConns, maxConns)
	}
}

// TestSingleConn_Warmup_WhenClosed verifies that calling Warmup on a closed
// singleConn transport is a no-op (covers the `if s.closed { return }` path).
func TestSingleConn_Warmup_WhenClosed(t *testing.T) {
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

	// Close the client first — this sets s.closed = true on the underlying
	// singleConn transport.
	_ = c.Close()

	// Warmup on a closed transport must be a no-op and must not panic.
	c.Warmup(1)
}

// TestSingleConn_Warmup_AlreadyInFlight verifies that a second Warmup call
// while a background dial is still in progress is a no-op
// (covers the `if s.warmupCancel != nil { return }` path).
func TestSingleConn_Warmup_AlreadyInFlight(t *testing.T) {
	// Use a slow dialer so the background warmup goroutine is still in flight
	// when we call Warmup a second time.
	var dialStarted = make(chan struct{})
	slow := &slowDialerWarmup{
		inner:   &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		started: dialStarted,
		delay:   300 * time.Millisecond,
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: slow,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// First Warmup: starts a background dial that will block for 300ms.
	c.Warmup(1)

	// Wait until the dial goroutine has actually started (i.e. warmupCancel != nil).
	select {
	case <-dialStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("background warmup dial did not start within 2s")
	}

	// Second Warmup: warmupCancel is non-nil → hits the `if s.warmupCancel != nil` path.
	c.Warmup(1) // must not panic or block
}

// slowDialerWarmup wraps a Dialer adding a delay; it signals the started
// channel when the dial begins so the test can synchronise.
type slowDialerWarmup struct {
	inner   conn.Dialer
	started chan struct{}
	delay   time.Duration
	once    sync.Once
}

func (d *slowDialerWarmup) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d.once.Do(func() { close(d.started) })
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return d.inner.Dial(ctx, addr)
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
