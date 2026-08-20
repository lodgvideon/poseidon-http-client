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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCountingWarmupClient starts a 200-answering h2 server and returns a client
// whose dials are counted, so every warmup test can assert on the number of
// dials actually performed rather than on the absence of a panic.
func newCountingWarmupClient(t *testing.T, opts ClientOptions) (*Client, *atomic.Int32) {
	t.Helper()

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	var dialCount atomic.Int32
	opts.Addr = srv.Listener.Addr().String()
	opts.ConnOpts = conn.ConnOptions{
		Dialer: &countingDialer{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
			count:  &dialCount,
		},
	}
	c, err := NewClient(opts)
	require.NoError(t, err, "NewClient against the local h2 server")
	return c, &dialCount
}

// waitDials polls until the counter reaches want or the deadline expires, and
// returns what it reached.
func waitDials(count *atomic.Int32, want int32, d time.Duration) int32 {
	deadline := time.Now().Add(d)
	for count.Load() < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return count.Load()
}

// TestWarmup_SingleConn_DialsAhead verifies Warmup on a single-conn client
// triggers a background dial so the first Do call returns faster.
func TestWarmup_SingleConn_DialsAhead(t *testing.T) {
	c, dialCount := newCountingWarmupClient(t, ClientOptions{})
	defer c.Close()

	c.Warmup(1)

	// Budget = expectedDials × per-dial timeout + slack: 1 dial × 2s + 1s.
	got := waitDials(dialCount, 1, 3*time.Second)
	require.NotZerof(t, got,
		"Warmup did not trigger a dial within 3s — the point of a warmup is that the "+
			"connection is already open when the first request arrives")
	var resp Response
	require.NoError(t, c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp),
		"the first Do after a warmup must hit the warm conn")
	assert.Equalf(t, 200, resp.Status, "status = %d, want 200", resp.Status)
}

// TestWarmup_ZeroNoop verifies Warmup(0) is a no-op.
//
// This used to call c.Warmup(0) and assert nothing at all, so it could only fail
// by panicking: a Warmup(0) that opened the full MaxConnsPerHost would have
// passed it. The dial counter is the observable that makes "no-op" mean
// something, and TestWarmup_SingleConn_DialsAhead above is the control that
// proves the same fixture does count dials when one is asked for.
func TestWarmup_ZeroNoop(t *testing.T) {
	c, dialCount := newCountingWarmupClient(t, ClientOptions{})
	defer c.Close()

	c.Warmup(0)
	time.Sleep(300 * time.Millisecond) // give any (wrongly) started dial time to land

	assert.Zerof(t, dialCount.Load(),
		"Warmup(0) started %d dials, want 0 — a zero count must mean 'open nothing', or a "+
			"caller passing a computed zero silently opens connections it did not ask for",
		dialCount.Load())
}

// TestWarmup_Pool_DialsMultiple verifies Warmup on a pool transport opens
// multiple conns, bounded by MaxConnsPerHost.
func TestWarmup_Pool_DialsMultiple(t *testing.T) {
	const (
		maxConns      = 4
		dialPerBudget = 1 * time.Second
		slack         = 1 * time.Second
	)
	c, dialCount := newCountingWarmupClient(t, ClientOptions{
		Transport: TransportPool,
		Pool:      &PoolOptions{MaxConnsPerHost: maxConns, MaxStreamsPerConn: 16},
	})
	defer c.Close()

	c.Warmup(maxConns)

	got := waitDials(dialCount, maxConns, time.Duration(maxConns)*dialPerBudget+slack)
	t.Logf("injections: Warmup(%d) against MaxConnsPerHost=%d produced %d dials",
		maxConns, maxConns, got)
	assert.GreaterOrEqualf(t, got, int32(1),
		"expected at least 1 dial, got %d — the warmup did nothing at all", got)
	assert.LessOrEqualf(t, got, int32(maxConns),
		"expected at most %d dials, got %d", maxConns, got)
}

// TestWarmup_Pool_CappedByMaxConns verifies Warmup(n) where n > MaxConnsPerHost
// is capped. The test must assert that Warmup actually triggered at least one
// dial — checking only ActiveConns <= 2 would pass even if Warmup did nothing
// (0 <= 2).
func TestWarmup_Pool_CappedByMaxConns(t *testing.T) {
	const (
		maxConns      = 2
		dialPerBudget = 1 * time.Second
		slack         = 1 * time.Second
	)
	c, dialCount := newCountingWarmupClient(t, ClientOptions{
		Transport: TransportPool,
		Pool:      &PoolOptions{MaxConnsPerHost: maxConns, MaxStreamsPerConn: 16},
	})
	defer c.Close()

	// Request way more than MaxConnsPerHost.
	c.Warmup(100)

	got := waitDials(dialCount, maxConns, time.Duration(maxConns)*dialPerBudget+slack)
	stats := c.PoolStats()
	t.Logf("injections: Warmup(100) against MaxConnsPerHost=%d produced %d dials, ActiveConns=%d",
		maxConns, got, stats.ActiveConns)
	assert.GreaterOrEqualf(t, got, int32(1),
		"expected at least 1 dial triggered by Warmup, got %d (warmup no-op?)", got)
	assert.LessOrEqualf(t, got, int32(maxConns),
		"expected at most %d dials (capped by MaxConnsPerHost), got %d — an uncapped warmup "+
			"opens 100 connections against a pool configured for %d", maxConns, got, maxConns)
	assert.LessOrEqualf(t, stats.ActiveConns, maxConns,
		"ActiveConns = %d, want <= %d (capped by MaxConnsPerHost)", stats.ActiveConns, maxConns)
}

// TestSingleConn_Warmup_WhenClosed verifies that calling Warmup on a closed
// singleConn transport is a no-op (covers the `if s.closed { return }` path).
//
// Like TestWarmup_ZeroNoop this asserted nothing, so a closed transport that
// re-dialled would have passed. The counter is what makes the no-op observable,
// and it also pins the reason the guard exists: a dial started after Close has
// no owner to close it again.
func TestSingleConn_Warmup_WhenClosed(t *testing.T) {
	c, dialCount := newCountingWarmupClient(t, ClientOptions{})
	// Close the client first — this sets s.closed = true on the underlying
	// singleConn transport.
	require.NoError(t, c.Close(), "closing the client before the warmup")
	before := dialCount.Load()

	c.Warmup(1)
	time.Sleep(300 * time.Millisecond) // give any (wrongly) started dial time to land

	assert.Equalf(t, before, dialCount.Load(),
		"Warmup on a CLOSED transport started %d further dial(s); that connection has no "+
			"owner left to close it", dialCount.Load()-before)
}

// TestSingleConn_Warmup_AlreadyInFlight verifies that a second Warmup call while
// a background dial is still in progress is a no-op (covers the
// `if s.warmupCancel != nil { return }` path).
func TestSingleConn_Warmup_AlreadyInFlight(t *testing.T) {
	// Use a slow dialer so the background warmup goroutine is still in flight
	// when we call Warmup a second time.
	dialStarted := make(chan struct{})
	var dialCount atomic.Int32
	slow := &slowDialerWarmup{
		inner:   &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		started: dialStarted,
		delay:   300 * time.Millisecond,
		count:   &dialCount,
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	c, err := NewClient(ClientOptions{
		Addr:     srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{Dialer: slow},
	})
	require.NoError(t, err, "NewClient with the slow dialer")
	defer c.Close()

	// First Warmup: starts a background dial that will block for 300ms.
	c.Warmup(1)
	// Wait until the dial goroutine has actually started (warmupCancel != nil).
	select {
	case <-dialStarted:
	case <-time.After(2 * time.Second):
		require.Fail(t, "background warmup dial did not start within 2s",
			"the second Warmup below would then be testing the idle path, not the in-flight one")
	}
	// Second Warmup: warmupCancel is non-nil → hits the guard.
	c.Warmup(1)
	time.Sleep(100 * time.Millisecond)

	assert.Equalf(t, int32(1), dialCount.Load(),
		"a second Warmup while one was still in flight started %d dials in total, want 1 — "+
			"the latch exists so two warmups cannot race a pair of dials onto one transport",
		dialCount.Load())
}

// slowDialerWarmup wraps a Dialer adding a delay; it signals the started
// channel when the dial begins so the test can synchronise, and counts dials.
type slowDialerWarmup struct {
	inner   conn.Dialer
	started chan struct{}
	delay   time.Duration
	count   *atomic.Int32
	once    sync.Once
}

func (d *slowDialerWarmup) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d.count.Add(1)
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
