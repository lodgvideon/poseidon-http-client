package client

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRateLimiter_AllowsBurst verifies the initial burst is honored, and that
// the request one past it is not. Those are the two sides of the same boundary:
// a limiter that always says yes satisfies the first half alone.
func TestRateLimiter_AllowsBurst(t *testing.T) {
	const burst = 5
	rl := newRateLimiter(10, burst) // 10 rps

	var errs []error
	for i := 0; i < burst; i++ {
		errs = append(errs, rl.Take(context.Background()))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	past := rl.Take(ctx)

	for i, err := range errs {
		assert.NoErrorf(t, err, "Take %d of the initial burst of %d failed: %v", i, burst, err)
	}
	assert.Errorf(t, past,
		"Take %d succeeded: the bucket handed out more than its configured burst of %d "+
			"back to back, so the limit is not a limit", burst+1, burst)
}

// TestRateLimiter_RefillsOverTime verifies tokens replenish at rps rate.
func TestRateLimiter_RefillsOverTime(t *testing.T) {
	rl := newRateLimiter(100, 1) // 100 rps, burst 1 → one token per 10ms
	require.NoError(t, rl.Take(context.Background()), "draining the single initial token")

	time.Sleep(15 * time.Millisecond)
	err := rl.Take(context.Background())

	assert.NoErrorf(t, err,
		"Take after 15ms at 100 rps (one token per 10ms) failed: %v — the bucket never refills", err)
}

// TestRateLimiter_ContextCancel verifies Take respects ctx cancellation, and
// that it does so by waiting rather than by refusing immediately: a limiter that
// returned ctx.Err() without ever blocking would satisfy the error check alone.
func TestRateLimiter_ContextCancel(t *testing.T) {
	rl := newRateLimiter(1, 1) // 1 rps, burst 1 → the next token is a second away
	require.NoError(t, rl.Take(context.Background()), "draining the single initial token")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := rl.Take(ctx)
	elapsed := time.Since(start)

	t.Logf("timings: deadline=30ms, Take returned after %v", elapsed)
	assert.Errorf(t, err, "Take with an expiring context returned nil; the caller was let through unlimited")
	assert.GreaterOrEqualf(t, elapsed, 25*time.Millisecond,
		"Take returned after %v, well inside its 30ms deadline — it refused rather than "+
			"waited, so cancellation is not what ended it", elapsed)
}

// TestRateLimiter_RefillStopsAtBurst pins refillLocked's ceiling, which no other
// test reaches. burst does two jobs: newRateLimiter uses it as the number of
// tokens the bucket starts with, and refillLocked uses it as the cap it clamps
// to. Every other test in this file exercises the first job only, so raising the
// cap alone — leaving the initial fill untouched — passed the whole suite.
//
// The two jobs come apart after an idle period. If the cap exceeded the
// configured burst, a limiter that had been quiet would hand out more
// back-to-back requests than it was configured for, which is the shape a load
// test never produces because it never lets the bucket sit idle.
//
// Timings are derived rather than tuned: at 5 rps a token is 200ms, so the 800ms
// idle is 4 tokens against a burst of 3 — enough for the clamp to bite — and the
// 60ms deadline on the last Take is well inside one token interval, so it can
// only succeed if a fourth token was really there.
func TestRateLimiter_RefillStopsAtBurst(t *testing.T) {
	const (
		rps           = 5.0
		burst         = 3
		tokenInterval = time.Second / rps
		idle          = 4 * tokenInterval // enough refill to overshoot the cap
	)
	rl := newRateLimiter(rps, burst)
	for i := 0; i < burst; i++ {
		require.NoErrorf(t, rl.Take(context.Background()),
			"draining the initial burst, Take %d", i+1)
	}
	time.Sleep(idle)

	// The bucket has been idle long enough to refill past burst. Exactly burst
	// tokens must be available, not the 4 that elapsed time alone would produce.
	start := time.Now()
	for i := 0; i < burst; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		err := rl.Take(ctx)
		cancel()
		require.NoErrorf(t, err, "after %v idle, Take %d/%d failed: %v — the bucket did not "+
			"refill to its full burst", idle, i+1, burst, err)
	}
	drain := time.Since(start)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	past := rl.Take(ctx)

	t.Logf("timings: idle=%v (%d token intervals of %v), refilled burst drained in %v",
		idle, int(idle/tokenInterval), tokenInterval, drain)
	require.LessOrEqualf(t, drain, tokenInterval/2,
		"draining the refilled burst took %v, more than half a token interval (%v) — the "+
			"process stalled, so the next assertion cannot distinguish a capped bucket "+
			"from a freshly refilled one", drain, tokenInterval)
	assert.Errorf(t, past,
		"a %d+1st token was available after %v idle: refillLocked let the bucket grow "+
			"past its burst of %d", burst, idle, burst)
}

// newRateLimitedH2Client starts an h2 server that counts requests and returns a
// client aimed at it. rps <= 0 configures no limiter at all, which is the control
// arm for the blocking test below.
func newRateLimitedH2Client(t *testing.T, rps, burst float64) (*Client, *atomic.Int32) {
	t.Helper()

	var reqCount atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reqCount.Add(1)
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	opts := ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	}
	if rps > 0 {
		opts.RateLimitPerSecond = rps
		opts.RateLimitBurst = burst
	}
	c, err := NewClient(opts)
	require.NoError(t, err, "NewClient against the local h2 server")
	t.Cleanup(func() { _ = c.Close() })

	return c, &reqCount
}

// doN issues n identical GETs and returns how long they took.
func doN(t *testing.T, c *Client, n int) time.Duration {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	for i := 0; i < n; i++ {
		var resp Response
		require.NoErrorf(t, c.Do(ctx, &Request{Method: "GET", Path: "/"}, &resp), "Do %d", i)
		require.Equalf(t, 200, resp.Status, "status on request %d = %d, want 200", i, resp.Status)
	}
	return time.Since(start)
}

// TestClient_RateLimit_BlocksExcess verifies the client blocks requests beyond
// the rate budget. The expected minimum elapsed time is derived from the
// parameters: burst tokens are free, the (need - burst)th request must wait for
// one token at 1/rps rate.
//
// TestClient_NoRateLimit below is this test's control arm — the same request
// count over the same server with the limiter absent. Without it, a floor of
// 450ms would also be met by a client that was merely slow, and the elapsed
// figure would say nothing about the limiter. Both log their measurement.
func TestClient_RateLimit_BlocksExcess(t *testing.T) {
	const (
		rps   = 2.0
		burst = 2
		need  = 3
		slack = 50 * time.Millisecond
	)
	// 3 requests, burst 2 → 1st two instant, 3rd waits one full token interval.
	// Floor = (need-burst)/rps - slack (we allow the 3rd to land a hair early if
	// the limiter just refilled).
	expectedMin := time.Duration(float64(need-burst)/rps*float64(time.Second)) - slack
	c, reqCount := newRateLimitedH2Client(t, rps, burst)

	elapsed := doN(t, c, need)

	t.Logf("timings: rps=%v burst=%d need=%d → floor %v, measured %v (control: TestClient_NoRateLimit)",
		rps, burst, need, expectedMin, elapsed)
	assert.GreaterOrEqualf(t, elapsed, expectedMin,
		"%d requests took %v, expected >= %v — the request past the burst of %d was "+
			"not made to wait for its token", need, elapsed, expectedMin, burst)
	assert.Equalf(t, int32(need), reqCount.Load(),
		"server received %d requests, want %d — the limiter dropped or duplicated one "+
			"rather than delaying it", reqCount.Load(), need)
}

// TestRateLimiter_NoDoneContext covers the no-Done-channel slow path in Take:
// when ctx.Done() == nil (context.Background()) tokens are exhausted,
// the limiter falls back to a blocking time.Sleep loop until tokens refill.
// Uses a very high rps (10000) so the wait is ~0.1ms — essentially instant.
func TestRateLimiter_NoDoneContext(t *testing.T) {
	// 10000 rps, burst 1. First Take drains the single token (fast path).
	// Second Take finds tokens < 1, ctx.Done() == nil → slow sleep loop.
	rl := newRateLimiter(10000, 1)
	require.NoError(t, rl.Take(context.Background()), "draining the single initial token")
	require.Truef(t, context.Background().Done() == nil,
		"context.Background() grew a Done channel, so this no longer reaches the slow path it names")

	// At 10000 rps each token takes 0.1ms; the deadline only stops a hang.
	done := make(chan error, 1)
	go func() { done <- rl.Take(context.Background()) }()

	select {
	case err := <-done:
		assert.NoErrorf(t, err, "Take on the no-Done path: %v", err)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Take (no-Done path) blocked for > 2s",
			"the fallback sleep loop never re-checked the bucket")
	}
}

// TestClient_NoRateLimit verifies the default (zero) does not rate-limit, and is
// the control arm for TestClient_RateLimit_BlocksExcess: same server, same
// request shape, limiter absent.
func TestClient_NoRateLimit(t *testing.T) {
	const need = 5
	c, reqCount := newRateLimitedH2Client(t, 0, 0)

	elapsed := doN(t, c, need)

	t.Logf("timings (control, no limiter): %d requests in %v", need, elapsed)
	assert.Lessf(t, elapsed, 500*time.Millisecond,
		"%d requests took %v with no RateLimitPerSecond set, expected < 500ms — a limiter "+
			"is throttling a client that configured none", need, elapsed)
	assert.Equalf(t, int32(need), reqCount.Load(),
		"server received %d requests, want %d", reqCount.Load(), need)
}
