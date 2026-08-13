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
)

// TestRateLimiter_AllowsBurst verifies the initial burst is honored.
func TestRateLimiter_AllowsBurst(t *testing.T) {
	rl := newRateLimiter(10, 5) // 10 rps, burst 5
	for i := 0; i < 5; i++ {
		if err := rl.Take(context.Background()); err != nil {
			t.Errorf("Take %d failed: %v", i, err)
		}
	}
	// 6th take should block briefly (no burst left).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := rl.Take(ctx); err == nil {
		t.Error("expected timeout on 6th take, got nil")
	}
}

// TestRateLimiter_RefillsOverTime verifies tokens replenish at rps rate.
func TestRateLimiter_RefillsOverTime(t *testing.T) {
	rl := newRateLimiter(100, 1) // 100 rps, burst 1
	// Drain.
	if err := rl.Take(context.Background()); err != nil {
		t.Fatalf("Take 1: %v", err)
	}
	// 10ms later we should have 1 token (100 rps = 1 per 10ms).
	time.Sleep(15 * time.Millisecond)
	if err := rl.Take(context.Background()); err != nil {
		t.Errorf("Take 2 after sleep failed: %v", err)
	}
}

// TestRateLimiter_ContextCancel verifies Take respects ctx cancellation.
func TestRateLimiter_ContextCancel(t *testing.T) {
	rl := newRateLimiter(1, 1) // 1 rps, burst 1
	if err := rl.Take(context.Background()); err != nil {
		t.Fatalf("Take 1: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := rl.Take(ctx); err == nil {
		t.Error("expected ctx error, got nil")
	}
	elapsed := time.Since(start)
	if elapsed < 25*time.Millisecond {
		t.Errorf("returned too fast: %v", elapsed)
	}
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
		if err := rl.Take(context.Background()); err != nil {
			t.Fatalf("draining the initial burst, Take %d: %v", i+1, err)
		}
	}

	time.Sleep(idle)

	// The bucket has been idle long enough to refill past burst. Exactly burst
	// tokens must be available, not the 4 that elapsed time alone would produce.
	start := time.Now()
	for i := 0; i < burst; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		err := rl.Take(ctx)
		cancel()
		if err != nil {
			t.Fatalf("after %v idle, Take %d/%d failed: %v — the bucket did not "+
				"refill to its full burst", idle, i+1, burst, err)
		}
	}
	if elapsed := time.Since(start); elapsed > tokenInterval/2 {
		t.Fatalf("draining the refilled burst took %v, more than half a token "+
			"interval (%v) — the process stalled, so the next assertion cannot "+
			"distinguish a capped bucket from a freshly refilled one",
			elapsed, tokenInterval)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := rl.Take(ctx); err == nil {
		t.Errorf("a %d+1st token was available after %v idle: refillLocked let the "+
			"bucket grow past its burst of %d", burst, idle, burst)
	}
}

// TestClient_RateLimit_BlocksExcess verifies the client blocks
// requests beyond the rate budget. The expected minimum elapsed
// time is derived from the parameters: burst tokens are free, the
// (need - burst)th request must wait for one token at 1/rps rate.
func TestClient_RateLimit_BlocksExcess(t *testing.T) {
	var reqCount atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	const (
		rps   = 2.0
		burst = 2
		need  = 3
		slack = 50 * time.Millisecond
	)
	// 3 requests, burst 2 → 1st two instant, 3rd waits one full
	// token interval. Floor = (need-burst)/rps - slack (we allow
	// the 3rd to land a hair early if the limiter just refilled).
	expectedMin := time.Duration(float64(need-burst)/rps*float64(time.Second)) - slack

	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		RateLimitPerSecond: rps,
		RateLimitBurst:     burst,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Cap the run to a generous absolute deadline so a bug in the
	// limiter (e.g. forgets to block) can't hang the test forever.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	for i := 0; i < need; i++ {
		var resp Response
		if err := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &resp); err != nil {
			t.Fatalf("Do %d: %v", i, err)
		}
		if resp.Status != 200 {
			t.Errorf("status = %d, want 200", resp.Status)
		}
	}
	elapsed := time.Since(start)

	if elapsed < expectedMin {
		t.Errorf("%d requests took %v, expected >= %v (rate limited)", need, elapsed, expectedMin)
	}
	if got := reqCount.Load(); got != int32(need) {
		t.Errorf("server received %d requests, want %d", got, need)
	}
}

// TestRateLimiter_NoDoneContext covers the no-Done-channel slow path in Take:
// when ctx.Done() == nil (context.Background()) tokens are exhausted,
// the limiter falls back to a blocking time.Sleep loop until tokens refill.
// Uses a very high rps (10000) so the wait is ~0.1ms — essentially instant.
func TestRateLimiter_NoDoneContext(t *testing.T) {
	// 10000 rps, burst 1.  First Take drains the single token (fast path).
	// Second Take finds tokens < 1, ctx.Done() == nil → slow sleep loop.
	rl := newRateLimiter(10000, 1)

	if err := rl.Take(context.Background()); err != nil {
		t.Fatalf("Take 1: %v", err)
	}

	// At 10000 rps each token takes 0.1ms; add a small deadline so the
	// test cannot hang if the implementation loops unexpectedly.
	done := make(chan error, 1)
	go func() {
		done <- rl.Take(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Take 2 (no-Done path): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Take 2 (no-Done path) blocked for > 2s")
	}
}

// TestClient_NoRateLimit verifies default (zero) doesn't rate-limit.
func TestClient_NoRateLimit(t *testing.T) {
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
		// No RateLimitPerSecond
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	for i := 0; i < 5; i++ {
		var resp Response
		if err := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &resp); err != nil {
			t.Fatalf("Do %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Errorf("5 requests took %v, expected < 500ms (no rate limit)", elapsed)
	}
}
