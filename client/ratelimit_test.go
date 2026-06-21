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

// TestRateLimiter_AllowNonBlocking verifies Allow() doesn't block.
func TestRateLimiter_AllowNonBlocking(t *testing.T) {
	rl := newRateLimiter(1, 2)
	if !rl.Allow() {
		t.Error("Allow 1 returned false")
	}
	if !rl.Allow() {
		t.Error("Allow 2 returned false")
	}
	if rl.Allow() {
		t.Error("Allow 3 should return false (no burst left)")
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
		rps     = 2.0
		burst   = 2
		need    = 3
		slack   = 50 * time.Millisecond
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
