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
// requests beyond the rate budget.
func TestClient_RateLimit_BlocksExcess(t *testing.T) {
	var reqCount atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
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
		RateLimitPerSecond: 2, // 2 rps
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Issue 3 requests back-to-back. With burst=2 the 3rd should
	// take ~500ms (waiting for a token at 2 rps).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	for i := 0; i < 3; i++ {
		var resp Response
		if err := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &resp); err != nil {
			t.Fatalf("Do %d: %v", i, err)
		}
		if resp.Status != 200 {
			t.Errorf("status = %d, want 200", resp.Status)
		}
	}
	elapsed := time.Since(start)

	// 3 requests at 2 rps with burst 2: first 2 instant, 3rd after ~500ms.
	if elapsed < 400*time.Millisecond {
		t.Errorf("3 requests took %v, expected >= 400ms (rate limited)", elapsed)
	}
	if got := reqCount.Load(); got != 3 {
		t.Errorf("server received %d requests, want 3", got)
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
