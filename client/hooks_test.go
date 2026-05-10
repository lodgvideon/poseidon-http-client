package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newH2TestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, srv.Listener.Addr().String()
}

func TestHooks_OnRequestStartAndComplete(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var startN, completeN atomic.Int32
	var lastStatus atomic.Int32
	hooks := &client.Hooks{
		OnRequestStart: func(e client.RequestStartEvent) {
			startN.Add(1)
			if e.Method != "GET" || e.Path != "/x" {
				t.Errorf("RequestStartEvent = %+v", e)
			}
		},
		OnRequestComplete: func(e client.RequestCompleteEvent) {
			completeN.Add(1)
			lastStatus.Store(int32(e.Status))
			if e.Latency <= 0 {
				t.Errorf("Latency = %v, want > 0", e.Latency)
			}
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	resp, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/x"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
	if startN.Load() != 1 {
		t.Errorf("OnRequestStart fired %d times, want 1", startN.Load())
	}
	if completeN.Load() != 1 {
		t.Errorf("OnRequestComplete fired %d times, want 1", completeN.Load())
	}
	if lastStatus.Load() != 200 {
		t.Errorf("complete event status = %d, want 200", lastStatus.Load())
	}
}

func TestHooks_NilSafe(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		// Hooks intentionally nil.
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do (nil hooks): %v", err)
	}
}

func TestHooks_SetHooksAfterNewClient(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var n atomic.Int32
	c.SetHooks(&client.Hooks{
		OnRequestComplete: func(client.RequestCompleteEvent) { n.Add(1) },
	})
	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if n.Load() != 1 {
		t.Errorf("OnRequestComplete after SetHooks fired %d times, want 1", n.Load())
	}
}

func TestHooks_DoStream_OnRequestStartAndComplete(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var startN, completeN atomic.Int32
	hooks := &client.Hooks{
		OnRequestStart:    func(client.RequestStartEvent) { startN.Add(1) },
		OnRequestComplete: func(client.RequestCompleteEvent) { completeN.Add(1) },
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	sr, err := c.DoStream(context.Background(), &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	_ = sr.Close()
	if startN.Load() != 1 {
		t.Errorf("OnRequestStart fired %d times, want 1", startN.Load())
	}
	if completeN.Load() != 1 {
		t.Errorf("OnRequestComplete fired %d times, want 1", completeN.Load())
	}
}

func TestHooks_OnRetry(t *testing.T) {
	t.Parallel()
	// Server: first request 503, subsequent 200.
	var attempts atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()

	var retryN atomic.Int32
	hooks := &client.Hooks{
		OnRetry: func(e client.RetryEvent) {
			retryN.Add(1)
			if e.Attempt < 1 {
				t.Errorf("retry attempt = %d, want >= 1", e.Attempt)
			}
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 10 * time.Millisecond },
		IsRetryable: func(_ error, resp *client.Response) bool { return resp != nil && resp.Status == 503 },
	})
	resp, err := r.Do(context.Background(), &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Retryer.Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
	if retryN.Load() != 1 {
		t.Errorf("OnRetry fired %d times, want 1", retryN.Load())
	}
}

func TestHooks_OnDial(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var dialN atomic.Int32
	hooks := &client.Hooks{
		OnDial: func(e client.DialEvent) {
			dialN.Add(1)
			if e.Addr != addr {
				t.Errorf("DialEvent.Addr = %q, want %q", e.Addr, addr)
			}
			if e.Duration <= 0 {
				t.Errorf("Duration = %v, want > 0", e.Duration)
			}
			if e.Err != nil {
				t.Errorf("Err = %v, want nil", e.Err)
			}
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
		Transport: client.TransportPool,
		Pool:      &client.PoolOptions{MaxConnsPerHost: 2},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if dialN.Load() != 1 {
		t.Errorf("OnDial fired %d times, want 1", dialN.Load())
	}
}

func TestHooks_OnConnClose_Idle(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var mu sync.Mutex
	var closeEvents []client.ConnCloseEvent
	hooks := &client.Hooks{
		OnConnClose: func(e client.ConnCloseEvent) {
			mu.Lock()
			closeEvents = append(closeEvents, e)
			mu.Unlock()
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			IdleTimeout:       50 * time.Millisecond,
			HealthCheckPeriod: 25 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(closeEvents)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(closeEvents) == 0 {
		t.Fatalf("OnConnClose never fired; expected at least 1 (idle eviction)")
	}
	if closeEvents[0].Reason != client.CloseIdle {
		t.Errorf("close reason = %v, want CloseIdle", closeEvents[0].Reason)
	}
	if closeEvents[0].Addr != addr {
		t.Errorf("close addr = %q, want %q", closeEvents[0].Addr, addr)
	}
}
