package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams verifies that the
// connection pool opens additional connections when the peer advertises a small
// MAX_CONCURRENT_STREAMS value, honoring RFC 7540 §5.1.2.
func TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams(t *testing.T) {
	// Server caps concurrent streams to 2, forcing the pool to open multiple
	// connections to serve more than 2 concurrent requests.
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(200)
	}))
	if err := http2.ConfigureServer(srv.Config, &http2.Server{MaxConcurrentStreams: 2}); err != nil {
		t.Fatalf("ConfigureServer: %v", err)
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   4,
			MaxStreamsPerConn: 0, // unbounded local — peer cap governs
			HealthCheckPeriod: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	const N = 8
	var wg sync.WaitGroup
	wg.Add(N)
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Do: %v", err)
	}

	s := c.PoolStats()
	if s.ActiveConns < 2 {
		t.Fatalf("ActiveConns = %d, want >= 2 (peer MAX_CONCURRENT_STREAMS=2 not honored)", s.ActiveConns)
	}
}

// TestConformance_RFC7540_Sec6_8_PoolDrainsOnGoAway verifies that the
// connection pool drains all connections after the peer sends GOAWAY,
// honoring RFC 7540 §6.8.
func TestConformance_RFC7540_Sec6_8_PoolDrainsOnGoAway(t *testing.T) {
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 4,
			HealthCheckPeriod: 50 * time.Millisecond,
			DialBackoff:       10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("first Do = %v", err)
	}

	shCtx, shCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = srv.Config.Shutdown(shCtx)
	shCancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.PoolStats().ActiveConns == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("ActiveConns = %d, want 0 after peer shutdown", c.PoolStats().ActiveConns)
}
