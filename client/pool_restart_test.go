package client_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// serveH2On starts a real HTTP/2 server on an existing listener, so a test can
// rebind the same address after killing one.
func serveH2On(ln net.Listener) *httptest.Server {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.Listener = ln
	srv.EnableHTTP2 = true
	srv.StartTLS()
	return srv
}

// TestIntegration_Pool_RecoversAfterServerRestart is the scenario this library
// exists for: a load generator pointed at a target that restarts. Deploys,
// crashes and autoscaling make this a Tuesday, not an edge case.
//
// The pool holds one connection. The server is really killed and really comes
// back on the same address. The pool must notice the corpse and redial.
//
// It did not. Measured before this test existed, with a 50ms health check:
// every attempt after the restart failed on the *same socket pair*, forever —
// the pool never evicted, because eviction asks conn.IsAlive(), and IsAlive did
// not consider a dead reader to be death. The health check ran and changed
// nothing: it asks the same question.
//
// A single connection is the point, not a simplification: at MaxConnsPerHost:1
// there is nowhere to hide. A larger cap would let healthy connections mask a
// poisoned one.
func TestIntegration_Pool_RecoversAfterServerRestart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	srv := serveH2On(ln)

	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1,
			HealthCheckPeriod: 50 * time.Millisecond,
		},
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{
			Config: &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{"h2"},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	do := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return c.Do(ctx, client.GET("/"), &client.Response{})
	}

	// The control: without a healthy baseline the rest measures a broken setup,
	// not a broken pool.
	if err := do(); err != nil {
		t.Fatalf("request against a healthy server failed: %v", err)
	}

	srv.Close()
	time.Sleep(200 * time.Millisecond)

	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("cannot rebind %s after the server closed it: %v", addr, err)
	}
	srv2 := serveH2On(ln2)
	defer srv2.Close()

	// The first attempt is allowed to fail: it is the one that discovers the
	// corpse. What matters is that a later one succeeds — that the pool learned.
	var lastErr error
	for i := range 5 {
		if lastErr = do(); lastErr == nil {
			t.Logf("recovered on attempt %d", i)
			return
		}
	}
	t.Errorf("pool never recovered after the server restarted: 5 attempts, last error: %v", lastErr)
}
