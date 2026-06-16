package client_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// startCountedTLSServer starts a TLS/HTTP2 test server that counts new
// connections via ConnState. The counter must be set before StartTLS, so the
// server is created unstarted, the ConnState callback attached, and then
// started. Cleanup is registered on t.
func startCountedTLSServer(t *testing.T) (*httptest.Server, client.Address, *atomic.Int32) {
	t.Helper()
	var count atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			count.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return srv, client.Address{Host: host, Port: port}, &count
}

// TestTransportManaged_RoundRobin_DistributesDials verifies that with three
// backend servers, RoundRobin selection causes each server to receive at least
// one connection when nine requests are issued sequentially.
func TestTransportManaged_RoundRobin_DistributesDials(t *testing.T) {
	t.Parallel()

	srv1, addr1, cnt1 := startCountedTLSServer(t)
	srv2, addr2, cnt2 := startCountedTLSServer(t)
	srv3, addr3, cnt3 := startCountedTLSServer(t)

	// All three servers share the same TLS config (all are httptest servers),
	// but we need the dialer to trust each server's cert.  Use srv1's TLS
	// config — in httptest all servers on the same process share the same
	// self-signed root, so one dialer covers all.
	dialer := newTLSDialer(srv1)
	_ = srv2
	_ = srv3

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr1, addr2, addr3),
		Selector:  client.RoundRobin(),
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
		Pool:      &client.PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	for i := 0; i < 9; i++ {
		var resp client.Response
		if err := doWithRetry(t, c, context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp); err != nil {
			t.Fatalf("Do(%d): %v", i, err)
		}
		if resp.Status != 200 {
			t.Errorf("request %d: status = %d, want 200", i, resp.Status)
		}
	}

	if cnt1.Load() < 1 {
		t.Errorf("server1 got %d conns, want >= 1", cnt1.Load())
	}
	if cnt2.Load() < 1 {
		t.Errorf("server2 got %d conns, want >= 1", cnt2.Load())
	}
	if cnt3.Load() < 1 {
		t.Errorf("server3 got %d conns, want >= 1", cnt3.Load())
	}
}

// TestTransportManaged_MultiServer_AllReachable verifies that all four requests
// succeed (200) when two backend servers are both healthy.
func TestTransportManaged_MultiServer_AllReachable(t *testing.T) {
	t.Parallel()

	srv1, addr1 := startOneTLSServer(t)
	srv2, addr2 := startOneTLSServer(t)

	// Both httptest servers share the same in-process TLS root; one dialer
	// trusts both.
	dialer := newTLSDialer(srv1)
	_ = srv2

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr1, addr2),
		Selector:  client.RoundRobin(),
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	for i := 0; i < 4; i++ {
		var resp client.Response
		if err := doWithRetry(t, c, context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp); err != nil {
			t.Fatalf("Do(%d): %v", i, err)
		}
		if resp.Status != 200 {
			t.Errorf("request %d: status = %d, want 200", i, resp.Status)
		}
	}
}

// TestTransportManaged_Validation_MissingResolver asserts that NewClient
// returns ErrInvalidOptions when Resolver is nil for TransportManaged.
func TestTransportManaged_Validation_MissingResolver(t *testing.T) {
	t.Parallel()

	srv, _ := startOneTLSServer(t)
	_, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  nil,
		ConnOpts:  conn.ConnOptions{Dialer: newTLSDialer(srv)},
	})
	if err == nil {
		t.Fatal("NewClient: expected error, got nil")
	}
	if !errors.Is(err, client.ErrInvalidOptions) {
		t.Errorf("NewClient error = %v, want errors.Is(err, ErrInvalidOptions)", err)
	}
}

// TestTransportManaged_Validation_AddrConflict asserts that NewClient returns
// ErrInvalidOptions when both Addr and Resolver are supplied for
// TransportManaged.
func TestTransportManaged_Validation_AddrConflict(t *testing.T) {
	t.Parallel()

	srv, addr := startOneTLSServer(t)
	_, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Addr:      "localhost:8080",
		Resolver:  client.StaticResolver(addr),
		ConnOpts:  conn.ConnOptions{Dialer: newTLSDialer(srv)},
	})
	if err == nil {
		t.Fatal("NewClient: expected error, got nil")
	}
	if !errors.Is(err, client.ErrInvalidOptions) {
		t.Errorf("NewClient error = %v, want errors.Is(err, ErrInvalidOptions)", err)
	}
}

// TestTransportManaged_PoolStats_AddressCount verifies that PoolStats reports
// the number of resolved addresses correctly after the pool has been used.
func TestTransportManaged_PoolStats_AddressCount(t *testing.T) {
	t.Parallel()

	srv1, addr1 := startOneTLSServer(t)
	srv2, addr2 := startOneTLSServer(t)

	dialer := newTLSDialer(srv1)
	_ = srv2

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr1, addr2),
		Selector:  client.RoundRobin(),
		ConnOpts:  conn.ConnOptions{Dialer: dialer},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var _res client.Response
	if err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &_res); err != nil {
		t.Fatalf("Do: %v", err)
	}

	st := c.PoolStats()
	if st.Addresses != 2 {
		t.Errorf("PoolStats.Addresses = %d, want 2", st.Addresses)
	}
}
