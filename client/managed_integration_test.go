package client_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	_, addr2, cnt2 := startCountedTLSServer(t)
	_, addr3, cnt3 := startCountedTLSServer(t)
	// All three servers share the same TLS config (all are httptest servers),
	// but we need the dialer to trust each server's cert.  Use srv1's TLS
	// config — in httptest all servers on the same process share the same
	// self-signed root, so one dialer covers all.
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr1, addr2, addr3),
		Selector:  client.RoundRobin(),
		ConnOpts:  conn.ConnOptions{Dialer: newTLSDialer(srv1)},
		Pool:      &client.PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	for i := 0; i < 9; i++ {
		var resp client.Response
		err := doWithRetry(t, c, context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)
		require.NoErrorf(t, err, "Do(%d)", i)
		assert.EqualValuesf(t, 200, resp.Status, "request %d: status = %d, want 200", i, resp.Status)
	}

	assert.GreaterOrEqualf(t, cnt1.Load(), int32(1),
		"server1 got %d conns, want >= 1 — RoundRobin left an address unused", cnt1.Load())
	assert.GreaterOrEqualf(t, cnt2.Load(), int32(1),
		"server2 got %d conns, want >= 1 — RoundRobin left an address unused", cnt2.Load())
	assert.GreaterOrEqualf(t, cnt3.Load(), int32(1),
		"server3 got %d conns, want >= 1 — RoundRobin left an address unused", cnt3.Load())
}

// TestTransportManaged_MultiServer_AllReachable verifies that all four requests
// succeed (200) when two backend servers are both healthy.
func TestTransportManaged_MultiServer_AllReachable(t *testing.T) {
	t.Parallel()

	srv1, addr1 := startOneTLSServer(t)
	_, addr2 := startOneTLSServer(t)
	// Both httptest servers share the same in-process TLS root; one dialer
	// trusts both.
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr1, addr2),
		Selector:  client.RoundRobin(),
		ConnOpts:  conn.ConnOptions{Dialer: newTLSDialer(srv1)},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	statuses := make([]int, 0, 4)
	for i := 0; i < 4; i++ {
		var resp client.Response
		err := doWithRetry(t, c, context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)
		require.NoErrorf(t, err, "Do(%d)", i)
		statuses = append(statuses, resp.Status)
	}

	assert.Equal(t, []int{200, 200, 200, 200}, statuses,
		"every request must reach a healthy backend while both addresses are up")
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

	require.Error(t, err, "NewClient: expected error, got nil")
	assert.ErrorIsf(t, err, client.ErrInvalidOptions,
		"NewClient error = %v; a caller classifying this cannot tell it from a transport failure", err)
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

	require.Error(t, err, "NewClient: expected error, got nil")
	assert.ErrorIsf(t, err, client.ErrInvalidOptions,
		"NewClient error = %v; a caller classifying this cannot tell it from a transport failure", err)
}

// TestTransportManaged_PoolStats_AddressCount verifies that PoolStats reports
// the number of resolved addresses correctly after the pool has been used.
func TestTransportManaged_PoolStats_AddressCount(t *testing.T) {
	t.Parallel()

	srv1, addr1 := startOneTLSServer(t)
	_, addr2 := startOneTLSServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr1, addr2),
		Selector:  client.RoundRobin(),
		ConnOpts:  conn.ConnOptions{Dialer: newTLSDialer(srv1)},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var res client.Response
	require.NoError(t, c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &res), "Do")
	st := c.PoolStats()

	assert.Equalf(t, 2, st.Addresses,
		"PoolStats.Addresses = %d, want 2 — the aggregate must report every resolved address, "+
			"not only the ones that have been dialled", st.Addresses)
}
