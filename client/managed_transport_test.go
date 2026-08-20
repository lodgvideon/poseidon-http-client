package client_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newTLSDialer(srv *httptest.Server) conn.Dialer {
	// Clone so we own an independent copy: srv.Close() writes to the server's
	// Transport.TLSClientConfig via http2configureTransports, which would race
	// with our TLSDialer.Dial() reading the same pointer.
	return &conn.TLSDialer{Config: srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()}
}

func startOneTLSServer(t *testing.T) (*httptest.Server, client.Address) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return srv, client.Address{Host: host, Port: port}
}

func TestNewClient_TransportManaged_Smoke(t *testing.T) {
	t.Parallel()

	srv, addr := startOneTLSServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr),
		ConnOpts:  conn.ConnOptions{Dialer: newTLSDialer(srv)},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var resp client.Response
	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do")
	assert.Equalf(t, 200, resp.Status, "status = %d, want 200", resp.Status)
}

func TestNewClient_TransportManaged_PoolStats(t *testing.T) {
	t.Parallel()

	srv, addr := startOneTLSServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr),
		ConnOpts:  conn.ConnOptions{Dialer: newTLSDialer(srv)},
		Pool:      &client.PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 4},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var res client.Response
	require.NoError(t, c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &res), "Do")
	st := c.PoolStats()

	assert.GreaterOrEqualf(t, st.ActiveConns, 1,
		"PoolStats.ActiveConns = %d after request, want >= 1 — a served request must be visible "+
			"in the aggregate stats", st.ActiveConns)
}
