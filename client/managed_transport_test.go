package client_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newTLSDialer(srv *httptest.Server) conn.Dialer {
	return &conn.TLSDialer{Config: srv.Client().Transport.(*http.Transport).TLSClientConfig}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	if err := c.Do(context.Background(), &client.Request{
		Method: "GET",
		Path:   "/",
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var _res client.Response
	if err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &_res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	st := c.PoolStats()
	if st.ActiveConns < 1 {
		t.Errorf("PoolStats.ActiveConns = %d after request, want >= 1", st.ActiveConns)
	}
}
