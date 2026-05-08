package client_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newTLSDialer(srv *httptest.Server) conn.Dialer {
	return &conn.TLSDialer{Config: srv.Client().Transport.(*http.Transport).TLSClientConfig}
}

func startOneTLSServer(t *testing.T) (*httptest.Server, client.Address) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port := 0
	for _, b := range []byte(portStr) {
		port = port*10 + int(b-'0')
	}
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

	resp, err := c.Do(context.Background(), &client.Request{
		Method: "GET",
		Path:   "/",
	})
	if err != nil {
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

	_, err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	st := c.PoolStats()
	if st.ActiveConns < 1 {
		t.Errorf("PoolStats.ActiveConns = %d after request, want >= 1", st.ActiveConns)
	}
}
