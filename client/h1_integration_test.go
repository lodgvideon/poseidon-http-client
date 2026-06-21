package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func startH1Server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proto", r.Proto)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestNewClient_H1SingleConn_Smoke(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
	if string(resp.Body) != "ok" {
		t.Errorf("body = %q, want %q", resp.Body, "ok")
	}
}

func TestNewClient_H1SingleConn_MultipleRequests(t *testing.T) {
	t.Parallel()
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		_, _ = w.Write([]byte("pong"))
	}))
	t.Cleanup(srv.Close)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	for i := 0; i < 5; i++ {
		var resp client.Response
		resp.Reset()
		if err := c.Do(context.Background(), &client.Request{
			Method:   "GET",
			Path:     "/",
			WantBody: true,
		}, &resp); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status != 200 {
			t.Errorf("request %d: status = %d, want 200", i, resp.Status)
		}
	}
	if count != 5 {
		t.Errorf("server received %d requests, want 5", count)
	}
}

func TestNewClient_H1SingleConn_POST_Body(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		w.WriteHeader(201)
		_, _ = w.Write(buf[:n])
	}))
	t.Cleanup(srv.Close)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	payload := []byte("hello world")
	if err := c.Do(context.Background(), &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     payload,
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 201 {
		t.Errorf("status = %d, want 201", resp.Status)
	}
	if string(resp.Body) != string(payload) {
		t.Errorf("echo body = %q, want %q", resp.Body, payload)
	}
}

func TestNewClient_ALPN_NegotiatesH2(t *testing.T) {
	t.Parallel()
	srv, _ := startOneTLSServer(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportALPN,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.FlexDialer{
				Config: srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone(),
			},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
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
