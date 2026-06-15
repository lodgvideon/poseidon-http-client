package client

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestClient_Shutdown_NewRequestAfterShutdownReturnsError verifies that
// after Shutdown, new requests return an error.
func TestClient_Shutdown_NewRequestAfterShutdownReturnsError(t *testing.T) {
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
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// Trigger lazy dial + run one request to establish conn.
	var resp Response
	if err := c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}

	// Shutdown.
	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// A second request should now fail.
	var resp2 Response
	if err := c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp2); err == nil {
		t.Error("expected error after Shutdown, got nil")
	}
}

// TestClient_Shutdown_Idempotent verifies that calling Shutdown twice
// is safe.
func TestClient_Shutdown_Idempotent(t *testing.T) {
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
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Errorf("Shutdown 1: %v", err)
	}
	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Errorf("Shutdown 2: %v", err)
	}
}

// TestClient_Shutdown_NoConnYet verifies Shutdown on a client that
// never made a request is a no-op.
func TestClient_Shutdown_NoConnYet(t *testing.T) {
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
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}
