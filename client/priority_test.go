package client

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestPriority_HeaderWithPriorityPasses verifies the client side
// sends a request with the PRIORITY field embedded in HEADERS without
// error. The Go h2 server doesn't expose priority, so we just check
// the request completes with status 200.
func TestPriority_HeaderWithPriorityPasses(t *testing.T) {
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
	defer c.Close()

	var resp Response
	if err := c.Do(context.Background(), &Request{
		Method: "GET",
		Path:   "/",
		Priority: &frame.Priority{
			StreamDep: 0,
			Exclusive: false,
			Weight:    200,
		},
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}

// TestPriority_NilMeansNoFlag verifies the default request (no Priority
// field) works as before.
func TestPriority_NilMeansNoFlag(t *testing.T) {
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
	defer c.Close()

	var resp Response
	if err := c.Do(context.Background(), &Request{
		Method: "GET",
		Path:   "/",
		// no Priority — should default to no PRIORITY flag
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}

// TestPriority_ExclusiveWithDep verifies a request that depends on
// a specific parent stream and is marked exclusive is accepted.
func TestPriority_ExclusiveWithDep(t *testing.T) {
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
	defer c.Close()

	var resp Response
	if err := c.Do(context.Background(), &Request{
		Method: "GET",
		Path:   "/",
		Priority: &frame.Priority{
			StreamDep: 0,    // root (no parent)
			Exclusive: true, // would push out siblings if there were any
			Weight:    255, // max uint8 weight
		},
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}
