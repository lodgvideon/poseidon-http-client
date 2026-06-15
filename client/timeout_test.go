package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestRequest_Timeout_Triggers verifies that a slow server causes
// the request to fail with DeadlineExceeded when Request.Timeout fires.
func TestRequest_Timeout_Triggers(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
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

	// Outer ctx has 5s timeout; per-request Timeout is 50ms.
	// The request should fail with DeadlineExceeded at ~50ms.
	outerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp Response
	start := time.Now()
	err = c.Do(outerCtx, &Request{
		Method:  "GET",
		Path:    "/",
		Timeout: 50 * time.Millisecond,
	}, &resp)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("request took %v, expected ~50ms (timeout)", elapsed)
	}
}

// TestRequest_Timeout_NotTriggered verifies that a fast server
// returns successfully when the timeout is generous.
func TestRequest_Timeout_NotTriggered(t *testing.T) {
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
		Method:  "GET",
		Path:    "/",
		Timeout: 5 * time.Second, // generous
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}

// TestRequest_NoTimeout verifies zero-value Timeout means no per-request limit.
func TestRequest_NoTimeout(t *testing.T) {
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

	// Outer ctx with 2s; per-request Timeout = 0 (use outer).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var resp Response
	if err := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}

// TestRequest_Timeout_StreamDoStream verifies Timeout applies to DoStream too.
func TestRequest_Timeout_DoStream(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
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

	var sr StreamResponse
	start := time.Now()
	err = c.DoStream(context.Background(), &Request{
		Method:  "GET",
		Path:    "/",
		Timeout: 50 * time.Millisecond,
	}, &sr)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("DoStream took %v, expected ~50ms", elapsed)
	}
}
