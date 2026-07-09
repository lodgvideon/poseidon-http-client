package http3

import (
	"context"
	"testing"
	"time"
)

// TestClientDo_ContextAlreadyCancelled: Do returns the context error without
// issuing the request.
func TestClientDo_ContextAlreadyCancelled(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"}
	if _, _, err := client.Do(ctx, req); err != context.Canceled {
		t.Fatalf("Do with a cancelled context = %v, want context.Canceled", err)
	}
	if conn.req.finSent {
		t.Fatal("no request should have been sent for an already-cancelled context")
	}
}

// TestClientDo_ContextCancelMidRequest: a context cancelled while the response
// loop is waiting on Poll makes Do return the context error.
func TestClientDo_ContextCancelMidRequest(t *testing.T) {
	conn := &fakeConn{
		req:      &fakeStream{}, // never finishes (fin=false)
		pollHook: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	req := &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"}
	if _, _, err := client.Do(ctx, req); err != context.Canceled {
		t.Fatalf("Do cancelled mid-request = %v, want context.Canceled", err)
	}
}
