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
	// The abandoned request stream is aborted so the server frees it.
	if !conn.req.stopped || !conn.req.reset {
		t.Fatalf("cancelled Do must abort the stream: stopped=%v reset=%v", conn.req.stopped, conn.req.reset)
	}
	if conn.req.stopCode != H3RequestCancelled || conn.req.resetCode != H3RequestCancelled {
		t.Fatalf("abort codes = stop %#x reset %#x, want H3_REQUEST_CANCELLED", conn.req.stopCode, conn.req.resetCode)
	}
}
