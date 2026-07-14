package http3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// waitSent spins until the request stream's FIN has been sent (the request is on
// the wire and Do has entered its response loop), so a test can act on a genuinely
// in-flight Do. Bounded so a stuck send fails loudly instead of hanging.
func waitSent(t *testing.T, c *fakeConn) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		sent := c.req != nil && c.req.finSent
		c.mu.Unlock()
		if sent {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("request was never sent")
		}
		time.Sleep(time.Millisecond)
	}
}

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

// TestClientDo_ContextCancelMidRequest: a per-request context cancelled while the
// response loop is parked in WaitReadable makes Do return the context error and
// abort its stream — and the connection survives (docs/HTTP3_DESIGN.md §5, PR 2c).
func TestClientDo_ContextCancelMidRequest(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}} // never finishes (no chunks, fin=false)
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
	// The connection survives a per-request cancel: only the stream was aborted.
	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if closed {
		t.Fatal("a per-request cancel must not close the connection")
	}
}

// TestClient_CloseWakesInflightDo: Close during an in-flight Do wakes it with the
// graceful ErrConnClosed, not context.Canceled (docs/HTTP3_DESIGN.md §5, PR 2c:
// Close latches the terminal error before cancelling connCtx).
func TestClient_CloseWakesInflightDo(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}} // never finishes
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"})
		done <- err
	}()
	waitSent(t, conn) // the Do is now parked in WaitReadable

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, quic.ErrConnClosed) {
			t.Fatalf("in-flight Do woke with %v, want quic.ErrConnClosed (graceful, not context.Canceled)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake the in-flight Do")
	}
}
