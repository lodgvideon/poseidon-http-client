package http3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		require.False(t, time.Now().After(deadline), "request was never sent")
		time.Sleep(time.Millisecond)
	}
}

// TestClientDo_ContextAlreadyCancelled: Do returns the context error without
// issuing the request.
func TestClientDo_ContextAlreadyCancelled(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"}

	_, _, doErr := client.Do(ctx, req)

	assert.Equalf(t, context.Canceled, doErr,
		"Do with a cancelled context = %v, want context.Canceled", doErr)
	assert.False(t, conn.req.finSent,
		"no request should have been sent for an already-cancelled context")
}

// TestClientDo_ContextCancelMidRequest: a per-request context cancelled while the
// response loop is parked in WaitReadable makes Do return the context error and
// abort its stream — and the connection survives (docs/HTTP3_DESIGN.md §5, PR 2c).
func TestClientDo_ContextCancelMidRequest(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}} // never finishes (no chunks, fin=false)
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	req := &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"}

	_, _, doErr := client.Do(ctx, req)

	assert.Equalf(t, context.Canceled, doErr,
		"Do cancelled mid-request = %v, want context.Canceled", doErr)
	// The abandoned request stream is aborted so the server frees it.
	assert.Truef(t, conn.req.stopped && conn.req.reset,
		"cancelled Do must abort the stream: stopped=%v reset=%v", conn.req.stopped, conn.req.reset)
	assert.Equalf(t, H3RequestCancelled, conn.req.stopCode,
		"abort stop code = %#x, want H3_REQUEST_CANCELLED", conn.req.stopCode)
	assert.Equalf(t, H3RequestCancelled, conn.req.resetCode,
		"abort reset code = %#x, want H3_REQUEST_CANCELLED", conn.req.resetCode)
	// The connection survives a per-request cancel: only the stream was aborted.
	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	assert.False(t, closed, "a per-request cancel must not close the connection")
}

// TestClient_CloseWakesInflightDo: Close during an in-flight Do wakes it with the
// graceful ErrConnClosed, not context.Canceled (docs/HTTP3_DESIGN.md §5, PR 2c:
// Close latches the terminal error before cancelling connCtx).
func TestClient_CloseWakesInflightDo(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}} // never finishes
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	done := make(chan error, 1)
	go func() {
		_, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"})
		done <- err
	}()
	waitSent(t, conn) // the Do is now parked in WaitReadable

	closeErr := client.Close()

	require.NoError(t, closeErr, "Close on a connection with one in-flight Do")
	select {
	case doErr := <-done:
		assert.Truef(t, errors.Is(doErr, quic.ErrConnClosed),
			"in-flight Do woke with %v, want quic.ErrConnClosed (graceful, not context.Canceled)", doErr)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Close did not wake the in-flight Do")
	}
}
