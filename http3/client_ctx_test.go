package http3

import (
	"context"
	"errors"
	"runtime"
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

// TestClient_CloseLatchesGracefullyBeforeCancelling pins the ordering inside
// Close that TestClient_CloseWakesInflightDo names but cannot express:
//
//	err := c.conn.CloseWithError(true, H3NoError, "") // latch + CONNECTION_CLOSE
//	c.connCancel()                                    // wake the parked reader
//
// Reversed, the reader's fatal(connCtx.Err()) latches FIRST, so the peer is told
// H3_INTERNAL_ERROR for what was a clean shutdown and the in-flight Do wakes with
// context.Canceled instead of the graceful ErrConnClosed. Two things kept that
// invisible (#786): the fake latched only in CloseWithError, so both orders
// answered ErrConnClosed, and no test read closeCode after a Close issued with a
// Do in flight. The first is fixed in fakeConn.terminate; this is the second.
//
// The window the wrong order opens is two instructions wide, and a test that wins
// that race by luck is not evidence. So it is WIDENED deliberately: closeHook
// makes a graceful close wait for the reader's abrupt teardown when — and only
// when — connCancel has already run. Under the correct order connCancel has not
// run when the hook fires, so the wait is skipped and the test costs nothing.
func TestClient_CloseLatchesGracefullyBeforeCancelling(t *testing.T) {
	// connCtx is published below, before anything can invoke closeHook: the poll
	// hook parks until it is cancelled, and only Close cancels it.
	var connCtx context.Context
	conn := &fakeConn{req: &fakeStream{}} // never finishes: the Do parks in WaitReadable
	conn.pollHook = func(ctx context.Context) error {
		<-ctx.Done()              // park exactly where the real reader parks
		conn.terminate(ctx.Err()) // quic.Conn.Poll latches its connCtx cancel
		return ctx.Err()
	}
	conn.closeHook = func(code uint64) {
		if code != H3NoError || connCtx == nil || connCtx.Err() == nil {
			return // connCancel has not run, so this close is first by construction
		}
		select {
		case <-conn.abrupt: // the reader's fatal() teardown; let it land, do not race it
		case <-time.After(5 * time.Second):
		}
	}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	connCtx = client.connCtx
	done := make(chan error, 1)
	go func() {
		_, _, doErr := client.Do(context.Background(),
			&Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"})
		done <- doErr
	}()
	waitSent(t, conn)
	waitParked(t, conn.req)

	closeErr := client.Close()

	require.NoErrorf(t, closeErr, "Close on a connection with one in-flight Do: %v", closeErr)
	assert.Equalf(t, H3NoError, conn.closeCode,
		"CONNECTION_CLOSE carried %#x, want H3_NO_ERROR (%#x): the peer was told this was an "+
			"internal failure when it was a clean shutdown, because connCancel woke the "+
			"reader's fatal() before Close latched the graceful error",
		conn.closeCode, H3NoError)
	assert.Truef(t, conn.closeApp,
		"§8.1 makes this an application CONNECTION_CLOSE, not a transport one")
	select {
	case doErr := <-done:
		assert.Truef(t, errors.Is(doErr, quic.ErrConnClosed),
			"in-flight Do woke with %v, want quic.ErrConnClosed: the latch is first-error-wins, "+
				"so cancelling connCtx first hands the caller the reader's context.Canceled and "+
				"a graceful shutdown becomes indistinguishable from an abort", doErr)
	case <-time.After(5 * time.Second):
		require.Fail(t, "Close did not wake the in-flight Do")
	}
}

// TestClient_CloseLatchesGracefullyWithNoDoInFlight is the control arm: the same
// fixture with no request outstanding. It shows the close code above is decided by
// the ordering rather than by the presence of a parked Do — and that the widened
// window itself does not change the answer.
func TestClient_CloseLatchesGracefullyWithNoDoInFlight(t *testing.T) {
	var connCtx context.Context
	conn := &fakeConn{req: &fakeStream{}}
	conn.pollHook = func(ctx context.Context) error {
		<-ctx.Done()
		conn.terminate(ctx.Err())
		return ctx.Err()
	}
	conn.closeHook = func(code uint64) {
		if code != H3NoError || connCtx == nil || connCtx.Err() == nil {
			return
		}
		select {
		case <-conn.abrupt:
		case <-time.After(5 * time.Second):
		}
	}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	connCtx = client.connCtx

	closeErr := client.Close()

	require.NoErrorf(t, closeErr, "Close on an idle connection: %v", closeErr)
	assert.Equalf(t, H3NoError, conn.closeCode,
		"CONNECTION_CLOSE carried %#x, want H3_NO_ERROR (%#x) with nothing in flight",
		conn.closeCode, H3NoError)
	assert.Falsef(t, client.Alive(),
		"Close returned with the reader still live; it waits on readerDone (F6)")
}

// waitParked spins until the stream's response loop has parked in WaitReadable, so
// a test acts on a Do that is genuinely blocked rather than one still sending.
// Bounded, so a Do that never parks fails loudly instead of hanging.
func waitParked(t *testing.T, s *fakeStream) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.waitReadables.Load() == 0 {
		require.False(t, time.Now().After(deadline),
			"the in-flight Do never parked in WaitReadable, so nothing below is testing a "+
				"Close that raced a blocked request")
		runtime.Gosched()
	}
}
