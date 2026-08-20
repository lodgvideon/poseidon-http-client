package client

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTimeoutClient starts an h2 server whose handler stalls for `stall` before
// answering 200, and returns a client aimed at it. stall = 0 answers instantly,
// which is the control shape for the two deadline tests.
func newTimeoutClient(t *testing.T, stall time.Duration) *Client {
	t.Helper()

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if stall > 0 {
			time.Sleep(stall)
		}
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoError(t, err, "NewClient against the local h2 server")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestRequest_Timeout_Triggers verifies that a slow server causes the request to
// fail with DeadlineExceeded when Request.Timeout fires.
//
// The elapsed bound is what makes this more than an error-type check: the
// handler stalls 500ms and the per-request Timeout is 50ms, so a client that
// ignored Timeout and rode the outer 5s context would still finish — with a
// SUCCESS at ~500ms, not a deadline error. TestRequest_Timeout_NotTriggered is
// the control arm: same server shape with the stall removed and a generous
// Timeout, proving the request completes when the deadline is not the binding
// constraint. Both log their measurement.
func TestRequest_Timeout_Triggers(t *testing.T) {
	const stall, timeout = 500 * time.Millisecond, 50 * time.Millisecond
	c := newTimeoutClient(t, stall)
	// Outer ctx has 5s; the per-request Timeout is 50ms, so the per-request
	// deadline is the only thing that can end this early.
	outerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp Response

	start := time.Now()
	err := c.Do(outerCtx, &Request{Method: "GET", Path: "/", Timeout: timeout}, &resp)
	elapsed := time.Since(start)

	t.Logf("timings: handler stall=%v, Request.Timeout=%v, outer ctx=5s, Do returned after %v",
		stall, timeout, elapsed)
	require.Error(t, err, "expected a timeout error, got nil")
	assert.ErrorIsf(t, err, context.DeadlineExceeded,
		"err = %v, want context.DeadlineExceeded — a caller distinguishing a deadline from "+
			"a transport failure cannot tell them apart otherwise", err)
	assert.Lessf(t, elapsed, 300*time.Millisecond,
		"request took %v, expected ~%v — anything at or past the %v handler stall means the "+
			"per-request Timeout did not bind and the outer context carried the request",
		elapsed, timeout, stall)
}

// TestRequest_Timeout_NotTriggered is the control arm for the two deadline
// tests: a fast server and a generous timeout must succeed.
func TestRequest_Timeout_NotTriggered(t *testing.T) {
	c := newTimeoutClient(t, 0)
	var resp Response

	start := time.Now()
	err := c.Do(context.Background(), &Request{
		Method:  "GET",
		Path:    "/",
		Timeout: 5 * time.Second, // generous
	}, &resp)
	elapsed := time.Since(start)

	t.Logf("timings (control, no stall): Request.Timeout=5s, Do returned after %v", elapsed)
	require.NoError(t, err, "Do against an instant server with a 5s Timeout")
	assert.Equalf(t, 200, resp.Status,
		"status = %d, want 200 — a Timeout well above the response time must not affect "+
			"the request at all", resp.Status)
}

// TestRequest_NoTimeout verifies zero-value Timeout means no per-request limit:
// the outer context governs instead.
func TestRequest_NoTimeout(t *testing.T) {
	c := newTimeoutClient(t, 0)
	// Outer ctx with 2s; per-request Timeout = 0, so the outer one is used.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var resp Response

	err := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do with a zero Timeout must fall through to the caller's context")
	assert.Equalf(t, 200, resp.Status,
		"status = %d, want 200 — a zero Timeout must mean 'no per-request limit', not "+
			"'deadline already expired'", resp.Status)
}

// TestRequest_Timeout_DoStream verifies Timeout applies to DoStream too — the
// two entry points apply it separately, so neither can lose it alone.
func TestRequest_Timeout_DoStream(t *testing.T) {
	const stall, timeout = 500 * time.Millisecond, 50 * time.Millisecond
	c := newTimeoutClient(t, stall)
	var sr StreamResponse

	start := time.Now()
	err := c.DoStream(context.Background(), &Request{Method: "GET", Path: "/", Timeout: timeout}, &sr)
	elapsed := time.Since(start)

	t.Logf("timings: handler stall=%v, Request.Timeout=%v, DoStream returned after %v",
		stall, timeout, elapsed)
	require.Error(t, err, "expected a timeout error, got nil")
	assert.ErrorIsf(t, err, context.DeadlineExceeded,
		"err = %v, want context.DeadlineExceeded", err)
	assert.Lessf(t, elapsed, 300*time.Millisecond,
		"DoStream took %v, expected ~%v — the streaming entry point applies Request.Timeout "+
			"separately from Do, so it can lose it on its own", elapsed, timeout)
}
