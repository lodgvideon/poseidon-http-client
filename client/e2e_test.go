package client_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Every test in this file opens with an unconditional t.Skip, so none of them
// runs anywhere — see the tracking issue filed alongside the #723 sweep. The
// helpers below are NOT dead: e2e_stress_test.go (//go:build e2e_remote) uses
// them.

// e2eClient creates a client connected to a real remote HTTP/2 server over TLS.
func e2eClient(t *testing.T, host string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr: net.JoinHostPort(host, "443"),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{
				Config: &tls.Config{
					ServerName: host,
					MinVersion: tls.VersionTLS12,
				},
			},
		},
	})
	require.NoErrorf(t, err, "NewClient(%s)", host)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ua() []hpack.HeaderField {
	return []hpack.HeaderField{{Name: []byte("user-agent"), Value: []byte("poseidon-e2e-test/1.0")}}
}

// doGET is a helper: runs GET, returns response. Caller inspects resp fields.
func doGET(c *client.Client, ctx context.Context, path string, wantBody bool) (client.Response, error) {
	mode := client.BodyDiscard
	if wantBody {
		mode = client.BodyBuffer
	}
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     path,
		Headers:  ua(),
		BodyMode: mode,
	}, &resp)
	return resp, err
}

// ---------- google.com ----------

func TestE2E_Google_GET_Root(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/", true)

	require.NoError(t, err, "Do")
	require.GreaterOrEqualf(t, resp.Status, 200, "unexpected status %d", resp.Status)
	require.LessOrEqualf(t, resp.Status, 399, "unexpected status %d", resp.Status)
	require.NotEmpty(t, resp.Body, "expected non-empty body")
	assert.Contains(t, string(resp.Body), "google", "body does not contain 'google'")
}

func TestE2E_Google_GET_404(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/this-page-does-not-exist-404-test-poseidon", true)

	require.NoError(t, err, "Do")
	assert.Equal(t, 404, resp.Status, "a path that does not exist must surface as 404")
}

func TestE2E_Google_HEAD(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "HEAD",
		Path:     "/",
		Headers:  ua(),
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoError(t, err, "Do")
	assert.GreaterOrEqualf(t, resp.Status, 200, "unexpected status %d", resp.Status)
	assert.LessOrEqualf(t, resp.Status, 399, "unexpected status %d", resp.Status)
}

// ---------- Connection reuse: 5 sequential requests ----------

func TestE2E_Google_MultipleRequests_SameConn(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 5 {
		resp, err := doGET(c, ctx, "/", false)

		require.NoErrorf(t, err, "request %d: Do", i)
		require.GreaterOrEqualf(t, resp.Status, 200, "request %d: got %d", i, resp.Status)
		require.LessOrEqualf(t, resp.Status, 399, "request %d: got %d", i, resp.Status)
	}

	snap := c.Metrics().Snapshot()
	// All 5 requests must reuse the same connection (1 dial) — that is the
	// property this test exists for, not the statuses above.
	require.EqualValues(t, 1, snap.Counters.DialsAttempted,
		"expected exactly 1 dial (conn reuse)")
	require.GreaterOrEqualf(t, snap.Counters.RequestsStarted, uint64(5),
		"expected >=5 started, got %d", snap.Counters.RequestsStarted)
}

// ---------- Concurrent requests on single connection ----------

func TestE2E_Google_ConcurrentRequests(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 10
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		go func() {
			resp, err := doGET(c, ctx, "/", false)
			if err != nil {
				errCh <- err
				return
			}
			// Google may return 200 or 302 depending on geo/AB — both are valid.
			if resp.Status < 200 || resp.Status > 399 {
				errCh <- fmt.Errorf("unexpected status %d", resp.Status)
				return
			}
			errCh <- nil
		}()
	}

	for i := 0; i < n; i++ {
		assert.NoErrorf(t, <-errCh, "goroutine %d", i)
	}
}

// ---------- Metrics ----------

func TestE2E_Google_Metrics(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := doGET(c, ctx, "/", true)

	require.NoError(t, err, "Do")
	snap := c.MetricsSnapshot()
	assert.NotZero(t, snap.Counters.RequestsStarted, "expected RequestsStarted > 0")
	assert.NotZero(t, snap.Counters.DialsAttempted, "expected DialsAttempted > 0")
}

// ---------- Headers round-trip ----------

func TestE2E_Google_ResponseHeaders(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/", false)

	require.NoError(t, err, "Do")
	// Check that standard response headers are present.
	var hasServer, hasContentType bool
	for _, h := range resp.Headers {
		if string(h.Name) == "server" {
			hasServer = true
		}
		if string(h.Name) == "content-type" {
			hasContentType = true
		}
	}
	assert.True(t, hasServer, "response missing 'server' header")
	assert.True(t, hasContentType, "response missing 'content-type' header")
}

// ---------- Large body (google returns ~80KB) ----------

func TestE2E_Google_LargeBody(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Use /robots.txt which reliably returns content.
	resp, err := doGET(c, ctx, "/robots.txt", true)

	require.NoError(t, err, "Do")
	assert.GreaterOrEqualf(t, resp.Status, 200, "unexpected status %d", resp.Status)
	assert.LessOrEqualf(t, resp.Status, 399, "unexpected status %d", resp.Status)
	assert.NotEmpty(t, resp.Body, "expected non-empty body")
}

// ---------- Repeated client usage (open/close cycle) ----------

func TestE2E_Google_ClientCloseReopen(t *testing.T) {
	t.Skip(e2eSkipReason)

	const host = "www.google.com"

	for i := range 3 {
		c := e2eClient(t, host)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := doGET(c, ctx, "/", false)
		cancel()

		require.NoErrorf(t, err, "cycle %d: Do", i)
		require.GreaterOrEqualf(t, resp.Status, 200, "cycle %d: got %d", i, resp.Status)
		require.LessOrEqualf(t, resp.Status, 399, "cycle %d: got %d", i, resp.Status)
		c.Close()
	}
}
