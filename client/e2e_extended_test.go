package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ============================================================
// Extended E2E tests: BodyStream, DoStream, Pool, cross-server,
// POST, context cancel, concurrent mixed workloads.
// ============================================================
//
// Every test in this file opens with an unconditional t.Skip, so none of them
// runs anywhere — see the tracking issue filed alongside the #723 sweep. They
// are kept compiling (and migrated to testify with the rest of the package) so
// that whoever re-enables them inherits assertions rather than a rewrite.

const e2eSkipReason = "E2E test against external service — disabled in local/CI environments without network access"

// ---------- BodyStream (io.ReadCloser) ----------

func TestE2E_Google_BodyStream_GET(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		Headers:  ua(),
		BodyMode: client.BodyStream,
	}, &resp)

	require.NoError(t, err, "Do(BodyStream)")
	require.GreaterOrEqualf(t, resp.Status, 200, "expected 2xx-3xx, got %d", resp.Status)
	require.LessOrEqualf(t, resp.Status, 399, "expected 2xx-3xx, got %d", resp.Status)
	require.NotNil(t, resp.BodyReader, "expected BodyReader to be non-nil for BodyMode=BodyStream")
	// resp.Body must be empty: BodyStream bypasses the buffered body.
	require.Emptyf(t, resp.Body, "expected empty resp.Body for BodyStream, got %d bytes", len(resp.Body))

	var buf bytes.Buffer
	n, err := io.Copy(&buf, resp.BodyReader)
	require.NoError(t, err, "io.Copy from BodyReader")
	assert.NotZero(t, n, "expected non-zero streamed body")
	assert.Contains(t, buf.String(), "google", "streamed body does not contain 'google'")
	// Close must be idempotent.
	if err := resp.BodyReader.Close(); err != nil {
		t.Logf("second Close: %v (ok)", err)
	}
}

func TestE2E_Google_BodyStream_Concurrent(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 5
	errCh := make(chan error, n)
	var totalBytes int64

	for i := 0; i < n; i++ {
		go func() {
			var resp client.Response
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/",
				Headers:  ua(),
				BodyMode: client.BodyStream,
			}, &resp)
			if err != nil {
				errCh <- fmt.Errorf("Do: %w", err)
				return
			}
			defer resp.BodyReader.Close()

			if resp.Status < 200 || resp.Status > 399 {
				errCh <- fmt.Errorf("status %d", resp.Status)
				return
			}

			var buf bytes.Buffer
			n, err := io.Copy(&buf, resp.BodyReader)
			if err != nil {
				errCh <- fmt.Errorf("Copy: %w", err)
				return
			}
			atomic.AddInt64(&totalBytes, n)
			errCh <- nil
		}()
	}

	for i := 0; i < n; i++ {
		assert.NoErrorf(t, <-errCh, "goroutine %d", i)
	}
	t.Logf("%d concurrent BodyStream reads: total=%d bytes", n, atomic.LoadInt64(&totalBytes))
}

// ---------- DoStream (StreamResponse.Recv) ----------

func TestE2E_Google_DoStream_GET(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var sr client.StreamResponse
	err := c.DoStream(ctx, &client.Request{
		Method:  "GET",
		Path:    "/",
		Headers: ua(),
	}, &sr)

	require.NoError(t, err, "DoStream")
	defer sr.Close()
	require.GreaterOrEqualf(t, sr.Status, 200, "expected 2xx-3xx, got %d", sr.Status)
	require.LessOrEqualf(t, sr.Status, 399, "expected 2xx-3xx, got %d", sr.Status)
	require.NotEmpty(t, sr.Headers, "expected response headers")

	// Pump events until EndStream.
	var totalData int64
	for {
		ev, rerr := sr.Recv(ctx)
		if errors.Is(rerr, client.ErrStreamEnded) {
			break
		}
		require.NoError(t, rerr, "Recv")
		switch ev.Type {
		case client.EventData:
			totalData += int64(len(ev.Data))
		case client.EventReset:
			require.Failf(t, "stream reset", "code=%v", ev.ResetCode)
		}
		if ev.EndStream {
			break
		}
	}
	require.NotZero(t, totalData, "expected non-zero DATA via DoStream")
}

func TestE2E_Google_DoStream_Concurrent(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 5
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		go func() {
			var sr client.StreamResponse
			err := c.DoStream(ctx, &client.Request{
				Method:  "GET",
				Path:    "/",
				Headers: ua(),
			}, &sr)
			if err != nil {
				errCh <- err
				return
			}
			defer sr.Close()

			if sr.Status < 200 || sr.Status > 399 {
				errCh <- fmt.Errorf("status %d", sr.Status)
				return
			}
			for {
				ev, rerr := sr.Recv(ctx)
				if errors.Is(rerr, client.ErrStreamEnded) || (rerr == nil && ev.EndStream) {
					break
				}
				if rerr != nil {
					errCh <- rerr
					return
				}
			}
			errCh <- nil
		}()
	}

	for i := 0; i < n; i++ {
		assert.NoErrorf(t, <-errCh, "goroutine %d", i)
	}
}

// ---------- Mixed Do + DoStream on same connection ----------

func TestE2E_Google_MixedDoAndDoStream(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 6
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			if idx%2 == 0 {
				// Synchronous Do — Google may return 302 redirects.
				resp, err := doGET(c, ctx, "/", true)
				if err != nil {
					errCh <- err
					return
				}
				if resp.Status < 200 || resp.Status > 399 {
					errCh <- fmt.Errorf("Do status %d", resp.Status)
					return
				}
				errCh <- nil
				return
			}
			// Streaming DoStream
			var sr client.StreamResponse
			err := c.DoStream(ctx, &client.Request{
				Method:  "GET",
				Path:    "/",
				Headers: ua(),
			}, &sr)
			if err != nil {
				errCh <- err
				return
			}
			defer sr.Close()
			if sr.Status < 200 || sr.Status > 399 {
				errCh <- fmt.Errorf("DoStream status %d", sr.Status)
				return
			}
			for {
				ev, rerr := sr.Recv(ctx)
				if errors.Is(rerr, client.ErrStreamEnded) || (rerr == nil && ev.EndStream) {
					break
				}
				if rerr != nil {
					errCh <- rerr
					return
				}
			}
			errCh <- nil
		}(i)
	}

	for i := 0; i < n; i++ {
		assert.NoErrorf(t, <-errCh, "goroutine %d", i)
	}
	snap := c.MetricsSnapshot()
	t.Logf("%d mixed Do+DoStream on 1 conn: dials=%d started=%d succeeded=%d",
		n, snap.Counters.DialsAttempted, snap.Counters.RequestsStarted, snap.Counters.RequestsSucceeded)
}

// ---------- Pool transport ----------

func TestE2E_Google_PoolTransport(t *testing.T) {
	t.Skip(e2eSkipReason)

	c, err := client.NewClient(client.ClientOptions{
		Addr: net.JoinHostPort("www.google.com", "443"),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{
				Config: &tls.Config{
					ServerName: "www.google.com",
					MinVersion: tls.VersionTLS12,
				},
			},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 2,
		},
	})
	require.NoError(t, err, "NewClient(pool)")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Sequential requests should reuse the same pooled conn.
	for i := range 5 {
		resp, derr := doGET(c, ctx, "/", false)

		require.NoErrorf(t, derr, "pool req %d", i)
		require.GreaterOrEqualf(t, resp.Status, 200, "pool req %d: status %d", i, resp.Status)
		require.LessOrEqualf(t, resp.Status, 399, "pool req %d: status %d", i, resp.Status)
	}

	stats := c.PoolStats()
	assert.NotZero(t, stats.ActiveConns, "expected at least 1 active pool connection")
}

func TestE2E_Google_Pool_Concurrent(t *testing.T) {
	t.Skip(e2eSkipReason)

	c, err := client.NewClient(client.ClientOptions{
		Addr: net.JoinHostPort("www.google.com", "443"),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{
				Config: &tls.Config{
					ServerName: "www.google.com",
					MinVersion: tls.VersionTLS12,
				},
			},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 3,
		},
	})
	require.NoError(t, err, "NewClient(pool)")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 10
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		go func() {
			resp, derr := doGET(c, ctx, "/", false)
			if derr != nil {
				errCh <- derr
				return
			}
			if resp.Status < 200 || resp.Status > 399 {
				errCh <- fmt.Errorf("status %d", resp.Status)
				return
			}
			errCh <- nil
		}()
	}

	for i := 0; i < n; i++ {
		assert.NoErrorf(t, <-errCh, "goroutine %d", i)
	}
	snap := c.MetricsSnapshot()
	t.Logf("Pool concurrent: dials=%d started=%d succeeded=%d errored=%d",
		snap.Counters.DialsAttempted, snap.Counters.RequestsStarted,
		snap.Counters.RequestsSucceeded, snap.Counters.RequestsErrored)
}

// ---------- Cross-server: GitHub API ----------

func TestE2E_GitHub_API_JSON(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "api.github.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method: "GET",
		Path:   "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("user-agent"), Value: []byte("poseidon-e2e-test/1.0")},
			{Name: []byte("accept"), Value: []byte("application/vnd.github.v3+json")},
		},
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoError(t, err, "Do")
	require.GreaterOrEqualf(t, resp.Status, 200, "expected 2xx-3xx, got %d", resp.Status)
	require.LessOrEqualf(t, resp.Status, 399, "expected 2xx-3xx, got %d", resp.Status)
	assert.Contains(t, string(resp.Body), "current_user_url",
		"GitHub API response missing 'current_user_url'")
	var hasJSON bool
	for _, h := range resp.Headers {
		if string(h.Name) == "content-type" && strings.Contains(string(h.Value), "json") {
			hasJSON = true
		}
	}
	assert.True(t, hasJSON, "response missing 'content-type: application/json'")
}

func TestE2E_GitHub_API_BodyStream(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "api.github.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method: "GET",
		Path:   "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("user-agent"), Value: []byte("poseidon-e2e-test/1.0")},
			{Name: []byte("accept"), Value: []byte("application/vnd.github.v3+json")},
		},
		BodyMode: client.BodyStream,
	}, &resp)

	require.NoError(t, err, "Do(BodyStream)")
	require.NotNil(t, resp.BodyReader, "expected BodyReader")
	defer resp.BodyReader.Close()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, resp.BodyReader)
	require.NoError(t, err, "Copy")
	assert.Contains(t, buf.String(), "current_user_url",
		"streamed GitHub response missing 'current_user_url'")
}

// ---------- POST with body ----------

func TestE2E_Google_POST_WithBody(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	body := []byte("hello poseidon e2e test body")

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Headers:  ua(),
		Body:     body,
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoError(t, err, "Do(POST)")
	// Google may return 405 (Method Not Allowed) or 302 — both prove the body
	// was sent on the wire without error.
	assert.GreaterOrEqualf(t, resp.Status, 200, "unexpected status %d", resp.Status)
	assert.LessOrEqualf(t, resp.Status, 499, "unexpected status %d", resp.Status)
}

// ---------- Context cancellation ----------

func TestE2E_Google_ContextCancel(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	// Cancel context immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := doGET(c, ctx, "/", false)

	require.Error(t, err, "expected error from cancelled context")
	assert.ErrorIsf(t, err, context.Canceled, "expected context.Canceled, got: %v", err)
}

func TestE2E_Google_ContextTimeout(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	// 1ns timeout — request should expire.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // let the deadline pass

	_, err := doGET(c, ctx, "/", true)

	require.Error(t, err, "expected error from expired context")
	assert.Truef(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
		"expected context error, got: %T: %v", err, err)
}

// ---------- Large body via BodyStream (chunked read) ----------

func TestE2E_Google_BodyStream_LargeBody(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		Headers:  ua(),
		BodyMode: client.BodyStream,
	}, &resp)

	require.NoError(t, err, "Do")
	require.NotNil(t, resp.BodyReader, "expected BodyReader")
	// Read in small chunks to verify buffering logic.
	var total int64
	buf := make([]byte, 512) // tiny read buffer
	for {
		n, rerr := resp.BodyReader.Read(buf)
		total += int64(n)
		if errors.Is(rerr, io.EOF) {
			break
		}
		require.NoError(t, rerr, "Read")
	}
	resp.BodyReader.Close()
	assert.NotZero(t, total, "expected non-zero body via BodyStream")
}

// ---------- Cross-server: nghttp2.org (reference HTTP/2) ----------

func TestE2E_Nghttp2_GET(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "nghttp2.org")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/httpbin/", true)

	if err != nil {
		t.Skipf("nghttp2.org unreachable: %v", err)
	}
	// nghttp2.org/httpbin/ may return 200 or 301.
	assert.GreaterOrEqualf(t, resp.Status, 200, "unexpected status %d", resp.Status)
	assert.LessOrEqualf(t, resp.Status, 399, "unexpected status %d", resp.Status)
}

func TestE2E_Nghttp2_DoStream(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "nghttp2.org")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var sr client.StreamResponse
	err := c.DoStream(ctx, &client.Request{
		Method:  "GET",
		Path:    "/httpbin/",
		Headers: ua(),
	}, &sr)

	if err != nil {
		t.Skipf("nghttp2.org unreachable: %v", err)
	}
	defer sr.Close()
	assert.GreaterOrEqualf(t, sr.Status, 200, "unexpected status %d", sr.Status)
	assert.LessOrEqualf(t, sr.Status, 399, "unexpected status %d", sr.Status)
	var data int64
	for {
		ev, rerr := sr.Recv(ctx)
		if errors.Is(rerr, client.ErrStreamEnded) || (rerr == nil && ev.EndStream) {
			break
		}
		require.NoError(t, rerr, "Recv")
		if ev.Type == client.EventData {
			data += int64(len(ev.Data))
		}
	}
	t.Logf("nghttp2.org DoStream: status=%d, data=%d bytes", sr.Status, data)
}

// ---------- Conn stats ----------

func TestE2E_Google_ConnStats(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/", true)

	require.NoError(t, err, "Do")
	// BytesReceived should match body length (BodyMode=BodyBuffer → body buffered).
	require.NotZero(t, resp.BytesReceived, "expected BytesReceived > 0")
	assert.EqualValuesf(t, len(resp.Body), resp.BytesReceived,
		"body=%d but BytesReceived=%d — mismatch", len(resp.Body), resp.BytesReceived)
}

// ---------- Auto-redial: close conn, next request redials ----------

func TestE2E_Google_AutoRedial(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First request — lazy dials.
	resp, err := doGET(c, ctx, "/", false)
	require.NoError(t, err, "first request")
	require.GreaterOrEqualf(t, resp.Status, 200, "first request: status %d", resp.Status)
	require.LessOrEqualf(t, resp.Status, 399, "first request: status %d", resp.Status)

	// Close the client's connection by closing and reopening. The singleConn
	// transport auto-redials on the next Do after detecting a dead conn; this
	// simulates it by closing and creating a new client (same behavior).
	c.Close()
	c2 := e2eClient(t, "www.google.com")
	resp2, err := doGET(c2, ctx, "/", false)

	require.NoError(t, err, "redialed request")
	require.GreaterOrEqualf(t, resp2.Status, 200, "redialed request: status %d", resp2.Status)
	require.LessOrEqualf(t, resp2.Status, 399, "redialed request: status %d", resp2.Status)
	assert.NotZero(t, c2.MetricsSnapshot().Counters.DialsAttempted,
		"expected new client to dial at least once")
}

// ---------- Response.Reuse across multiple Do calls ----------

func TestE2E_Google_ResponseReuse(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	for i := range 5 {
		resp.Reset()
		err := c.Do(ctx, &client.Request{
			Method:   "GET",
			Path:     "/",
			Headers:  ua(),
			BodyMode: client.BodyBuffer,
		}, &resp)

		require.NoErrorf(t, err, "request %d", i)
		require.GreaterOrEqualf(t, resp.Status, 200, "request %d: status %d", i, resp.Status)
		require.LessOrEqualf(t, resp.Status, 399, "request %d: status %d", i, resp.Status)
		require.NotEmptyf(t, resp.Body, "request %d: empty body after Reset()", i)
	}
}

// ---------- Multiple status codes ----------

func TestE2E_Google_VariousPaths(t *testing.T) {
	t.Skip(e2eSkipReason)

	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	paths := []struct {
		path    string
		wantMin int
		wantMax int
	}{
		{"/", 200, 399},
		{"/search?q=poseidon", 200, 399},
		{"/robots.txt", 200, 200},
		{"/nonexistent-page-xyz", 400, 404},
	}

	for _, tc := range paths {
		resp, err := doGET(c, ctx, tc.path, true)

		require.NoErrorf(t, err, "GET %s", tc.path)
		assert.GreaterOrEqualf(t, resp.Status, tc.wantMin,
			"GET %s: status %d, want %d-%d", tc.path, resp.Status, tc.wantMin, tc.wantMax)
		assert.LessOrEqualf(t, resp.Status, tc.wantMax,
			"GET %s: status %d, want %d-%d", tc.path, resp.Status, tc.wantMin, tc.wantMax)
	}
}
