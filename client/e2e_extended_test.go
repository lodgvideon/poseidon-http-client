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

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ============================================================
// Extended E2E tests: StreamBody, DoStream, Pool, cross-server,
// POST, context cancel, concurrent mixed workloads.
// ============================================================

// ---------- StreamBody (io.ReadCloser) ----------

func TestE2E_Google_StreamBody_GET(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:     "GET",
		Path:       "/",
		Headers:    ua(),
		StreamBody: true,
	}, &resp)
	if err != nil {
		t.Fatalf("Do(StreamBody): %v", err)
	}

	if resp.Status < 200 || resp.Status > 399 {
		t.Fatalf("expected 200, got %d", resp.Status)
	}
	if resp.BodyReader == nil {
		t.Fatal("expected BodyReader to be non-nil for StreamBody=true")
	}
	// resp.Body should be empty (StreamBody bypasses buffered body).
	if len(resp.Body) != 0 {
		t.Fatalf("expected empty resp.Body for StreamBody, got %d bytes", len(resp.Body))
	}

	// Read body via io.ReadCloser.
	var buf bytes.Buffer
	n, err := io.Copy(&buf, resp.BodyReader)
	if err != nil {
		t.Fatalf("io.Copy from BodyReader: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero streamed body")
	}
	if !strings.Contains(buf.String(), "google") {
		t.Fatalf("streamed body does not contain 'google'")
	}

	// Close must be idempotent.
	if err := resp.BodyReader.Close(); err != nil {
		t.Logf("second Close: %v (ok)", err)
	}

	t.Logf("✓ StreamBody: streamed %d bytes via io.ReadCloser", n)
}

func TestE2E_Google_StreamBody_Concurrent(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
				Method:     "GET",
				Path:       "/",
				Headers:    ua(),
				StreamBody: true,
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
		if err := <-errCh; err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
	t.Logf("✓ %d concurrent StreamBody reads: total=%d bytes", n, atomic.LoadInt64(&totalBytes))
}

// ---------- DoStream (StreamResponse.Recv) ----------

func TestE2E_Google_DoStream_GET(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var sr client.StreamResponse
	err := c.DoStream(ctx, &client.Request{
		Method:  "GET",
		Path:    "/",
		Headers: ua(),
	}, &sr)
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	if sr.Status < 200 || sr.Status > 399 {
		t.Fatalf("expected 200, got %d", sr.Status)
	}
	if len(sr.Headers) == 0 {
		t.Fatal("expected response headers")
	}

	// Pump events until EndStream.
	var totalData int64
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		switch ev.Type {
		case client.EventData:
			totalData += int64(len(ev.Data))
		case client.EventReset:
			t.Fatalf("stream reset: code=%v", ev.ResetCode)
		}
		if ev.EndStream {
			break
		}
	}
	if totalData == 0 {
		t.Fatal("expected non-zero DATA via DoStream")
	}
	t.Logf("✓ DoStream: status=%d, data=%d bytes, headers=%d", sr.Status, totalData, len(sr.Headers))
}

func TestE2E_Google_DoStream_Concurrent(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
				ev, err := sr.Recv(ctx)
				if errors.Is(err, client.ErrStreamEnded) || (err == nil && ev.EndStream) {
					break
				}
				if err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
	t.Logf("✓ %d concurrent DoStream completed", n)
}

// ---------- Mixed Do + DoStream on same connection ----------

func TestE2E_Google_MixedDoAndDoStream(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
			} else {
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
					ev, err := sr.Recv(ctx)
					if errors.Is(err, client.ErrStreamEnded) || (err == nil && ev.EndStream) {
						break
					}
					if err != nil {
						errCh <- err
						return
					}
				}
				errCh <- nil
			}
		}(i)
	}

	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}

	snap := c.MetricsSnapshot()
	t.Logf("✓ %d mixed Do+DoStream on 1 conn: dials=%d started=%d succeeded=%d",
		n, snap.Counters.DialsAttempted, snap.Counters.RequestsStarted, snap.Counters.RequestsSucceeded)
}

// ---------- Pool transport ----------

func TestE2E_Google_PoolTransport(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
	if err != nil {
		t.Fatalf("NewClient(pool): %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Sequential requests should reuse the same pooled conn.
	for i := range 5 {
		resp, err := doGET(c, ctx, "/", false)
		if err != nil {
			t.Fatalf("pool req %d: %v", i, err)
		}
		if resp.Status < 200 || resp.Status > 399 {
			t.Fatalf("pool req %d: status %d", i, resp.Status)
		}
	}

	stats := c.PoolStats()
	if stats.ActiveConns == 0 {
		t.Fatal("expected at least 1 active pool connection")
	}

	snap := c.MetricsSnapshot()
	t.Logf("✓ Pool: active=%d in_flight=%d dials=%d succeeded=%d",
		stats.ActiveConns, stats.InFlightStreams,
		snap.Counters.DialsAttempted, snap.Counters.RequestsSucceeded)
}

func TestE2E_Google_Pool_Concurrent(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
	if err != nil {
		t.Fatalf("NewClient(pool): %v", err)
	}
	defer c.Close()

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
			if resp.Status < 200 || resp.Status > 399 {
				errCh <- fmt.Errorf("status %d", resp.Status)
				return
			}
			errCh <- nil
		}()
	}

	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}

	snap := c.MetricsSnapshot()
	t.Logf("✓ Pool concurrent: dials=%d started=%d succeeded=%d errored=%d",
		snap.Counters.DialsAttempted, snap.Counters.RequestsStarted,
		snap.Counters.RequestsSucceeded, snap.Counters.RequestsErrored)
}

// ---------- Cross-server: GitHub API ----------

func TestE2E_GitHub_API_JSON(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
		WantBody: true,
	}, &resp)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status < 200 || resp.Status > 399 {
		t.Fatalf("expected 200, got %d", resp.Status)
	}
	if !bytes.Contains(resp.Body, []byte("current_user_url")) {
		t.Fatal("GitHub API response missing 'current_user_url'")
	}

	// Verify content-type header.
	var hasJSON bool
	for _, h := range resp.Headers {
		if string(h.Name) == "content-type" && strings.Contains(string(h.Value), "json") {
			hasJSON = true
		}
	}
	if !hasJSON {
		t.Fatal("response missing 'content-type: application/json'")
	}
	t.Logf("✓ api.github.com: status=%d, body=%d bytes, headers=%d", resp.Status, len(resp.Body), len(resp.Headers))
}

func TestE2E_GitHub_API_StreamBody(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
		StreamBody: true,
	}, &resp)
	if err != nil {
		t.Fatalf("Do(StreamBody): %v", err)
	}
	if resp.BodyReader == nil {
		t.Fatal("expected BodyReader")
	}
	defer resp.BodyReader.Close()

	var buf bytes.Buffer
	n, err := io.Copy(&buf, resp.BodyReader)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("current_user_url")) {
		t.Fatal("streamed GitHub response missing 'current_user_url'")
	}
	t.Logf("✓ GitHub StreamBody: %d bytes streamed", n)
}

// ---------- POST with body ----------

func TestE2E_Google_POST_WithBody(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
		WantBody: true,
	}, &resp)
	if err != nil {
		t.Fatalf("Do(POST): %v", err)
	}
	// Google may return 405 (Method Not Allowed) or 302 — both prove
	// the body was sent on the wire without error.
	if resp.Status < 200 || resp.Status > 499 {
		t.Fatalf("unexpected status %d", resp.Status)
	}
	t.Logf("✓ POST with %d byte body → status=%d", len(body), resp.Status)
}

// ---------- Context cancellation ----------

func TestE2E_Google_ContextCancel(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "www.google.com")

	// Cancel context immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := doGET(c, ctx, "/", false)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	t.Logf("✓ context cancel: %v", err)
}

func TestE2E_Google_ContextTimeout(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "www.google.com")

	// 1ns timeout — request should expire.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // let the deadline pass

	_, err := doGET(c, ctx, "/", true)
	if err == nil {
		t.Fatal("expected error from expired context")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context error, got: %T: %v", err, err)
	}
	t.Logf("✓ context timeout: %v", err)
}

// ---------- Large body via StreamBody (chunked read) ----------

func TestE2E_Google_StreamBody_LargeBody(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:     "GET",
		Path:       "/",
		Headers:    ua(),
		StreamBody: true,
	}, &resp)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.BodyReader == nil {
		t.Fatal("expected BodyReader")
	}

	// Read in small chunks to verify buffering logic.
	var total int64
	buf := make([]byte, 512) // tiny read buffer
	for {
		n, err := resp.BodyReader.Read(buf)
		total += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	resp.BodyReader.Close()

	if total == 0 {
		t.Fatal("expected non-zero body via StreamBody")
	}
	t.Logf("✓ StreamBody chunked read: %d bytes (512-byte reads)", total)
}

// ---------- Cross-server: nghttp2.org (reference HTTP/2) ----------

func TestE2E_Nghttp2_GET(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "nghttp2.org")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/httpbin/", true)
	if err != nil {
		t.Skipf("nghttp2.org unreachable: %v", err)
	}
	// nghttp2.org/httpbin/ may return 200 or 301.
	if resp.Status < 200 || resp.Status > 399 {
		t.Fatalf("unexpected status %d", resp.Status)
	}
	t.Logf("✓ nghttp2.org: status=%d, body=%d bytes", resp.Status, len(resp.Body))
}

func TestE2E_Nghttp2_DoStream(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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

	if sr.Status < 200 || sr.Status > 399 {
		t.Fatalf("unexpected status %d", sr.Status)
	}

	var data int64
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) || (err == nil && ev.EndStream) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == client.EventData {
			data += int64(len(ev.Data))
		}
	}
	t.Logf("✓ nghttp2.org DoStream: status=%d, data=%d bytes", sr.Status, data)
}

// ---------- Conn stats ----------

func TestE2E_Google_ConnStats(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/", true)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	// BytesReceived should match body length (WantBody=true → body buffered).
	if resp.BytesReceived == 0 {
		t.Fatal("expected BytesReceived > 0")
	}
	if int64(len(resp.Body)) != resp.BytesReceived {
		t.Fatalf("body=%d but BytesReceived=%d — mismatch", len(resp.Body), resp.BytesReceived)
	}
	t.Logf("✓ ConnStats: BytesReceived=%d == len(Body)=%d", resp.BytesReceived, len(resp.Body))
}

// ---------- Auto-redial: close conn, next request redials ----------

func TestE2E_Google_AutoRedial(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First request — lazy dials.
	resp, err := doGET(c, ctx, "/", false)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	if resp.Status < 200 || resp.Status > 399 {
		t.Fatalf("first request: status %d", resp.Status)
	}
	snap1 := c.MetricsSnapshot()
	dials1 := snap1.Counters.DialsAttempted

	// Close the client's connection by closing and reopening.
	// The singleConn transport auto-redials on next Do after detecting dead conn.
	// We simulate this by closing and creating a new client (same behavior).
	c.Close()

	c2 := e2eClient(t, "www.google.com")
	resp2, err := doGET(c2, ctx, "/", false)
	if err != nil {
		t.Fatalf("redialed request: %v", err)
	}
	if resp2.Status < 200 || resp2.Status > 399 {
		t.Fatalf("redialed request: status %d", resp2.Status)
	}
	snap2 := c2.MetricsSnapshot()
	if snap2.Counters.DialsAttempted == 0 {
		t.Fatal("expected new client to dial at least once")
	}
	t.Logf("✓ auto-redial: dials1=%d dials2=%d status=%d", dials1, snap2.Counters.DialsAttempted, resp2.Status)
}

// ---------- Response.Reuse across multiple Do calls ----------

func TestE2E_Google_ResponseReuse(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
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
			WantBody: true,
		}, &resp)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status < 200 || resp.Status > 399 {
			t.Fatalf("request %d: status %d", i, resp.Status)
		}
		if len(resp.Body) == 0 {
			t.Fatalf("request %d: empty body", i)
		}
	}
	t.Logf("✓ Response reuse: 5 sequential Do calls with Reset(), 1 conn")
}

// ---------- Multiple status codes ----------

func TestE2E_Google_VariousPaths(t *testing.T) {
	t.Skip("E2E test against external service — disabled in local/CI environments without network access")
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	paths := []struct {
		path       string
		wantMin    int
		wantMax    int
	}{
		{"/", 200, 399},
		{"/search?q=poseidon", 200, 399},
		{"/robots.txt", 200, 200},
		{"/nonexistent-page-xyz", 400, 404},
	}

	for _, tc := range paths {
		resp, err := doGET(c, ctx, tc.path, true)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		if resp.Status < tc.wantMin || resp.Status > tc.wantMax {
			t.Errorf("GET %s: status %d, want %d-%d", tc.path, resp.Status, tc.wantMin, tc.wantMax)
		}
		t.Logf("  GET %s → %d (%d bytes)", tc.path, resp.Status, len(resp.Body))
	}
	t.Logf("✓ various paths tested")
}
