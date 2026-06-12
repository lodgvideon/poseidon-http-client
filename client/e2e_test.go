package client_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

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
	if err != nil {
		t.Fatalf("NewClient(%s): %v", host, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ua() []hpack.HeaderField {
	return []hpack.HeaderField{{Name: []byte("user-agent"), Value: []byte("poseidon-e2e-test/1.0")}}
}

// doGET is a helper: runs GET, returns response. Caller inspects resp fields.
func doGET(c *client.Client, ctx context.Context, path string, wantBody bool) (client.Response, error) {
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     path,
		Headers:  ua(),
		WantBody: wantBody,
	}, &resp)
	return resp, err
}

// ---------- google.com ----------

func TestE2E_Google_GET_Root(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/", true)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}
	if len(resp.Body) == 0 {
		t.Fatal("expected non-empty body")
	}
	if !strings.Contains(string(resp.Body), "google") {
		t.Fatalf("body does not contain 'google'")
	}
	t.Logf("✓ GET https://www.google.com/ → %d, body=%d bytes, headers=%d",
		resp.Status, len(resp.Body), len(resp.Headers))
}

func TestE2E_Google_GET_404(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/this-page-does-not-exist-404-test-poseidon", true)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if resp.Status != 404 {
		t.Fatalf("expected status 404, got %d", resp.Status)
	}
	t.Logf("✓ GET /nonexistent → %d", resp.Status)
}

func TestE2E_Google_HEAD(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	if err := c.Do(ctx, &client.Request{
		Method:   "HEAD",
		Path:     "/",
		Headers:  ua(),
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}
	t.Logf("✓ HEAD https://www.google.com/ → %d", resp.Status)
}

// ---------- Connection reuse: 5 sequential requests ----------

func TestE2E_Google_MultipleRequests_SameConn(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 5 {
		resp, err := doGET(c, ctx, "/", false)
		if err != nil {
			t.Fatalf("request %d: Do: %v", i, err)
		}
		if resp.Status != 200 {
			t.Fatalf("request %d: expected 200, got %d", i, resp.Status)
		}
		t.Logf("  [%d] status=%d bytes_recv=%d", i, resp.Status, resp.BytesReceived)
	}

	snap := c.Metrics().Snapshot()
	t.Logf("✓ 5 sequential requests: started=%d succeeded=%d dials=%d",
		snap.Counters.RequestsStarted, snap.Counters.RequestsSucceeded, snap.Counters.DialsAttempted)

	// All 5 requests should reuse the same connection (1 dial).
	if snap.Counters.DialsAttempted != 1 {
		t.Fatalf("expected exactly 1 dial (conn reuse), got %d", snap.Counters.DialsAttempted)
	}
	if snap.Counters.RequestsStarted < 5 {
		t.Fatalf("expected ≥5 started, got %d", snap.Counters.RequestsStarted)
	}
}

// ---------- Concurrent requests on single connection ----------

func TestE2E_Google_ConcurrentRequests(t *testing.T) {
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
			if resp.Status != 200 && resp.Status != 302 {
				errCh <- fmt.Errorf("unexpected status %d", resp.Status)
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

	snap := c.Metrics().Snapshot()
	t.Logf("✓ %d concurrent requests: started=%d succeeded=%d dials=%d",
		n, snap.Counters.RequestsStarted, snap.Counters.RequestsSucceeded, snap.Counters.DialsAttempted)
}

// ---------- Metrics ----------

func TestE2E_Google_Metrics(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/", true)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	snap := c.MetricsSnapshot()
	t.Logf("✓ metrics: started=%d succeeded=%d errored=%d dials=%d body=%d",
		snap.Counters.RequestsStarted,
		snap.Counters.RequestsSucceeded,
		snap.Counters.RequestsErrored,
		snap.Counters.DialsAttempted,
		len(resp.Body))

	if snap.Counters.RequestsStarted == 0 {
		t.Fatal("expected RequestsStarted > 0")
	}
	if snap.Counters.DialsAttempted == 0 {
		t.Fatal("expected DialsAttempted > 0")
	}
}

// ---------- Headers round-trip ----------

func TestE2E_Google_ResponseHeaders(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/", false)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

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
	if !hasServer {
		t.Fatal("response missing 'server' header")
	}
	if !hasContentType {
		t.Fatal("response missing 'content-type' header")
	}
	t.Logf("✓ response headers present: server=%v content-type=%v total=%d",
		hasServer, hasContentType, len(resp.Headers))
}

// ---------- Large body (google returns ~80KB) ----------

func TestE2E_Google_LargeBody(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := doGET(c, ctx, "/", true)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if resp.Status != 200 {
		t.Fatalf("expected 200, got %d", resp.Status)
	}
	// Google homepage is typically 50-100KB
	if len(resp.Body) < 10000 {
		t.Fatalf("expected large body (>10KB), got %d bytes", len(resp.Body))
	}
	t.Logf("✓ large body: %d bytes received", len(resp.Body))
}

// ---------- Repeated client usage (open/close cycle) ----------

func TestE2E_Google_ClientCloseReopen(t *testing.T) {
	host := "www.google.com"

	for i := range 3 {
		c := e2eClient(t, host)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		resp, err := doGET(c, ctx, "/", false)
		cancel()
		if err != nil {
			t.Fatalf("cycle %d: Do: %v", i, err)
		}
		if resp.Status != 200 {
			t.Fatalf("cycle %d: expected 200, got %d", i, resp.Status)
		}
		c.Close()
		t.Logf("  cycle %d: status=%d", i, resp.Status)
	}
	t.Logf("✓ 3 open/close cycles completed")
}
