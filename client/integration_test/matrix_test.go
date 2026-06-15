//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// allReadyServers returns every server that passed healthcheck.
// Tests using this automatically parameterize across implementations.
func allReadyServers(t *testing.T) []*TestServer {
	t.Helper()
	var out []*TestServer
	for k := ServerGoHTTP; k <= ServerUndertow; k++ {
		if srv, ok := allServers[k]; ok && srv.Ready {
			out = append(out, srv)
		}
	}
	if len(out) == 0 {
		t.Skip("no servers ready")
	}
	return out
}

// ── Matrix: basic round-trip ────────────────────────────────────

func TestMatrix_Healthz(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			status, body := doGET(t, c, "/healthz", true)
			if status != 200 {
				t.Fatalf("status: got %d, want 200", status)
			}
			if string(body) != "ok" {
				t.Fatalf("body: got %q, want %q", body, "ok")
			}
		})
	}
}

func TestMatrix_Root(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			status, body := doGET(t, c, "/", true)
			if status != 200 {
				t.Fatalf("status: got %d", status)
			}
			if len(body) == 0 {
				t.Fatal("body: empty")
			}
		})
	}
}

// ── Matrix: status codes ────────────────────────────────────────

func TestMatrix_StatusCodes(t *testing.T) {
	codes := []int{200, 201, 301, 400, 404, 500}
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			for _, code := range codes {
				t.Run(fmt.Sprintf("code_%d", code), func(t *testing.T) {
					status, _ := doGET(t, c, fmt.Sprintf("/status/%d", code), false)
					if status != code {
						t.Fatalf("got %d, want %d", status, code)
					}
				})
			}
		})
	}
}

// ── Matrix: echo (POST) ─────────────────────────────────────────

func TestMatrix_Echo(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			payload := []byte("cross-server echo test")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "POST",
				Path:     "/echo",
				Body:     payload,
				WantBody: true,
			}, &resp)
			if err != nil {
				t.Fatalf("Do POST /echo: %v", err)
			}
			if resp.Status != 200 {
				t.Fatalf("status: got %d", resp.Status)
			}
			if !bytes.Equal(resp.Body, payload) {
				t.Fatalf("body: got %q, want %q", resp.Body, payload)
			}
		})
	}
}

// ── Matrix: connection reuse ────────────────────────────────────

func TestMatrix_ConnectionReuse(t *testing.T) {
	const N = 10
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			for i := 0; i < N; i++ {
				status, _ := doGET(t, c, "/healthz", false)
				if status != 200 {
					t.Fatalf("req %d/%d: status %d", i+1, N, status)
				}
			}
		})
	}
}

// ── Matrix: concurrent requests ─────────────────────────────────

func TestMatrix_Concurrent(t *testing.T) {
	const N = 30
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			var wg sync.WaitGroup
			errs := make(chan error, N)

			wg.Add(N)
			for i := 0; i < N; i++ {
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()

					var resp client.Response
					resp.Reset()
					if err := c.Do(ctx, &client.Request{
						Method:   "GET",
						Path:     "/healthz",
						WantBody: true,
					}, &resp); err != nil {
						errs <- fmt.Errorf("Do: %w", err)
						return
					}
					if resp.Status != 200 {
						errs <- fmt.Errorf("status %d", resp.Status)
					}
				}()
			}
			wg.Wait()
			close(errs)

			for err := range errs {
				t.Error(err)
			}
		})
	}
}

// ── Matrix: chunked streaming ───────────────────────────────────

func TestMatrix_ChunkedBody(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			// 100 × 1KB chunks with 10ms delay = ~1s
			status, body := doGET(t, c, "/chunked", true)
			if status != 200 {
				t.Fatalf("status: got %d", status)
			}
			expected := 100 * 1024
			if len(body) != expected {
				t.Fatalf("body length: got %d, want %d", len(body), expected)
			}
		})
	}
}

// ── Matrix: large body (within window) ──────────────────────────

func TestMatrix_LargeBody_32KB(t *testing.T) {
	const sz = 32 * 1024
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			status, body := doGET(t, c, fmt.Sprintf("/large?bytes=%d", sz), true)
			if status != 200 {
				t.Fatalf("status: got %d", status)
			}
			if len(body) != sz {
				t.Fatalf("body length: got %d, want %d", len(body), sz)
			}
		})
	}
}

// ── Matrix: delay + context cancel ──────────────────────────────

func TestMatrix_Delay(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			status, body := doGET(t, c, "/delay?ms=100", true)
			if status != 200 {
				t.Fatalf("status: got %d", status)
			}
			if !strings.Contains(string(body), "delayed") {
				t.Fatalf("body: got %q", body)
			}
		})
	}
}

func TestMatrix_ContextCancel(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method: "GET",
				Path:   "/delay?ms=5000",
			}, &resp)
			if err == nil {
				t.Fatal("expected timeout error, got nil")
			}
		})
	}
}

// ── Matrix: response headers ────────────────────────────────────

func TestMatrix_ResponseHeaders(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/healthz",
				WantBody: true,
			}, &resp)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			if resp.Status != 200 {
				t.Fatalf("status: got %d", resp.Status)
			}
			// Every server should send at least content-type
			if len(resp.Headers) == 0 {
				t.Fatal("no response headers")
			}
		})
	}
}

// ── Matrix: request headers ─────────────────────────────────────

func TestMatrix_RequestHeaders(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method: "GET",
				Path:   "/healthz",
				Headers: []conn.HeaderField{
					{Name: []byte("x-matrix-test"), Value: []byte("cross-server")},
				},
			}, &resp)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			if resp.Status != 200 {
				t.Fatalf("status: got %d", resp.Status)
			}
		})
	}
}

// ── Matrix: metrics ─────────────────────────────────────────────

func TestMatrix_Metrics(t *testing.T) {
	const sz = 8192
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     fmt.Sprintf("/large?bytes=%d", sz),
				WantBody: true,
			}, &resp)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			if resp.Status != 200 {
				t.Fatalf("status: got %d", resp.Status)
			}
			if resp.BytesReceived < sz {
				t.Fatalf("BytesReceived: got %d, want >= %d", resp.BytesReceived, sz)
			}
		})
	}
}
