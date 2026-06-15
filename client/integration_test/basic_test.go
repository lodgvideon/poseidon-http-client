//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ── Basic round-trip ────────────────────────────────────────────

func TestIT_GoHTTP_Healthz(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	status, body := doGET(t, c, "/healthz", true)
	if status != 200 {
		t.Fatalf("status: got %d, want 200", status)
	}
	if string(body) != "ok" {
		t.Fatalf("body: got %q, want %q", body, "ok")
	}
}

func TestIT_GoHTTP_Root(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	status, body := doGET(t, c, "/", true)
	if status != 200 {
		t.Fatalf("status: got %d, want 200", status)
	}
	if !strings.Contains(string(body), "hello") {
		t.Fatalf("body: got %q, want greeting", body)
	}
}

// ── Status codes ────────────────────────────────────────────────

func TestIT_GoHTTP_StatusCodes(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	codes := []int{200, 201, 204, 301, 400, 404, 500, 502, 503}
	for _, code := range codes {
		status, _ := doGET(t, c, "/status/"+strconv.Itoa(code), false)
		if status != code {
			t.Errorf("status %d: got %d", code, status)
		}
	}
}

// ── Echo (POST body) ────────────────────────────────────────────

func TestIT_GoHTTP_Echo(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	payload := []byte("hello poseidon!")
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
		t.Fatalf("status: got %d, want 200", resp.Status)
	}
	if !bytes.Equal(resp.Body, payload) {
		t.Fatalf("echo body mismatch: got %q, want %q", resp.Body, payload)
	}
}

// ── Large body ──────────────────────────────────────────────────

func TestIT_GoHTTP_LargeBody(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	// 1 MiB — previously hung. With StreamEventBuffer=1024 it should work.
	status, body := doGET(t, c, "/large?bytes=1048576", true)
	if status != 200 {
		t.Fatalf("status: got %d, want 200", status)
	}
	if len(body) != 1048576 {
		t.Fatalf("body length: got %d, want 1048576", len(body))
	}
}

func TestIT_GoHTTP_LargeBody_WithinWindow(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	// 60 KiB — still within initial 65535-byte window.
	const sz = 60 * 1024
	status, body := doGET(t, c, "/large?bytes="+strconv.Itoa(sz), true)
	if status != 200 {
		t.Fatalf("status: got %d", status)
	}
	if len(body) != sz {
		t.Fatalf("body length: got %d, want %d", len(body), sz)
	}
}

// ── Delay / timeout ─────────────────────────────────────────────

func TestIT_GoHTTP_Delay(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	status, body := doGET(t, c, "/delay?ms=200", true)
	if status != 200 {
		t.Fatalf("status: got %d, want 200", status)
	}
	if !strings.Contains(string(body), "delayed") {
		t.Fatalf("body: got %q, want 'delayed'", body)
	}
}

func TestIT_GoHTTP_ContextCancel(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/delay?ms=5000",
		WantBody: false,
	}, &resp)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

// ── Multiple sequential requests (reuse) ────────────────────────

func TestIT_GoHTTP_MultipleRequests(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	for i := 0; i < 20; i++ {
		status, _ := doGET(t, c, "/healthz", false)
		if status != 200 {
			t.Fatalf("req %d: status %d", i, status)
		}
	}
}

// ── Concurrent requests ─────────────────────────────────────────

func TestIT_GoHTTP_Concurrent(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	const N = 50
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
				errs <- err
				return
			}
			if resp.Status != 200 {
				errs <- fmt.Errorf("status %d", resp.Status)
				return
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

// ── Headers ─────────────────────────────────────────────────────

func TestIT_GoHTTP_ResponseHeaders(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
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

	// Verify Content-Type header present
	var foundCT bool
	for _, h := range resp.Headers {
		if strings.EqualFold(string(h.Name), "content-type") {
			foundCT = true
			break
		}
	}
	if !foundCT {
		t.Fatalf("response headers: content-type not found in %v", resp.Headers)
	}
}

func TestIT_GoHTTP_RequestHeaders(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method: "GET",
		Path:   "/healthz",
		Headers: []conn.HeaderField{
			{Name: []byte("x-test-header"), Value: []byte("poseidon-integration")},
		},
		WantBody: false,
	}, &resp)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status: got %d, want 200", resp.Status)
	}
}

// ── Connection lifecycle ────────────────────────────────────────

func TestIT_GoHTTP_ConnectionReuse(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	// First request establishes connection
	status1, _ := doGET(t, c, "/healthz", false)
	if status1 != 200 {
		t.Fatalf("first request: status %d", status1)
	}

	// Second request should reuse the same connection
	status2, _ := doGET(t, c, "/healthz", false)
	if status2 != 200 {
		t.Fatalf("second request: status %d", status2)
	}
}

func TestIT_GoHTTP_ClientClose(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	// Make a successful request first
	status, _ := doGET(t, c, "/healthz", false)
	if status != 200 {
		t.Fatalf("before close: status %d", status)
	}

	// Close the client
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Request after close should fail
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method: "GET",
		Path:   "/healthz",
	}, &resp)
	if err == nil {
		t.Fatal("Do after Close: expected error, got nil")
	}
}

// ── Chunked / streaming ─────────────────────────────────────────

func TestIT_GoHTTP_ChunkedBody(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	// /chunked sends 100 × 1KB chunks with 10ms delay = ~1s total
	status, body := doGET(t, c, "/chunked", true)
	if status != 200 {
		t.Fatalf("status: got %d", status)
	}
	if len(body) != 100*1024 {
		t.Fatalf("body length: got %d, want %d", len(body), 100*1024)
	}
}

// ── Metrics ─────────────────────────────────────────────────────

func TestIT_GoHTTP_Metrics(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/large?bytes=8192",
		WantBody: true,
	}, &resp)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status: got %d", resp.Status)
	}

	// BytesReceived should reflect the DATA payload size
	if resp.BytesReceived < 8192 {
		t.Fatalf("BytesReceived: got %d, want >= 8192", resp.BytesReceived)
	}
}

