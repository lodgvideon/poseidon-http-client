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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Basic round-trip ────────────────────────────────────────────

func TestIT_GoHTTP_Healthz(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	status, body := doGET(t, c, "/healthz", true)

	require.Equalf(t, 200, status, "status: got %d, want 200", status)
	require.Equalf(t, "ok", string(body), "body: got %q, want %q", body, "ok")
}

func TestIT_GoHTTP_Root(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	status, body := doGET(t, c, "/", true)

	require.Equalf(t, 200, status, "status: got %d, want 200", status)
	require.Containsf(t, string(body), "hello", "body: got %q, want greeting", body)
}

// ── Status codes ────────────────────────────────────────────────

func TestIT_GoHTTP_StatusCodes(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)
	codes := []int{200, 201, 204, 301, 400, 404, 500, 502, 503}

	got := make([]int, 0, len(codes))
	for _, code := range codes {
		status, _ := doGET(t, c, "/status/"+strconv.Itoa(code), false)
		got = append(got, status)
	}

	for i, code := range codes {
		assert.Equalf(t, code, got[i], "status %d: got %d", code, got[i])
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
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoErrorf(t, err, "Do POST /echo: %v", err)
	require.Equalf(t, 200, resp.Status, "status: got %d, want 200", resp.Status)
	require.Truef(t, bytes.Equal(resp.Body, payload),
		"echo body mismatch: got %q, want %q", resp.Body, payload)
}

// ── Large body ──────────────────────────────────────────────────

func TestIT_GoHTTP_LargeBody(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	// 1 MiB — previously hung. With StreamEventBuffer=1024 it should work.
	status, body := doGET(t, c, "/large?bytes=1048576", true)

	require.Equalf(t, 200, status, "status: got %d, want 200", status)
	require.Lenf(t, body, 1048576, "body length: got %d, want 1048576", len(body))
}

func TestIT_GoHTTP_LargeBody_WithinWindow(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)
	// 60 KiB — still within initial 65535-byte window.
	const sz = 60 * 1024

	status, body := doGET(t, c, "/large?bytes="+strconv.Itoa(sz), true)

	require.Equalf(t, 200, status, "status: got %d", status)
	require.Lenf(t, body, sz, "body length: got %d, want %d", len(body), sz)
}

// ── Delay / timeout ─────────────────────────────────────────────

func TestIT_GoHTTP_Delay(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	status, body := doGET(t, c, "/delay?ms=200", true)

	require.Equalf(t, 200, status, "status: got %d, want 200", status)
	require.Containsf(t, string(body), "delayed", "body: got %q, want a delayed marker", body)
}

func TestIT_GoHTTP_ContextCancel(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method: "GET",
		Path:   "/delay?ms=5000",
	}, &resp)

	require.Error(t, err, "expected timeout error, got nil")
}

// ── Multiple sequential requests (reuse) ────────────────────────

func TestIT_GoHTTP_MultipleRequests(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	statuses := make([]int, 0, 20)
	for i := 0; i < 20; i++ {
		status, _ := doGET(t, c, "/healthz", false)
		statuses = append(statuses, status)
	}

	for i, status := range statuses {
		require.Equalf(t, 200, status, "req %d: status %d", i, status)
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
				BodyMode: client.BodyBuffer,
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
		assert.NoError(t, err,
			"one of 50 concurrent requests multiplexed on a single connection failed")
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
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	var foundCT bool
	for _, h := range resp.Headers {
		if strings.EqualFold(string(h.Name), "content-type") {
			foundCT = true
			break
		}
	}
	require.Truef(t, foundCT, "response headers: content-type not found in %v", resp.Headers)
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
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	require.Equalf(t, 200, resp.Status, "status: got %d, want 200", resp.Status)
}

// ── Connection lifecycle ────────────────────────────────────────

func TestIT_GoHTTP_ConnectionReuse(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	// The first request establishes the connection; the second reuses it.
	status1, _ := doGET(t, c, "/healthz", false)
	status2, _ := doGET(t, c, "/healthz", false)

	require.Equalf(t, 200, status1, "first request: status %d", status1)
	require.Equalf(t, 200, status2, "second request: status %d", status2)
}

func TestIT_GoHTTP_ClientClose(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)
	// A successful request first, so the transport really holds a connection
	// when Close runs — otherwise the refusal below could come from a lazy
	// transport that never dialled rather than from the closed flag.
	status, _ := doGET(t, c, "/healthz", false)
	require.Equalf(t, 200, status, "before close: status %d", status)
	require.NoError(t, c.Close(), "Close")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method: "GET",
		Path:   "/healthz",
	}, &resp)

	require.Error(t, err, "Do after Close: expected error, got nil")
}

// ── Chunked / streaming ─────────────────────────────────────────

func TestIT_GoHTTP_ChunkedBody(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	// /chunked sends 100 × 1KB chunks with 10ms delay = ~1s total
	status, body := doGET(t, c, "/chunked", true)

	require.Equalf(t, 200, status, "status: got %d", status)
	require.Lenf(t, body, 100*1024, "body length: got %d, want %d", len(body), 100*1024)
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
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	require.Equalf(t, 200, resp.Status, "status: got %d", resp.Status)
	// BytesReceived must reflect the DATA payload size: a counter that stops
	// short is the metric a load generator reports throughput from.
	require.GreaterOrEqualf(t, resp.BytesReceived, int64(8192),
		"BytesReceived: got %d, want >= 8192", resp.BytesReceived)
}
