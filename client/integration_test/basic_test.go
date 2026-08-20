//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// TestIT_GoHTTP_MultipleRequests is the many-request half of the reuse pair, and
// like TestIT_GoHTTP_ConnectionReuse below it counts dials rather than statuses:
// twenty successful responses are equally consistent with twenty connections
// (#893).
func TestIT_GoHTTP_MultipleRequests(t *testing.T) {
	const N = 20
	srv := requireServer(t, ServerGoHTTP)
	var dials atomic.Int64
	c := newCountingTestClient(t, srv, &dials)

	statuses := make([]int, 0, N)
	for i := 0; i < N; i++ {
		status, _ := doGET(t, c, "/healthz", false)
		statuses = append(statuses, status)
	}

	for i, status := range statuses {
		require.Equalf(t, 200, status, "req %d: status %d", i, status)
	}
	assert.EqualValuesf(t, 1, dials.Load(),
		"%d sequential requests completed %d dials, want exactly 1.\n"+
			"A transport that reconnects per request answers every one of them with a "+
			"200, so the statuses above cannot tell reuse from a fresh connection each "+
			"time - and reconnecting per request is a TLS handshake per request for a "+
			"load generator", N, dials.Load())
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

// TestIT_GoHTTP_RequestHeaders proves the caller-supplied header reached the
// server, which is the only thing this test name has ever promised.
//
// It used to send the header to /healthz and assert nothing but the status, so a
// client that dropped every Request.Headers entry on the floor kept it green
// (#892). /echo is where the answer lives: CONTRACT.md has every peer return the
// request headers it saw in X-Echo-Headers, and reading that back is a
// single-request equivalence-class case of the property
// TestMatrix_ConcurrentHeaderIdentity only covers under concurrency.
func TestIT_GoHTTP_RequestHeaders(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)
	const name, value = "x-test-header", "poseidon-integration"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/echo",
		BodyMode: client.BodyBuffer,
		Headers: []conn.HeaderField{
			{Name: []byte(name), Value: []byte(value)},
		},
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	require.Equalf(t, 200, resp.Status, "status: got %d, want 200", resp.Status)
	echoed := findHeader(resp.Headers, "x-echo-headers")
	require.NotEmpty(t, echoed, "no X-Echo-Headers on the response - CONTRACT.md "+
		"specifies /echo returns the request headers there, so without it this test "+
		"cannot see whether the header was sent at all")
	assert.Truef(t, containsFold(echoed, name),
		"the server never saw the header name %q; it echoed %q.\n"+
			"A client that silently drops Request.Headers still answers 200 to every "+
			"request, so the status this test used to assert is not evidence the header "+
			"channel works", name, echoed)
	assert.Truef(t, containsFold(echoed, value),
		"the server saw the header name but not the value %q; it echoed %q.\n"+
			"A name carried without its value is the same loss to a caller that sets "+
			"an authorization or a routing header", value, echoed)
}

// TestIT_GoHTTP_Trailers is the trailer section's only consumer in this suite.
//
// /trailers has existed since the fixture contract was written and nothing read
// it (#896), which left client.Request.WantTrailers and drainResponse's
// conn.EventTrailers arm with no integration coverage at all. Both directions of
// that decision are asserted here, because a one-sided test is satisfied by a
// client that always surfaces trailers and equally by one that always drops them.
//
// Go net/http is the only peer in this matrix that emits a real trailer section,
// measured against the live stack: nginx sends X-Trailer-Foo as an ordinary
// response header (its fixture uses add_header), and Undertow - and so nghttpx,
// which proxies it - drops the field it sets after the body entirely. Widening
// this to the matrix would be testing three fixtures rather than the client.
func TestIT_GoHTTP_Trailers(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	c := newTestClient(t, srv)

	t.Run("wanted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var resp client.Response
		resp.Reset()
		err := c.Do(ctx, &client.Request{
			Method:       "GET",
			Path:         "/trailers",
			BodyMode:     client.BodyBuffer,
			WantTrailers: true,
		}, &resp)

		require.NoErrorf(t, err, "Do GET /trailers: %v", err)
		require.Equalf(t, 200, resp.Status, "status: got %d, want 200", resp.Status)
		require.Equalf(t, "trailers", string(resp.Body),
			"body: got %q, want %q - the trailer section must not eat the body", resp.Body, "trailers")
		assert.Equalf(t, "bar", findHeader(resp.Trailers, "x-trailer-foo"),
			"the trailer section did not reach the caller: Trailers=%v.\n"+
				"A trailer HEADERS frame arrives after the end of the body and is the only "+
				"channel a peer has for a checksum or a gRPC status, so a client that "+
				"parses it and then drops it loses the result rather than the metadata",
			resp.Trailers)
		assert.Empty(t, findHeader(resp.Headers, "x-trailer-foo"),
			"x-trailer-foo turned up among the response HEADERS as well as the "+
				"trailers; a trailer folded into the header block is indistinguishable "+
				"from one the peer sent before the body, which is the distinction "+
				"RFC 9110 section 6.5.1 exists to keep")
	})

	t.Run("not wanted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var resp client.Response
		resp.Reset()
		err := c.Do(ctx, &client.Request{
			Method:   "GET",
			Path:     "/trailers",
			BodyMode: client.BodyBuffer,
		}, &resp)

		require.NoErrorf(t, err, "Do GET /trailers: %v", err)
		require.Equalf(t, 200, resp.Status, "status: got %d, want 200", resp.Status)
		assert.Emptyf(t, resp.Trailers,
			"trailers were surfaced although WantTrailers is false: %v.\n"+
				"The default path releases the trailer block back to the pool instead of "+
				"copying it out, so a Response carrying trailers nobody asked for is a "+
				"live reference into storage the next stream will decode into",
			resp.Trailers)
		assert.Equalf(t, "trailers", string(resp.Body),
			"body: got %q, want %q - dropping the trailer section must not cost the body",
			resp.Body, "trailers")
	})
}

// ── Connection lifecycle ────────────────────────────────────────

// TestIT_GoHTTP_ConnectionReuse asserts the second request was served by the
// connection the first one established.
//
// It used to assert only that both requests returned 200, which is exactly as
// true of a transport that dials twice (#893): forcing singleConn.acquireConn to
// skip its cached connection left this test green. The dial count is the
// observation that separates the two.
func TestIT_GoHTTP_ConnectionReuse(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	var dials atomic.Int64
	c := newCountingTestClient(t, srv, &dials)

	// The first request establishes the connection; the second reuses it.
	status1, _ := doGET(t, c, "/healthz", false)
	afterFirst := dials.Load()
	status2, _ := doGET(t, c, "/healthz", false)

	require.Equalf(t, 200, status1, "first request: status %d", status1)
	require.Equalf(t, 200, status2, "second request: status %d", status2)
	require.EqualValuesf(t, 1, afterFirst,
		"the first request completed %d dials, want exactly 1; without one established "+
			"connection there is nothing for the second request to reuse and the "+
			"assertion below would hold vacuously", afterFirst)
	assert.EqualValuesf(t, 1, dials.Load(),
		"the second request brought the dial count to %d, want it still at 1.\n"+
			"It was served by a connection of its own, so this client is not reusing "+
			"anything - and a 200 looks identical either way", dials.Load())
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
