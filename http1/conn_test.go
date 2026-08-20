package http1_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// roundTrip sends a single bodyless HTTP/1.1 request via http1.Exchange and
// returns the response status code and body.
func roundTrip(t *testing.T, srv *httptest.Server, method, path string) (int, string) {
	t.Helper()
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })

	ex := c.NewExchange()
	host := srv.Listener.Addr().String()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte(method)},
		{Name: []byte(":path"), Value: []byte(path)},
		{Name: []byte(":authority"), Value: []byte(host)},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	ctx := context.Background()
	require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")

	status, _, err := ex.ReadResponse(ctx)
	require.NoError(t, err, "ReadResponse")
	var sb strings.Builder
	buf := make([]byte, 1024)
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if done || rerr != nil {
			break
		}
	}
	return status, sb.String()
}

func TestHTTP1_GET_200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("hello"))
	}))
	t.Cleanup(srv.Close)

	status, body := roundTrip(t, srv, "GET", "/")

	require.Equalf(t, 200, status, "status = %d, want 200", status)
	assert.Equalf(t, "hello", body, "body = %q, want %q", body, "hello")
}

// TestHTTP1_WriteRequest_SkipsHopByHopHeaders verifies that hop-by-hop and
// H2-forbidden headers (connection, te, keep-alive, etc.) supplied by the
// caller are dropped from the H1.1 wire request, while ordinary headers pass
// through. Covers the forbidden-header skip branch in WriteRequest.
func TestHTTP1_WriteRequest_SkipsHopByHopHeaders(t *testing.T) {
	t.Parallel()
	gotConnection := make(chan string, 1)
	gotCustom := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotConnection <- r.Header.Get("Connection")
		gotCustom <- r.Header.Get("X-Custom")
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	host := srv.Listener.Addr().String()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(host)},
		{Name: []byte(":scheme"), Value: []byte("http")},
		// Forbidden / hop-by-hop — must be skipped by WriteRequest.
		{Name: []byte("connection"), Value: []byte("close")},
		{Name: []byte("te"), Value: []byte("trailers")},
		{Name: []byte("keep-alive"), Value: []byte("timeout=5")},
		// Ordinary header — must pass through.
		{Name: []byte("x-custom"), Value: []byte("present")},
	}

	require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")

	_, _, err = ex.ReadResponse(ctx)
	require.NoError(t, err, "ReadResponse")
	// The caller-supplied "connection: close" must not reach the server as a
	// client-set close (Go's server only reports an explicit close token; our
	// request omits it, so net/http manages keep-alive itself).
	conn := <-gotConnection
	assert.NotContainsf(t, strings.ToLower(conn), "close", "Connection header leaked through: %q", conn)
	custom := <-gotCustom
	assert.Equalf(t, "present", custom, "X-Custom = %q, want %q (ordinary header dropped)", custom, "present")
}

func TestHTTP1_POST_Echo(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		w.WriteHeader(201)
		_, _ = w.Write(buf[:n])
	}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	host := srv.Listener.Addr().String()
	payload := "ping"
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/echo")},
		{Name: []byte(":authority"), Value: []byte(host)},
		{Name: []byte(":scheme"), Value: []byte("http")},
		// No content-length → chunked encoding.
	}
	require.NoError(t, ex.WriteRequest(ctx, fields, false), "WriteRequest")
	require.NoError(t, ex.WriteBody(ctx, []byte(payload), true), "WriteBody")

	status, _, err := ex.ReadResponse(ctx)

	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 201, status, "status = %d, want 201", status)
	buf := make([]byte, 64)
	var got string
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		if n > 0 {
			got += string(buf[:n])
		}
		if done || rerr != nil {
			break
		}
	}
	assert.Equalf(t, payload, got, "body = %q, want %q", got, payload)
}

func TestHTTP1_HEAD_NoBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		// HEAD: server sends headers only; no body.
	}))
	t.Cleanup(srv.Close)

	status, body := roundTrip(t, srv, "HEAD", "/")

	require.Equalf(t, 200, status, "status = %d, want 200", status)
	assert.Emptyf(t, body, "HEAD body = %q, want empty", body)
}

func TestHTTP1_404(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	status, _ := roundTrip(t, srv, "GET", "/notfound")

	require.Equalf(t, 404, status, "status = %d, want 404", status)
}

func TestHTTP1_Chunked_Response(t *testing.T) {
	t.Parallel()
	want := strings.Repeat("chunk", 100)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Force chunked by flushing in pieces.
		f := w.(http.Flusher)
		for i := 0; i < 100; i++ {
			_, _ = w.Write([]byte("chunk"))
			f.Flush()
		}
	}))
	t.Cleanup(srv.Close)

	_, body := roundTrip(t, srv, "GET", "/big")

	assert.Equalf(t, want, body, "chunked body length = %d, want %d", len(body), len(want))
}

func TestHTTP1_KeepAlive_TwoRequests(t *testing.T) {
	t.Parallel()
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	host := srv.Listener.Addr().String()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(host)},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}

	for i := 0; i < 2; i++ {
		ex := c.NewExchange()
		require.NoErrorf(t, ex.WriteRequest(ctx, fields, true), "req %d WriteRequest", i)
		status, _, rerr := ex.ReadResponse(ctx)
		require.NoErrorf(t, rerr, "req %d ReadResponse", i)
		require.Equalf(t, 200, status, "req %d status = %d, want 200", i, status)
		// Drain body.
		buf := make([]byte, 64)
		for {
			_, done, derr := ex.ReadBodyChunk(buf)
			if done || derr != nil {
				break
			}
		}
		require.Truef(t, ex.KeepAlive(), "req %d: expected keep-alive", i)
	}
}

// TestHTTP1_ParseStatus checks that :status is first in returned headers.
func TestHTTP1_ParseStatus(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "value")
		w.WriteHeader(201)
	}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")

	status, headers, err := ex.ReadResponse(ctx)

	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 201, status, "status = %d, want 201", status)
	require.NotEmpty(t, headers, "no headers returned; want :status first")
	require.Equalf(t, ":status", string(headers[0].Name),
		"first header = %q, want :status", headers[0].Name)
	require.Equalf(t, "201", string(headers[0].Value),
		"status header value = %q, want 201", headers[0].Value)
}

func TestHTTP1_IsAlive(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)

	aliveBefore := c.IsAlive()
	_ = c.Close()

	require.True(t, aliveBefore, "expected IsAlive=true before close")
	require.False(t, c.IsAlive(), "expected IsAlive=false after close")
}

func TestHTTP1_204_NoBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	t.Cleanup(srv.Close)

	status, body := roundTrip(t, srv, "DELETE", "/item")

	require.Equalf(t, 204, status, "status = %d, want 204", status)
	assert.Emptyf(t, body, "204 body = %q, want empty", body)
}

// TestHTTP1_POST_EndStream verifies that WriteRequest adds "Content-Length: 0"
// for POST/PUT/PATCH when endStream=true (no body follows).
func TestHTTP1_POST_EndStream(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"POST", "PUT", "PATCH"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.ContentLength != 0 {
					w.WriteHeader(400)
					_, _ = fmt.Fprintf(w, "bad content-length: %d", r.ContentLength)
					return
				}
				w.WriteHeader(200)
			}))
			t.Cleanup(srv.Close)
			nc, err := net.Dial("tcp", srv.Listener.Addr().String())
			require.NoError(t, err, "dial")
			c := http1.NewConn(nc)
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()
			ex := c.NewExchange()
			fields := []header.Field{
				{Name: []byte(":method"), Value: []byte(method)},
				{Name: []byte(":path"), Value: []byte("/")},
				{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
				{Name: []byte(":scheme"), Value: []byte("http")},
			}
			require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")

			status, _, err := ex.ReadResponse(ctx)

			require.NoError(t, err, "ReadResponse")
			require.Equalf(t, 200, status,
				"status = %d, want 200 (missing Content-Length: 0 for %s)", status, method)
		})
	}
}

// TestHTTP1_WriteBody_NonChunked sends a body using Content-Length (not chunked).
func TestHTTP1_WriteBody_NonChunked(t *testing.T) {
	t.Parallel()
	payload := "hello"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64)
		n, _ := r.Body.Read(buf)
		_, _ = w.Write(buf[:n])
	}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte("content-length"), Value: []byte(strconv.Itoa(len(payload)))},
	}
	// endStream=false + content-length present → non-chunked body
	require.NoError(t, ex.WriteRequest(ctx, fields, false), "WriteRequest")

	require.NoError(t, ex.WriteBody(ctx, []byte(payload), false), "WriteBody")

	status, _, err := ex.ReadResponse(ctx)
	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 200, status, "status = %d, want 200", status)
	var got strings.Builder
	buf := make([]byte, 64)
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		got.Write(buf[:n])
		if done || rerr != nil {
			break
		}
	}
	assert.Equalf(t, payload, got.String(), "echo = %q, want %q", got.String(), payload)
}

// TestHTTP1_WriteBody_EmptyChunkNonFinal verifies WriteBody is a no-op when
// len(p)==0 and fin==false on a chunked exchange.
func TestHTTP1_WriteBody_EmptyChunkNonFinal(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64)
		n, _ := r.Body.Read(buf)
		_, _ = w.Write(buf[:n])
	}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
		// No content-length → chunked
	}
	require.NoError(t, ex.WriteRequest(ctx, fields, false), "WriteRequest")

	// Empty non-final chunk — should be a no-op.
	err = ex.WriteBody(ctx, nil, false)

	require.NoError(t, err, "WriteBody empty non-final")
	// Now send real data and finish.
	require.NoError(t, ex.WriteBody(ctx, []byte("ping"), true), "WriteBody fin")
	status, _, err := ex.ReadResponse(ctx)
	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 200, status, "status = %d, want 200", status)
}

// TestHTTP1_1xx_Response verifies that ReadResponse skips 100 Continue.
func TestHTTP1_1xx_Response(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Read(make([]byte, 4096)) // drain request
		_, _ = conn.Write([]byte(
			"HTTP/1.1 100 Continue\r\n\r\n" +
				"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
		))
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")

	status, _, err := ex.ReadResponse(ctx)

	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 200, status, "status = %d, want 200 (100 not skipped)", status)
}

// TestHTTP1_MalformedStatusLine verifies ReadResponse returns error on bad response.
func TestHTTP1_MalformedStatusLine(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Read(make([]byte, 4096))
		_, _ = conn.Write([]byte("BOGUS not-http\r\n\r\n"))
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")

	_, _, err = ex.ReadResponse(ctx)

	require.Error(t, err, "expected error for malformed status line, got nil")
}

// TestHTTP1_ConnectionClose verifies read-until-close body path (contentLen==-1).
func TestHTTP1_ConnectionClose(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		_, _ = conn.Read(make([]byte, 4096))
		// HTTP/1.0 response: no Content-Length, connection closes when done.
		_, _ = conn.Write([]byte(
			"HTTP/1.0 200 OK\r\nConnection: close\r\n\r\nhello",
		))
		_ = conn.Close()
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")

	status, _, err := ex.ReadResponse(ctx)

	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 200, status, "status = %d, want 200", status)
	var got strings.Builder
	buf := make([]byte, 32)
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		got.Write(buf[:n])
		if done || rerr != nil {
			break
		}
	}
	assert.Equalf(t, "hello", got.String(), "body = %q, want %q", got.String(), "hello")
	assert.False(t, ex.KeepAlive(), "expected KeepAlive=false after connection-close body")
}

// TestHTTP1_WriteRequest_Deadline exercises the deadline context branch.
func TestHTTP1_WriteRequest_Deadline(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}

	err = ex.WriteRequest(ctx, fields, true)

	require.NoError(t, err, "WriteRequest")
	status, _, err := ex.ReadResponse(ctx)
	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 200, status, "status = %d, want 200", status)
}

// TestHTTP1_WriteBody_Deadline exercises the deadline context branch in WriteBody.
func TestHTTP1_WriteBody_Deadline(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64)
		n, _ := r.Body.Read(buf)
		_, _ = w.Write(buf[:n])
	}))
	t.Cleanup(srv.Close)
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
		// No content-length → chunked
	}
	require.NoError(t, ex.WriteRequest(ctx, fields, false), "WriteRequest")

	err = ex.WriteBody(ctx, []byte("data"), true)

	require.NoError(t, err, "WriteBody")
	status, _, err := ex.ReadResponse(ctx)
	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 200, status, "status = %d, want 200", status)
}

// TestHTTP1_ChunkExtensions verifies chunk-extension stripping (e.g. "a;ext=foo").
func TestHTTP1_ChunkExtensions(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Read(make([]byte, 4096))
		// Chunked response with extensions.
		_, _ = conn.Write([]byte(
			"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
				"5;ext=ignored\r\nhello\r\n" +
				"0\r\n\r\n",
		))
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err, "dial")
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	ex := c.NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")

	status, _, err := ex.ReadResponse(ctx)

	require.NoError(t, err, "ReadResponse")
	require.Equalf(t, 200, status, "status = %d, want 200", status)
	var got strings.Builder
	buf := make([]byte, 32)
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		got.Write(buf[:n])
		if done || rerr != nil {
			break
		}
	}
	assert.Equalf(t, "hello", got.String(), "body = %q, want %q", got.String(), "hello")
}
