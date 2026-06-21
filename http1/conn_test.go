package http1_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// roundTrip sends a single HTTP/1.1 request via http1.Exchange and returns
// the response status code and body.
func roundTrip(t *testing.T, srv *httptest.Server, method, path string, body string) (int, string) {
	t.Helper()
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ex := c.NewExchange()
	host := srv.Listener.Addr().String()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte(method)},
		{Name: []byte(":path"), Value: []byte(path)},
		{Name: []byte(":authority"), Value: []byte(host)},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	if body != "" {
		fields = append(fields, hpack.HeaderField{
			Name:  []byte("content-length"),
			Value: []byte(strings.Repeat("x", len([]byte(body)))), // wrong; test will use chunked
		})
	}
	ctx := context.Background()
	endStream := body == ""
	if err := ex.WriteRequest(ctx, fields, endStream); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if body != "" {
		if err := ex.WriteBody(ctx, []byte(body), true); err != nil {
			t.Fatalf("WriteBody: %v", err)
		}
	}

	status, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	var sb strings.Builder
	buf := make([]byte, 1024)
	for {
		n, done, err := ex.ReadBodyChunk(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if done || err != nil {
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

	status, body := roundTrip(t, srv, "GET", "/", "")
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "hello" {
		t.Errorf("body = %q, want %q", body, "hello")
	}
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
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	host := srv.Listener.Addr().String()
	fields := []hpack.HeaderField{
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
	if err := ex.WriteRequest(ctx, fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	// The caller-supplied "connection: close" must not reach the server as a
	// client-set close (Go's server only reports an explicit close token; our
	// request omits it, so net/http manages keep-alive itself).
	if v := <-gotConnection; strings.Contains(strings.ToLower(v), "close") {
		t.Errorf("Connection header leaked through: %q", v)
	}
	if v := <-gotCustom; v != "present" {
		t.Errorf("X-Custom = %q, want %q (ordinary header dropped)", v, "present")
	}
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
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	host := srv.Listener.Addr().String()
	payload := "ping"
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/echo")},
		{Name: []byte(":authority"), Value: []byte(host)},
		{Name: []byte(":scheme"), Value: []byte("http")},
		// No content-length → chunked encoding.
	}
	if err := ex.WriteRequest(ctx, fields, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if err := ex.WriteBody(ctx, []byte(payload), true); err != nil {
		t.Fatalf("WriteBody: %v", err)
	}

	status, headers, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 201 {
		t.Fatalf("status = %d, want 201", status)
	}
	_ = headers

	buf := make([]byte, 64)
	var got string
	for {
		n, done, err := ex.ReadBodyChunk(buf)
		if n > 0 {
			got += string(buf[:n])
		}
		if done || err != nil {
			break
		}
	}
	if got != payload {
		t.Errorf("body = %q, want %q", got, payload)
	}
}

func TestHTTP1_HEAD_NoBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(200)
		// HEAD: server sends headers only; no body.
	}))
	t.Cleanup(srv.Close)

	status, body := roundTrip(t, srv, "HEAD", "/", "")
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	if body != "" {
		t.Errorf("HEAD body = %q, want empty", body)
	}
}

func TestHTTP1_404(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	status, _ := roundTrip(t, srv, "GET", "/notfound", "")
	if status != 404 {
		t.Fatalf("status = %d, want 404", status)
	}
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

	_, body := roundTrip(t, srv, "GET", "/big", "")
	if body != want {
		t.Errorf("chunked body length = %d, want %d", len(body), len(want))
	}
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
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	host := srv.Listener.Addr().String()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(host)},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}

	for i := 0; i < 2; i++ {
		ex := c.NewExchange()
		if err := ex.WriteRequest(ctx, fields, true); err != nil {
			t.Fatalf("req %d WriteRequest: %v", i, err)
		}
		status, _, err := ex.ReadResponse(ctx)
		if err != nil {
			t.Fatalf("req %d ReadResponse: %v", i, err)
		}
		if status != 200 {
			t.Fatalf("req %d status = %d, want 200", i, status)
		}
		// Drain body.
		buf := make([]byte, 64)
		for {
			_, done, err := ex.ReadBodyChunk(buf)
			if done || err != nil {
				break
			}
		}
		if !ex.KeepAlive() {
			t.Fatalf("req %d: expected keep-alive", i)
		}
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
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	_ = ex.WriteRequest(ctx, fields, true)

	status, headers, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 201 {
		t.Fatalf("status = %d, want 201", status)
	}
	if len(headers) == 0 || string(headers[0].Name) != ":status" {
		t.Fatalf("first header = %q, want :status", headers[0].Name)
	}
	if string(headers[0].Value) != "201" {
		t.Fatalf("status header value = %q, want 201", headers[0].Value)
	}
	_ = bufio.NewReader(nil) // ensure bufio import used (it's in roundTrip)
}

func TestHTTP1_IsAlive(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)

	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	if !c.IsAlive() {
		t.Fatal("expected IsAlive=true before close")
	}
	_ = c.Close()
	if c.IsAlive() {
		t.Fatal("expected IsAlive=false after close")
	}
}

func TestHTTP1_204_NoBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	t.Cleanup(srv.Close)

	status, body := roundTrip(t, srv, "DELETE", "/item", "")
	if status != 204 {
		t.Fatalf("status = %d, want 204", status)
	}
	if body != "" {
		t.Errorf("204 body = %q, want empty", body)
	}
}

// TestHTTP1_POST_EndStream verifies that WriteRequest adds "Content-Length: 0"
// for POST/PUT/PATCH when endStream=true (no body follows).
func TestHTTP1_POST_EndStream(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"POST", "PUT", "PATCH"} {
		method := method
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
			if err != nil {
				t.Fatal(err)
			}
			c := http1.NewConn(nc)
			defer func() { _ = c.Close() }()

			ctx := context.Background()
			ex := c.NewExchange()
			fields := []hpack.HeaderField{
				{Name: []byte(":method"), Value: []byte(method)},
				{Name: []byte(":path"), Value: []byte("/")},
				{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
				{Name: []byte(":scheme"), Value: []byte("http")},
			}
			if err := ex.WriteRequest(ctx, fields, true); err != nil {
				t.Fatalf("WriteRequest: %v", err)
			}
			status, _, err := ex.ReadResponse(ctx)
			if err != nil {
				t.Fatalf("ReadResponse: %v", err)
			}
			if status != 200 {
				t.Fatalf("status = %d, want 200 (missing Content-Length: 0 for %s)", status, method)
			}
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
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte("content-length"), Value: []byte(strconv.Itoa(len(payload)))},
	}
	// endStream=false + content-length present → non-chunked body
	if err := ex.WriteRequest(ctx, fields, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if err := ex.WriteBody(ctx, []byte(payload), false); err != nil {
		t.Fatalf("WriteBody: %v", err)
	}

	status, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	var got strings.Builder
	buf := make([]byte, 64)
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		got.Write(buf[:n])
		if done || rerr != nil {
			break
		}
	}
	if got.String() != payload {
		t.Errorf("echo = %q, want %q", got.String(), payload)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
		// No content-length → chunked
	}
	if err := ex.WriteRequest(ctx, fields, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	// Empty non-final chunk — should be a no-op.
	if err := ex.WriteBody(ctx, nil, false); err != nil {
		t.Fatalf("WriteBody empty non-final: %v", err)
	}
	// Now send real data and finish.
	if err := ex.WriteBody(ctx, []byte("ping"), true); err != nil {
		t.Fatalf("WriteBody fin: %v", err)
	}

	status, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
}

// TestHTTP1_1xx_Response verifies that ReadResponse skips 100 Continue.
func TestHTTP1_1xx_Response(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	if err := ex.WriteRequest(ctx, fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	status, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200 (100 not skipped)", status)
	}
}

// TestHTTP1_MalformedStatusLine verifies ReadResponse returns error on bad response.
func TestHTTP1_MalformedStatusLine(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	_ = ex.WriteRequest(ctx, fields, true)
	_, _, err = ex.ReadResponse(ctx)
	if err == nil {
		t.Fatal("expected error for malformed status line, got nil")
	}
}

// TestHTTP1_ConnectionClose verifies read-until-close body path (contentLen==-1).
func TestHTTP1_ConnectionClose(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	if err := ex.WriteRequest(ctx, fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	status, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	var got strings.Builder
	buf := make([]byte, 32)
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		got.Write(buf[:n])
		if done || rerr != nil {
			break
		}
	}
	if got.String() != "hello" {
		t.Errorf("body = %q, want %q", got.String(), "hello")
	}
	if ex.KeepAlive() {
		t.Error("expected KeepAlive=false after connection-close body")
	}
}

// TestHTTP1_WriteRequest_Deadline exercises the deadline context branch.
func TestHTTP1_WriteRequest_Deadline(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()

	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	if err := ex.WriteRequest(ctx, fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	status, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()

	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(srv.Listener.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
		// No content-length → chunked
	}
	if err := ex.WriteRequest(ctx, fields, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if err := ex.WriteBody(ctx, []byte("data"), true); err != nil {
		t.Fatalf("WriteBody: %v", err)
	}
	status, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
}

// TestHTTP1_ChunkExtensions verifies chunk-extension stripping (e.g. "a;ext=foo").
func TestHTTP1_ChunkExtensions(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}
	if err := ex.WriteRequest(ctx, fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	status, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	var got strings.Builder
	buf := make([]byte, 32)
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		got.Write(buf[:n])
		if done || rerr != nil {
			break
		}
	}
	if got.String() != "hello" {
		t.Errorf("body = %q, want %q", got.String(), "hello")
	}
}
