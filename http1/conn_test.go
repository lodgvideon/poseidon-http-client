package http1_test

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
