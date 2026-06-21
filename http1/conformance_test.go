package http1_test

// RFC 2616 conformance tests — wire-level fixtures feed the http1 parser
// directly so behaviour is verified against the spec, not just against
// net/http's implementation.
//
// Each test is named TestConformance_RFC2616_Sec<N>_<Behaviour> and adds a
// row to docs/RFC_COVERAGE.md.

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// wireExchange dials a net.Pipe, starts a server goroutine that drains the
// request headers then writes serverResponse verbatim, and returns an
// Exchange ready for ReadResponse. The method used in the synthetic request
// is "GET" unless method is non-empty.
func wireExchange(t *testing.T, method, serverResponse string) *http1.Exchange {
	t.Helper()
	if method == "" {
		method = "GET"
	}
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })

	go func() {
		defer server.Close()
		br := bufio.NewReader(server)
		// Drain request headers (stop at blank line).
		for {
			line, err := br.ReadString('\n')
			if err != nil || strings.TrimRight(line, "\r\n") == "" {
				break
			}
		}
		_, _ = server.Write([]byte(serverResponse))
	}()

	c := http1.NewConn(client)
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte(method)},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	if err := ex.WriteRequest(context.Background(), fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	return ex
}

// TestConformance_RFC2616_Sec4_4_Rule3_ChunkedWinsContentLengthFirst verifies
// that when Content-Length appears before Transfer-Encoding: chunked in the
// response headers, chunked framing takes precedence (RFC 2616 §4.4 Rule 3).
func TestConformance_RFC2616_Sec4_4_Rule3_ChunkedWinsContentLengthFirst(t *testing.T) {
	t.Parallel()
	// Content-Length: 999 must be ignored; only 5 bytes ("hello") are present.
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 999\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" +
		"5\r\nhello\r\n" +
		"0\r\n\r\n"
	ex := wireExchange(t, "GET", resp)
	ctx := context.Background()
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	body := drainBody(t, ex)
	if body != "hello" {
		t.Errorf("body = %q, want %q", body, "hello")
	}
}

// TestConformance_RFC2616_Sec4_4_Rule3_ChunkedWinsTransferEncodingFirst verifies
// that when Transfer-Encoding: chunked appears before Content-Length, chunked
// still wins (RFC 2616 §4.4 Rule 3 — order-independent).
func TestConformance_RFC2616_Sec4_4_Rule3_ChunkedWinsTransferEncodingFirst(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Content-Length: 999\r\n" +
		"\r\n" +
		"5\r\nworld\r\n" +
		"0\r\n\r\n"
	ex := wireExchange(t, "GET", resp)
	ctx := context.Background()
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	body := drainBody(t, ex)
	if body != "world" {
		t.Errorf("body = %q, want %q", body, "world")
	}
}

// TestConformance_RFC2616_Sec8_1_HTTP10DefaultClose verifies that an HTTP/1.0
// response without a Connection header results in KeepAlive() == false.
// RFC 2616 §8.1: HTTP/1.0 does not define persistent connections; the default
// is connection-close.
func TestConformance_RFC2616_Sec8_1_HTTP10DefaultClose(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.0 200 OK\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n"
	ex := wireExchange(t, "GET", resp)
	ctx := context.Background()
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if ex.KeepAlive() {
		t.Error("HTTP/1.0 response without Connection header: KeepAlive() = true, want false")
	}
}

// TestConformance_RFC2616_Sec8_1_HTTP10KeepAliveHeader verifies that an
// HTTP/1.0 response carrying "Connection: keep-alive" opts in to persistence.
func TestConformance_RFC2616_Sec8_1_HTTP10KeepAliveHeader(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.0 200 OK\r\n" +
		"Connection: keep-alive\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n"
	ex := wireExchange(t, "GET", resp)
	ctx := context.Background()
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if !ex.KeepAlive() {
		t.Error("HTTP/1.0 + Connection: keep-alive: KeepAlive() = false, want true")
	}
}

// TestConformance_RFC2616_Sec6_1_HTTP10StatusLineParsed verifies that the
// parser accepts HTTP/1.0 status lines (RFC 2616 §6.1: "HTTP-Version SP
// Status-Code SP Reason-Phrase CRLF").
func TestConformance_RFC2616_Sec6_1_HTTP10StatusLineParsed(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.0 201 Created\r\n" +
		"Content-Length: 2\r\n" +
		"\r\n" +
		"ok"
	ex := wireExchange(t, "GET", resp)
	ctx := context.Background()
	code, _, err := ex.ReadResponse(ctx)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if code != 201 {
		t.Errorf("status = %d, want 201", code)
	}
}

// TestConformance_RFC2616_Sec10_3_5_304NoBody verifies that a 304 Not Modified
// response has no body regardless of any Content-Length header (RFC 2616
// §10.3.5: "The response MUST NOT include a message-body").
func TestConformance_RFC2616_Sec10_3_5_304NoBody(t *testing.T) {
	t.Parallel()
	// Server mistakenly includes Content-Length; client must not try to read body.
	resp := "HTTP/1.1 304 Not Modified\r\n" +
		"Content-Length: 100\r\n" +
		"\r\n"
	ex := wireExchange(t, "GET", resp)
	ctx := context.Background()
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	buf := make([]byte, 16)
	n, done, err := ex.ReadBodyChunk(buf)
	if err != nil {
		t.Fatalf("ReadBodyChunk: %v", err)
	}
	if n != 0 || !done {
		t.Errorf("304: ReadBodyChunk = (%d, %v), want (0, true)", n, done)
	}
}

// TestConformance_RFC2616_Sec3_6_1_MultipleChunks verifies that multi-chunk
// bodies are reassembled correctly across multiple ReadBodyChunk calls
// (RFC 2616 §3.6.1: chunk-size CRLF chunk-data CRLF … terminal-chunk).
func TestConformance_RFC2616_Sec3_6_1_MultipleChunks(t *testing.T) {
	t.Parallel()
	// Three data chunks then the terminal 0-chunk.
	resp := "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" +
		"4\r\nfoo \r\n" +
		"3\r\nbar\r\n" +
		"1\r\n!\r\n" +
		"0\r\n\r\n"
	ex := wireExchange(t, "GET", resp)
	ctx := context.Background()
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	body := drainBody(t, ex)
	if body != "foo bar!" {
		t.Errorf("body = %q, want %q", body, "foo bar!")
	}
}

// TestConformance_RFC2616_Sec3_6_1_EmptyChunkedBody verifies that a response
// with only the terminal zero-chunk produces an empty body (done immediately).
func TestConformance_RFC2616_Sec3_6_1_EmptyChunkedBody(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" +
		"0\r\n\r\n"
	ex := wireExchange(t, "GET", resp)
	ctx := context.Background()
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	buf := make([]byte, 16)
	n, done, err := ex.ReadBodyChunk(buf)
	if err != nil {
		t.Fatalf("ReadBodyChunk: %v", err)
	}
	if n != 0 || !done {
		t.Errorf("empty chunked: ReadBodyChunk = (%d, %v), want (0, true)", n, done)
	}
}

// TestConformance_RFC2616_Sec14_23_HostHeaderInRequest verifies that the
// request wire bytes include a "Host:" header derived from :authority
// (RFC 2616 §14.23: HTTP/1.1 requests MUST include a Host header).
func TestConformance_RFC2616_Sec14_23_HostHeaderInRequest(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer client.Close()

	got := make(chan string, 1)
	go func() {
		defer server.Close()
		br := bufio.NewReader(server)
		var sb strings.Builder
		for {
			line, err := br.ReadString('\n')
			sb.WriteString(line)
			if err != nil || strings.TrimRight(line, "\r\n") == "" {
				break
			}
		}
		got <- sb.String()
		// Write minimal response so client doesn't stall.
		_, _ = server.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	}()

	c := http1.NewConn(client)
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/resource")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	ctx := context.Background()
	if err := ex.WriteRequest(ctx, fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	wire := <-got
	if !strings.Contains(wire, "Host: example.com\r\n") {
		t.Errorf("request wire missing Host header; got:\n%s", wire)
	}
	if !strings.HasPrefix(wire, "GET /resource HTTP/1.1\r\n") {
		t.Errorf("request line wrong; got first line: %q", strings.SplitN(wire, "\n", 2)[0])
	}
}

// drainBody reads the full body from ex and returns it as a string.
func drainBody(t *testing.T, ex *http1.Exchange) string {
	t.Helper()
	buf := make([]byte, 256)
	var sb strings.Builder
	for {
		n, done, err := ex.ReadBodyChunk(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if err != nil {
			t.Fatalf("ReadBodyChunk: %v", err)
		}
		if done {
			return sb.String()
		}
	}
}
