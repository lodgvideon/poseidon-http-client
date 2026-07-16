package http1_test

// Tests for the response read path's deadline handling: the trailer section
// after the terminal chunk, and the read deadline's lifetime across a pooled
// connection.
//
// Both are the same underlying rule this package applies everywhere else — a
// stream whose position is indeterminate must not be pooled (RFC 9112 §11.2 is
// what it buys you if you do) — reached from two directions.

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

func getFields() []hpack.HeaderField {
	return []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
}

// drainReqHead consumes one request head from server.
func drainReqHead(br *bufio.Reader) {
	for {
		l, e := br.ReadString('\n')
		if e != nil || strings.TrimRight(l, "\r\n") == "" {
			return
		}
	}
}

// TestTrailerSectionStall_DoesNotSwallowDeadlineOrPoolConn pins that a trailer
// section that never terminates is an error, not a silent success.
//
// The server sends a complete chunked body and the terminal "0\r\n", then
// stalls past the caller's deadline where the trailer section belongs, then
// plants a whole response. Reporting done=true / err=nil there swallows the
// caller's deadline AND leaves the planted bytes on a poolable socket for the
// next request to read as its status line.
func TestTrailerSectionStall_DoesNotSwallowDeadlineOrPoolConn(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		defer server.Close()
		drainReqHead(bufio.NewReader(server))
		_, _ = server.Write([]byte("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n"))
		time.Sleep(400 * time.Millisecond) // outlast the caller's deadline
		_, _ = server.Write([]byte("HTTP/1.1 401 Unauthorized\r\nContent-Length: 3\r\n\r\nBAD"))
		time.Sleep(200 * time.Millisecond)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	ex := http1.NewConn(client).NewExchange()
	if err := ex.WriteRequest(ctx, getFields(), true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	buf := make([]byte, 64)
	var lastErr error
	for {
		_, done, err := ex.ReadBodyChunk(buf)
		lastErr = err
		if err != nil || done {
			break
		}
	}
	if lastErr == nil {
		t.Error("ReadBodyChunk returned a nil error after the trailer read timed out — " +
			"the caller's deadline was swallowed and the response reported complete")
	}
	if ex.KeepAlive() {
		t.Error("KeepAlive() = true after an unterminated trailer section: the stream " +
			"position is indeterminate and the server's next bytes are still on the " +
			"socket, so the pool would hand them to the next request as a status line")
	}
}

// TestTrailerSectionEOF_StillCompletesButNotPoolable is the other side, and the
// case the old tolerance was actually written for: a server that closes right
// after the terminal chunk. The body IS complete, so the response is good and
// must not become an error — but the socket is gone, so it must not be pooled.
func TestTrailerSectionEOF_StillCompletesButNotPoolable(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if body := drainBody(t, ex); body != "hello" {
		t.Errorf("body = %q, want %q — a server closing after the terminal chunk still "+
			"delivered a complete body; erroring here would fail a good response", body, "hello")
	}
	if ex.KeepAlive() {
		t.Error("KeepAlive() = true after the server closed the connection, want false")
	}
}

// TestReadDeadline_DoesNotLeakToTheNextExchange pins that a read deadline
// belongs to the exchange that set it.
//
// The write path installs its deadline and clears it with a defer; the read path
// cannot, because ReadBodyChunk runs after ReadResponse returns and must stay
// under the same deadline. So the deadline stayed installed on the socket — and
// the socket is pooled. A later request with NO deadline of its own then failed
// with i/o timeout at an instant belonging to a previous request.
func TestReadDeadline_DoesNotLeakToTheNextExchange(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		defer server.Close()
		br := bufio.NewReader(server)
		drainReqHead(br)
		_, _ = server.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
		time.Sleep(300 * time.Millisecond) // let exchange 1's deadline lapse
		drainReqHead(br)
		_, _ = server.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi"))
		time.Sleep(100 * time.Millisecond)
	}()

	c := http1.NewConn(client)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel1()
	ex1 := c.NewExchange()
	if err := ex1.WriteRequest(ctx1, getFields(), true); err != nil {
		t.Fatalf("exchange 1 WriteRequest: %v", err)
	}
	if _, _, err := ex1.ReadResponse(ctx1); err != nil {
		t.Fatalf("exchange 1 ReadResponse: %v", err)
	}
	if body := drainBody(t, ex1); body != "ok" {
		t.Fatalf("exchange 1 body = %q, want %q", body, "ok")
	}

	// Wait past exchange 1's deadline, then run one with none of its own.
	time.Sleep(250 * time.Millisecond)
	ex2 := c.NewExchange()
	if err := ex2.WriteRequest(context.Background(), getFields(), true); err != nil {
		t.Fatalf("exchange 2 WriteRequest: %v", err)
	}
	if _, _, err := ex2.ReadResponse(context.Background()); err != nil {
		t.Errorf("exchange 2 failed with no deadline of its own: %v\n"+
			"exchange 1's read deadline is still installed on the pooled socket", err)
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Log("(confirmed: the failure is a deadline, inherited from exchange 1)")
		}
	}
}
