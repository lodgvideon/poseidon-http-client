package http1_test

// Reuse safety on the SEND side. The read side established the invariant "any
// error means this connection is not poolable" with a blanket defer in
// ReadResponse and ReadBodyChunk; the write side never had it, and a failed or
// abandoned write leaves the peer counting octets just as surely.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// halfDeadConn writes okBytes octets and then fails every write, while serving
// resp on the read side. It stands in for a socket that dies mid-request: the
// peer has a partial message and this client is about to decide whether to hand
// the connection to the next one.
type halfDeadConn struct {
	net.Conn
	okBytes int
	resp    []byte
	off     int
}

func (c *halfDeadConn) Write(p []byte) (int, error) {
	if c.okBytes <= 0 {
		return 0, errors.New("connection reset by peer")
	}
	n := len(p)
	if n > c.okBytes {
		n = c.okBytes
	}
	c.okBytes -= n
	if n < len(p) {
		return n, errors.New("connection reset by peer")
	}
	return n, nil
}

func (c *halfDeadConn) Read(p []byte) (int, error) {
	if c.off >= len(c.resp) {
		return 0, net.ErrClosed
	}
	n := copy(p, c.resp[c.off:])
	c.off += n
	return n, nil
}

func (c *halfDeadConn) Close() error                     { return nil }
func (c *halfDeadConn) SetReadDeadline(time.Time) error  { return nil }
func (c *halfDeadConn) SetWriteDeadline(time.Time) error { return nil }
func (c *halfDeadConn) SetDeadline(time.Time) error      { return nil }

// TestConformance_RFC9112_Sec9_3_PartialWriteNotPoolable pins that a socket
// write failure condemns the connection.
//
// A partial head is a partial request-line or a half-written field: the peer is
// mid-message and cannot resynchronise. ReadResponse computed
// `keepAlive = respMinor >= 1 && !ex.condemned`, and nothing on the write path
// ever set condemned — so a truncated request followed by a readable response
// came back poolable, and the next request's octets landed on top of the
// unfinished one.
func TestConformance_RFC9112_Sec9_3_PartialWriteNotPoolable(t *testing.T) {
	const resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"

	t.Run("head truncated", func(t *testing.T) {
		nc := &halfDeadConn{okBytes: 12, resp: []byte(resp)}
		ex := http1.NewConn(nc).NewExchange()
		if err := ex.WriteRequest(context.Background(), reqCL("GET"), true); err == nil {
			t.Fatal("WriteRequest = nil on a socket that failed mid-write")
		}
		if _, _, err := ex.ReadResponse(context.Background()); err == nil && ex.KeepAlive() {
			t.Error("KeepAlive() = true after a truncated head; the peer has half a request " +
				"and the next one would be appended to it")
		}
		if ex.KeepAlive() {
			t.Error("KeepAlive() = true after a truncated head")
		}
	})

	t.Run("body truncated", func(t *testing.T) {
		// Enough for the whole head, not enough for the body.
		nc := &halfDeadConn{okBytes: 4096, resp: []byte(resp)}
		c := http1.NewConn(nc)
		ex := c.NewExchange()
		fields := reqCL("POST", hpack.HeaderField{
			Name: []byte("content-length"), Value: []byte("5"),
		})
		if err := ex.WriteRequest(context.Background(), fields, false); err != nil {
			t.Fatalf("WriteRequest: %v", err)
		}
		nc.okBytes = 2 // the body write now fails part-way
		if err := ex.WriteBody(context.Background(), []byte("HELLO"), true); err == nil {
			t.Fatal("WriteBody = nil on a socket that failed mid-write")
		}
		if ex.KeepAlive() {
			t.Error("KeepAlive() = true after a truncated body; the peer is still counting " +
				"octets against the Content-Length this client declared")
		}
	})
}

// TestConformance_RFC9112_Sec9_3_AbandonedUploadNotPoolable pins the case no
// error reports at all: the caller declared a Content-Length, wrote part of it,
// and stopped — a cancelled request, a body source that failed. Every write
// SUCCEEDED, so nothing on the error paths fires.
//
// The under-run check inside WriteBody only runs on the final chunk, which by
// definition never comes here. KeepAlive is where the question belongs: it is
// the caller's decision to stop that makes the body short, and this is the
// moment the caller asks whether the connection survived it.
func TestConformance_RFC9112_Sec9_3_AbandonedUploadNotPoolable(t *testing.T) {
	srv, ex := bodyExchange(t, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	defer srv.Close()

	fields := reqCL("POST", hpack.HeaderField{
		Name: []byte("content-length"), Value: []byte("5"),
	})
	if err := ex.WriteRequest(context.Background(), fields, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	// Three of the five declared octets, then the caller gives up.
	if err := ex.WriteBody(context.Background(), []byte("HEL"), false); err != nil {
		t.Fatalf("WriteBody: %v", err)
	}
	// Reading the response is what makes this test say anything. A server may
	// answer before it has consumed the body — 413, 401, a redirect — and that
	// success is what sets keepAlive true. Without it KeepAlive() is false for
	// the trivial reason that no response has been read, and the assertion below
	// would hold with the under-run guard deleted.
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if ex.KeepAlive() {
		t.Error("KeepAlive() = true with 3 of 5 declared octets written — the peer is " +
			"waiting for two more and would read the next request's line as them")
	}
}

// TestConformance_RFC9112_Sec9_3_CompleteUploadStillPoolable is the control. A
// predicate that simply refused every request carrying a body would pass the
// test above; this fails unless the full-body case is left alone.
func TestConformance_RFC9112_Sec9_3_CompleteUploadStillPoolable(t *testing.T) {
	srv, ex := bodyExchange(t, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	defer srv.Close()

	fields := reqCL("POST", hpack.HeaderField{
		Name: []byte("content-length"), Value: []byte("5"),
	})
	if err := ex.WriteRequest(context.Background(), fields, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if err := ex.WriteBody(context.Background(), []byte("HELLO"), true); err != nil {
		t.Fatalf("WriteBody: %v", err)
	}
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if !ex.KeepAlive() {
		t.Error("KeepAlive() = false after a complete, well-framed exchange — the under-run " +
			"guard is firing on a body that was fully written")
	}
}

// bodyExchange gives an Exchange over a real socket whose peer drains whatever
// is sent and answers with resp. A real socket rather than net.Pipe: the client
// writes a head and a body before reading anything, which an unbuffered
// synchronous pipe deadlocks on.
func bodyExchange(t *testing.T, resp string) (net.Conn, *http1.Exchange) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer nc.Close()
		buf := make([]byte, 4096)
		_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = nc.Read(buf)
		_, _ = nc.Write([]byte(resp))
		_, _ = nc.Read(buf)
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli, http1.NewConn(cli).NewExchange()
}
