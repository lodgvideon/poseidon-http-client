package http1_test

import (
	"context"
	"net"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// The Connection field is scoped to the CONNECTION, not to the message carrying
// it (RFC 9110 §7.6.1), and "close" means the sender will close after this
// response (RFC 9112 §9.6). An interim 1xx is still a message on that
// connection, so a "close" on one binds the whole connection.
//
// It used to be dropped: the interim head is drained with the body-framing
// parse disabled, and that gate also covered the Connection field, so the
// option was read off the wire and discarded. The socket then went back to the
// pool. Not a desync — the 1xx is fully drained and nothing is left on the
// reader — but the next request went out on a connection the server had said it
// was closing, racing the FIN.
//
// No test covered a 1xx carrying ANY field before this: both existing ones use a
// bare "HTTP/1.1 100 Continue\r\n\r\n".

// serveOnce answers one connection with the given raw bytes.
func serveOnce(t *testing.T, raw string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_, _ = c.Read(make([]byte, 4096))
		_, _ = c.Write([]byte(raw))
	}()
	return ln
}

// readOnce drives one exchange to completion and reports its poolability.
func readOnce(t *testing.T, ln net.Listener) (status int, keepAlive bool) {
	t.Helper()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c := http1.NewConn(nc)
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	ex := c.NewExchange()
	if werr := ex.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte(ln.Addr().String())},
		{Name: []byte(":scheme"), Value: []byte("http")},
	}, true); werr != nil {
		t.Fatalf("WriteRequest: %v", werr)
	}
	code, _, rerr := ex.ReadResponse(ctx)
	if rerr != nil {
		t.Fatalf("ReadResponse: %v", rerr)
	}
	buf := make([]byte, 256)
	for {
		_, done, berr := ex.ReadBodyChunk(buf)
		if berr != nil {
			t.Fatalf("ReadBodyChunk: %v", berr)
		}
		if done {
			break
		}
	}
	return code, ex.KeepAlive()
}

// TestConformance_RFC9112_Sec9_6_CloseOnInterimBindsTheConnection is the gate.
func TestConformance_RFC9112_Sec9_6_CloseOnInterimBindsTheConnection(t *testing.T) {
	t.Parallel()
	ln := serveOnce(t, "HTTP/1.1 100 Continue\r\nConnection: close\r\n\r\n"+
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")

	status, keepAlive := readOnce(t, ln)
	if status != 200 {
		t.Fatalf("status = %d, want 200 — the interim must still be skipped", status)
	}
	if keepAlive {
		t.Error("the connection is poolable after a 1xx carried Connection: close — the " +
			"server said it is closing, and the next request would race the FIN")
	}
}

// TestConformance_RFC9112_Sec9_6_InterimWithoutCloseStaysPoolable is the control.
// The fix must not condemn every connection that sees an interim response.
func TestConformance_RFC9112_Sec9_6_InterimWithoutCloseStaysPoolable(t *testing.T) {
	t.Parallel()
	ln := serveOnce(t, "HTTP/1.1 100 Continue\r\nX-Note: hello\r\n\r\n"+
		"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")

	status, keepAlive := readOnce(t, ln)
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	if !keepAlive {
		t.Error("an interim response with an ordinary field made the connection " +
			"unpoolable; only Connection: close should")
	}
}
