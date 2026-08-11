package http1_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// TestConformance_RFC9110_Sec7_8_UnsolicitedUpgradeRejected pins that a 101 is
// refused rather than drained as an interim response.
//
// RFC 9110 §7.8 lets a server switch only to a protocol the client named in
// Upgrade. This client emits no Upgrade at all — WriteRequest strips it — so
// every 101 it can receive names a protocol it never offered. Treating 101 as an
// ordinary 1xx was worse than accepting it: consumeHeaders drained it and the
// loop kept reading the SAME socket for a "final" status line, so a server that
// answered any request with 101 followed by a synthetic response had that
// fabricated response handed to the caller with err == nil, on a connection that
// stayed poolable.
func TestConformance_RFC9110_Sec7_8_UnsolicitedUpgradeRejected(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n"+
			"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED")

	status, _, err := ex.ReadResponse(context.Background())
	if !errors.Is(err, http1.ErrUnsolicitedUpgrade) {
		t.Fatalf("ReadResponse = (%d, %v), want ErrUnsolicitedUpgrade — the fabricated "+
			"200 that follows the 101 must never be returned as this request's response", status, err)
	}
	if ex.KeepAlive() {
		t.Error("KeepAlive() = true after a 101, want false — the peer considers the " +
			"connection to be speaking another protocol")
	}
}

// TestConformance_RFC9112_Sec2_3_VersionMustBeTwoDigits pins that the status
// line's HTTP-version is parsed to the ABNF (`HTTP/1.DIGIT`) rather than by a
// loose prefix test.
//
// "HTTP/1.00" passed a HasPrefix(s, "HTTP/1") accept and then compared unequal to
// the literal "HTTP/1.0", so it seeded a persistent connection and slipped past
// the §6.1 Transfer-Encoding close while still being an HTTP/1.0 message.
func TestConformance_RFC9112_Sec2_3_VersionMustBeTwoDigits(t *testing.T) {
	for _, v := range []string{"HTTP/1.00", "HTTP/1.", "HTTP/1", "HTTP/11", "HTTP/2.0", "HTTP/1.0.1"} {
		t.Run(v, func(t *testing.T) {
			ex := wireExchange(t, "GET", v+" 200 OK\r\nContent-Length: 0\r\n\r\n")
			if _, _, err := ex.ReadResponse(context.Background()); !errors.Is(err, http1.ErrInvalidStatusLine) {
				t.Fatalf("version %q: err = %v, want ErrInvalidStatusLine", v, err)
			}
			if ex.KeepAlive() {
				t.Error("KeepAlive() = true after a malformed status line, want false")
			}
		})
	}
	// Over-rejection guard: every well-formed 1.x version is still accepted, with
	// persistence keyed on the minor.
	for _, tc := range []struct {
		version   string
		keepAlive bool
	}{
		{"HTTP/1.0", false},
		{"HTTP/1.1", true},
		{"HTTP/1.9", true},
	} {
		t.Run("accepted "+tc.version, func(t *testing.T) {
			ex := wireExchange(t, "GET", tc.version+" 200 OK\r\nContent-Length: 0\r\n\r\n")
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("ReadResponse(%s): %v", tc.version, err)
			}
			if _, done, err := ex.ReadBodyChunk(make([]byte, 8)); !done || err != nil {
				t.Fatalf("body should end immediately: done=%v err=%v", done, err)
			}
			if got := ex.KeepAlive(); got != tc.keepAlive {
				t.Errorf("%s: KeepAlive() = %v, want %v", tc.version, got, tc.keepAlive)
			}
		})
	}
}

// TestConformance_RFC9112_Sec6_1_NoChunkedToObservedHttp10Peer pins that once the
// peer has answered HTTP/1.0, the client will not frame a later request body as
// chunked on that connection.
//
// RFC 9112 §6.1: a client MUST NOT send Transfer-Encoding unless it knows the
// server handles HTTP/1.1. Framing was decided with no reference to the peer, and
// a "Connection: keep-alive" makes a 1.0 response poolable — so the client sent
// chunked to a peer it had itself observed as 1.0.
func TestConformance_RFC9112_Sec6_1_NoChunkedToObservedHttp10Peer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			accepted <- nil
			return
		}
		// A 1.0 peer that nevertheless agrees to keep the connection alive.
		_, _ = c.Write([]byte("HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n"))
		accepted <- c
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	srv := <-accepted
	if srv == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = srv.Close() })

	c := http1.NewConn(cli)
	first := c.NewExchange()
	head := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	if err := first.WriteRequest(context.Background(), head, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if _, _, err := first.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if _, done, err := first.ReadBodyChunk(make([]byte, 8)); !done || err != nil {
		t.Fatalf("body should end immediately: done=%v err=%v", done, err)
	}
	if !first.KeepAlive() {
		t.Fatal("the 1.0 + keep-alive response should be poolable; the test needs that shape")
	}

	// Same connection, a bodied request with no Content-Length — which would be
	// framed chunked.
	second := c.NewExchange()
	err = second.WriteRequest(context.Background(), []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, false)
	if !errors.Is(err, http1.ErrInvalidRequest) {
		t.Fatalf("WriteRequest(chunked to an observed HTTP/1.0 peer) = %v, want ErrInvalidRequest", err)
	}

	// Over-rejection guard: the same request with a Content-Length is fine.
	third := c.NewExchange()
	if err := third.WriteRequest(context.Background(), []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte("content-length"), Value: []byte("2")},
	}, false); err != nil {
		t.Fatalf("WriteRequest with Content-Length to a 1.0 peer = %v, want nil", err)
	}
}
