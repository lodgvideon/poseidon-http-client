package http1_test

import (
	"net"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// opaqueConn embeds the net.Conn INTERFACE, so it satisfies net.Conn while
// promoting neither SyscallConn nor a NetConn accessor. This is the shape a
// wrapping conn takes in practice — this module's own conn/proxy.go returns one
// after a CONNECT over-read, and ConnOpts.Dialer is a public extension point
// where wrapping is idiomatic.
type opaqueConn struct{ net.Conn }

// TestHasResidue_OpaqueWrapperStillDetects pins that the residue check does not
// go blind on a transport whose socket it cannot reach.
//
// The kernel-queue layer needs a syscall.Conn. When it cannot get one, the
// fallback used to be the past-deadline read — which on a plain socket returns a
// timeout WITHOUT issuing a recv, so the verdict was "clean" no matter what the
// peer had sent. That is a security guard failing OPEN, silently, on a whole
// class of transport, and it disabled RFC 9112 §6.3 protection at every call
// site at once. The fallback now uses a brief future deadline instead: ~1ms
// rather than ~0.5µs, and correct.
func TestHasResidue_OpaqueWrapperStillDetects(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	type accepted struct {
		nc  net.Conn
		err error
	}
	ch := make(chan accepted, 1)
	go func() {
		nc, aerr := ln.Accept()
		ch <- accepted{nc, aerr}
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()
	a := <-ch
	if a.err != nil {
		t.Fatalf("Accept: %v", a.err)
	}
	defer a.nc.Close()

	c := http1.NewConn(&opaqueConn{Conn: cli})
	if c.HasResidue() {
		t.Fatal("HasResidue() = true on a quiet wrapped socket")
	}

	if _, err := a.nc.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if !waitResidue(c, true) {
		t.Error("HasResidue() = false on a wrapped conn with a complete unsolicited " +
			"response waiting — the check is blind on this transport, so every " +
			"connection it guards is reusable no matter what the peer sent")
	}
}
