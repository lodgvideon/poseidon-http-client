package http1_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "Listen")
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
	require.NoError(t, err, "Dial")
	defer cli.Close()
	a := <-ch
	require.NoError(t, a.err, "Accept")
	defer a.nc.Close()
	c := http1.NewConn(&opaqueConn{Conn: cli})
	require.False(t, c.HasResidue(), "HasResidue() = true on a quiet wrapped socket")

	_, err = a.nc.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"))
	require.NoError(t, err, "peer write")

	assert.True(t, waitResidue(c, true),
		"HasResidue() = false on a wrapped conn with a complete unsolicited "+
			"response waiting — the check is blind on this transport, so every "+
			"connection it guards is reusable no matter what the peer sent")
}
