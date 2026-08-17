package http1_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// recordConn is a net.Conn that counts Close calls, standing in for the
// *tls.Conn a TLS dial hands http1.NewConn.
type recordConn struct {
	net.Conn
	closes int
}

func (c *recordConn) Close() error {
	c.closes++
	return c.Conn.Close()
}

// TestConformance_RFC9112_Sec9_8_CloseDelegatesToUnderlyingConn pins that
// http1.Conn.Close closes the underlying net.Conn rather than unwrapping it to a
// bare TCP socket. On a TLS dial that underlying net.Conn is the *tls.Conn, so
// Close() is (*tls.Conn).Close, which emits a close_notify alert before the FIN
// as RFC 9112 §9.8 requires of clients. Sending the alert is crypto/tls's job;
// this guards the poseidon side of it — that no code path bypasses the TLS layer
// with a raw-socket close.
func TestConformance_RFC9112_Sec9_8_CloseDelegatesToUnderlyingConn(t *testing.T) {
	inner, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	rc := &recordConn{Conn: inner}
	c := http1.NewConn(rc)

	err := c.Close()

	require.NoError(t, err, "Close")
	require.Equalf(t, 1, rc.closes,
		"Close delegated to the underlying conn %d times, want exactly 1 "+
			"(a bare-TCP-close bypass would skip it)", rc.closes)
	require.False(t, c.IsAlive(), "IsAlive() is true after Close()")
}
