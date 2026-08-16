package quic

import (
	"context"
	"net"
	"testing"
	"time"
)

// addrlessPC is a PacketConn with no RemoteAddr method — the shape every
// in-memory test transport has, and the reason Conn.RemoteAddr type-asserts
// rather than widening the PacketConn interface, which would break all of them.
type addrlessPC struct{}

func (addrlessPC) Read([]byte) (int, error)    { return 0, net.ErrClosed }
func (addrlessPC) Write(p []byte) (int, error) { return len(p), nil }
func (addrlessPC) Close() error                { return nil }

// TestConn_RemoteAddr_ServerRole is the case #710 was filed for: a connection
// accepted by a Listener must report the peer it is talking to, because nothing
// above it can find that out otherwise — the shared socket is unconnected and
// Listener.Addr() is the local side.
func TestConn_RemoteAddr_ServerRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	l, pool := startListener(t)
	client, sc := dialListener(t, ctx, l, pool)

	got := sc.RemoteAddr()
	if got == nil {
		t.Fatal("server-role RemoteAddr = nil; an accepted connection knows its peer")
	}
	// The peer of the accepted connection is exactly the client socket's local
	// address. Comparing against it rather than merely asserting non-nil is what
	// makes this fail if the listener ever hands over the wrong view.
	clientLocal := client.pc.(*net.UDPConn).LocalAddr()
	if got.String() != clientLocal.String() {
		t.Fatalf("server-role RemoteAddr = %v, want the client's own address %v", got, clientLocal)
	}
}

// TestConn_RemoteAddr_ClientRole checks the client side is unaffected and useful:
// a *net.UDPConn from net.DialUDP already answers RemoteAddr, so the same
// assertion works without the Listener's per-connection view.
func TestConn_RemoteAddr_ClientRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	l, pool := startListener(t)
	client, _ := dialListener(t, ctx, l, pool)

	got := client.RemoteAddr()
	if got == nil {
		t.Fatal("client-role RemoteAddr = nil; a dialled *net.UDPConn reports its peer")
	}
	if got.String() != l.Addr().String() {
		t.Fatalf("client-role RemoteAddr = %v, want the dialled listener address %v", got, l.Addr())
	}
}

// TestConn_RemoteAddr_TransportWithoutAddr pins the documented fallback. Every
// in-memory transport in this package's tests is addressless, so returning nil
// rather than panicking is what keeps RemoteAddr callable on any Conn.
func TestConn_RemoteAddr_TransportWithoutAddr(t *testing.T) {
	c := &Conn{pc: addrlessPC{}, now: func() time.Time { return time.Unix(0, 0) }}
	if got := c.RemoteAddr(); got != nil {
		t.Fatalf("RemoteAddr = %v, want nil for a transport that cannot report one", got)
	}
}
