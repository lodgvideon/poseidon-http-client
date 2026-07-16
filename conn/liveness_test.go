package conn

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// startRealH2Server returns a running HTTP/2 server and a dialed *Conn against
// it. Both are real: a net/http2 server on a real socket, spoken to over TLS.
// Killing the server here kills it for real, which is the whole point — every
// other "dead peer" in this package is a flag on a struct.
func startRealH2Server(t *testing.T) (*httptest.Server, *Conn) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()

	d := &TLSDialer{Config: &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // a test server's self-signed cert
		NextProtos:         []string{"h2"},
	}}
	nc, err := d.Dial(t.Context(), srv.Listener.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatalf("dial: %v", err)
	}
	c, err := NewClientConn(t.Context(), nc, ConnOptions{})
	if err != nil {
		srv.Close()
		t.Fatalf("NewClientConn: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return srv, c
}

// TestConn_IsAlive_FalseAfterPeerDies pins the one thing a pool needs from
// IsAlive: that it stops saying yes once the peer is gone.
//
// The reader owns the transport, so its exit is the connection's death. But
// readerLoop returns without calling Close, and the only listener on readerDone
// is keepaliveLoop — which does not run unless opts.KeepaliveInterval > 0. That
// defaults to 0 and nothing in client/ sets it, so on a default connection
// nothing ever set `closed`, and IsAlive answered true forever. Measured before
// this test existed: a killed server left IsAlive true indefinitely while
// readerDone was already closed. The connection knew, and said otherwise.
//
// Note the deliberate absence of KeepaliveInterval here: conn/ping_test.go
// already covers the keepalive path, and it passes whether or not the default
// path works. This test is the default path.
func TestConn_IsAlive_FalseAfterPeerDies(t *testing.T) {
	srv, c := startRealH2Server(t)

	if !c.IsAlive() {
		t.Fatal("IsAlive() is false against a healthy server; the test proves nothing from here")
	}

	srv.Close()

	// The reader notices within a round trip; poll rather than sleep a fixed
	// span so a slow machine does not turn this into a flake.
	deadline := time.Now().Add(3 * time.Second)
	for c.IsAlive() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-c.readerDone:
	default:
		t.Fatal("readerDone still open after the peer died; this test is measuring the wrong thing")
	}
	if c.IsAlive() {
		t.Error("IsAlive() = true after the peer died and readerDone closed; a pool will hand this connection out forever")
	}
}

// TestConn_IsAlive_HandBuiltConnHasNoReader guards the nil case: tests across
// this package build a bare &Conn{} whose readerDone is nil. A receive on a nil
// channel blocks forever, so the liveness select must fall through to default
// rather than treat "no reader" as "dead".
func TestConn_IsAlive_HandBuiltConnHasNoReader(t *testing.T) {
	c := &Conn{}
	if !c.IsAlive() {
		t.Error("IsAlive() = false for a hand-built Conn with no reader; a nil readerDone must not read as death")
	}
}
