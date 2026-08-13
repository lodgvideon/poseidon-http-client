package quic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"runtime"
	"testing"
	"time"
)

// startListener starts a Listener with a self-signed certificate and returns it
// with the pool that trusts it.
func startListener(t *testing.T) (*Listener, *x509.CertPool) {
	t.Helper()
	cert, pool := genServerCert(t)
	l, err := Listen("127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}},
		ServerTransportParams{MaxStreamsBidi: 16, MaxStreamsUni: 4})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, pool
}

// dialListener dials l with a real client Conn and completes the handshake,
// returning the client and the accepted server connection.
func dialListener(t *testing.T, ctx context.Context, l *Listener, pool *x509.CertPool) (*Conn, *Conn) {
	t.Helper()
	uc, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = uc.Close() })

	clientTP := AppendTransportParams(nil, LocalTransportParams{
		InitialMaxData:                1 << 20,
		InitialMaxStreamDataBidiLocal: 1 << 20, // the server's send window for the response
		InitialMaxStreamDataUni:       1 << 20,
		InitialMaxStreamsUni:          4,
	})
	client, err := NewConn(uc, &tls.Config{ServerName: "example.com", RootCAs: pool}, clientTP)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	if err := client.Establish(ctx); err != nil {
		t.Fatalf("client Establish against listener: %v", err)
	}
	sc, err := l.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	return client, sc
}

// clientHelloInitial builds a real client Initial carrying a genuine ClientHello
// for dcid, so a listener starts a handshake for it and then waits for a Finished
// the test never sends — a half-open connection.
func clientHelloInitial(t *testing.T, pool *x509.CertPool, dcid []byte) []byte {
	t.Helper()
	tp := AppendTransportParams(nil, LocalTransportParams{
		InitialMaxData:                1 << 20,
		InitialMaxStreamDataBidiLocal: 1 << 20,
		InitialMaxStreamDataUni:       1 << 20,
		InitialMaxStreamsUni:          4,
	})
	hs := newClientHandshake(&tls.Config{ServerName: "example.com", RootCAs: pool}, tp)
	// Release it here, not at test end: only the ClientHello bytes are wanted (the
	// sink copies them), and a lingering handshake would be counted by the
	// goroutine-leak test below.
	defer func() { _ = hs.Close() }()
	if err := hs.Start(context.Background()); err != nil {
		t.Fatalf("client handshake Start: %v", err)
	}
	sink := newMemSink()
	if err := hs.Pump(sink); err != nil {
		t.Fatalf("client handshake Pump: %v", err)
	}
	hello := sink.crypto[tls.QUICEncryptionLevelInitial]
	if len(hello) == 0 {
		t.Fatal("client handshake produced no ClientHello")
	}
	clientKeys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(clientKeys)
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	pkt, err := buildInitialPacket(nil, sealer, dcid, nil, nil, 0, 4, 0, hello, InitialDatagramMinSize)
	if err != nil {
		t.Fatalf("buildInitialPacket: %v", err)
	}
	return pkt
}

// loopbackPair returns a shared socket (the listener's) and a peer socket
// standing in for a client, both on the loopback.
func loopbackPair(t *testing.T) (shared, peer *net.UDPConn) {
	t.Helper()
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
	shared, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("listen shared: %v", err)
	}
	t.Cleanup(func() { _ = shared.Close() })
	peer, err = net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("listen peer: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	return shared, peer
}

// TestListener_AcceptAndRoundTrip runs a real client Conn against a Listener over
// loopback UDP: the listener accepts the client's Initial, drives the handshake,
// and hands back a server Conn; then the two exchange a request and a response
// over 1-RTT, each side driven by its own Poll loop. This is the end-to-end cover
// for the listener, the per-connection socket view, and the server-role Conn.
func TestListener_AcceptAndRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	l, pool := startListener(t)
	client, sc := dialListener(t, ctx, l, pool)
	if !sc.isServer || sc.oneRTTSealer == nil {
		t.Fatal("accepted connection is not an established server connection")
	}

	// The server serves exactly one request: poll until a request stream arrives,
	// read it, answer it.
	const want = "200 OK from listener"
	served := make(chan error, 1)
	go func() {
		for {
			if err := sc.Poll(ctx); err != nil {
				served <- err
				return
			}
			rs := sc.AcceptBidiStream()
			if rs == nil {
				continue
			}
			if got := string(rs.Recv()); got != "GET /" {
				served <- errors.New("server read request " + got + ", want GET /")
				return
			}
			_, err := rs.Send([]byte(want), true)
			served <- err
			return
		}
	}()

	reqStream, err := client.OpenStream()
	if err != nil {
		t.Fatalf("client OpenStream: %v", err)
	}
	if _, err := reqStream.Send([]byte("GET /"), true); err != nil {
		t.Fatalf("client Send request: %v", err)
	}
	if err := <-served; err != nil {
		t.Fatalf("server: %v", err)
	}

	// Poll the client until the response lands on its stream.
	var got string
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if err := client.Poll(ctx); err != nil {
			t.Fatalf("client Poll: %v", err)
		}
		if got = string(reqStream.Recv()); got != "" {
			break
		}
	}
	if got != want {
		t.Fatalf("client read response %q, want %q", got, want)
	}

	// The server confirms the handshake with HANDSHAKE_DONE (RFC 9001 §4.1.2).
	// Until the client sees it, it holds its Handshake keys and cannot key-update.
	client.mu.Lock()
	confirmed := client.handshakeConfirmed
	client.mu.Unlock()
	if !confirmed {
		t.Error("client never confirmed the handshake: the server sent no HANDSHAKE_DONE")
	}
}

// TestListener_DropsRouteWhenConnectionCloses pins a routing table that otherwise
// grew with every connection ever served: a connection that ends must drop its
// route, not linger for the life of the process.
func TestListener_DropsRouteWhenConnectionCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	l, pool := startListener(t)
	_, sc := dialListener(t, ctx, l, pool)

	l.mu.Lock()
	routes := len(l.conns)
	l.mu.Unlock()
	if routes != 1 {
		t.Fatalf("listener holds %d routes with one connection up, want 1", routes)
	}

	if err := sc.Close(); err != nil {
		t.Fatalf("server Close: %v", err)
	}
	l.mu.Lock()
	routes = len(l.conns)
	l.mu.Unlock()
	if routes != 0 {
		t.Fatalf("listener still holds %d routes after the connection closed, want 0", routes)
	}
}

// TestListener_CloseIsPromptWithHandshakeInFlight pins Close waking a half-open
// handshake rather than waiting out listenerHandshakeTimeout.
func TestListener_CloseIsPromptWithHandshakeInFlight(t *testing.T) {
	l, pool := startListener(t)
	uc, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = uc.Close() }()

	// A genuine ClientHello starts the server handshake; then we go silent, so it
	// parks waiting for a Finished that never arrives.
	if _, err := uc.Write(clientHelloInitial(t, pool, []byte{1, 2, 3, 4, 5, 6, 7, 8})); err != nil {
		t.Fatalf("write Initial: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // let the listener park in completeHandshake

	start := time.Now()
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Close took %v with a handshake in flight: it waited out the %v handshake timeout instead of waking it",
			d, listenerHandshakeTimeout)
	}
}

// TestListener_AbandonedHandshakesDoNotLeak pins the crypto/tls handshake
// goroutine being released when a handshake is abandoned. The listener answers
// every Initial without validation, so a peer that never finishes could otherwise
// grow the goroutine count without bound — a remotely triggerable leak.
func TestListener_AbandonedHandshakesDoNotLeak(t *testing.T) {
	l, pool := startListener(t)
	uc, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = uc.Close() }()

	runtime.GC()
	base := runtime.NumGoroutine()

	const n = 10
	for i := 0; i < n; i++ {
		dcid := []byte{byte(i), 2, 3, 4, 5, 6, 7, 8} // a distinct connection each time
		if _, err := uc.Write(clientHelloInitial(t, pool, dcid)); err != nil {
			t.Fatalf("write Initial %d: %v", i, err)
		}
	}
	time.Sleep(500 * time.Millisecond) // let the handshakes start and park
	if err := l.Close(); err != nil {  // wakes them: every one is abandoned
		t.Fatalf("Close: %v", err)
	}

	for i := 0; i < 50; i++ { // let the abandoned handshakes release
		runtime.GC()
		if runtime.NumGoroutine() <= base+2 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("goroutines did not settle after %d abandoned handshakes: %d now vs %d before — the TLS handshakes leak",
		n, runtime.NumGoroutine(), base)
}

// TestConnPacketConn_WriteReachesPeer checks a write goes out of the shared
// socket addressed to this connection's peer.
func TestConnPacketConn_WriteReachesPeer(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 4)

	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	buf := make([]byte, 64)
	if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	n, err := peer.Read(buf)
	if err != nil {
		t.Fatalf("peer Read: %v", err)
	}
	if got := string(buf[:n]); got != "hello" {
		t.Fatalf("peer read %q, want %q", got, "hello")
	}
}

// TestConnPacketConn_DeliverThenRead checks a demuxed datagram is handed to the
// connection's Read.
func TestConnPacketConn_DeliverThenRead(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 4)

	c.deliver([]byte("datagram"))
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := string(buf[:n]); got != "datagram" {
		t.Fatalf("Read = %q, want %q", got, "datagram")
	}
}

// TestConnPacketConn_ReadDeadlineExpired returns a timeout when the deadline is
// already in the past.
func TestConnPacketConn_ReadDeadlineExpired(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 4)

	if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err := c.Read(make([]byte, 64))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read = %v, want os.ErrDeadlineExceeded", err)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("Read error %v does not report Timeout()", err)
	}
}

// TestConnPacketConn_DeadlineWakesParkedRead pins the net.Conn contract the
// Conn's PTO path relies on: poking the deadline into the past must unblock a
// Read that is already parked.
func TestConnPacketConn_DeadlineWakesParkedRead(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 4)

	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 64))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the Read park with no deadline

	if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("parked Read = %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked Read did not wake when the deadline was poked into the past")
	}
}

// TestConnPacketConn_CloseUnblocksRead checks Close wakes a parked Read.
func TestConnPacketConn_CloseUnblocksRead(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 4)

	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 64))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("parked Read after Close = %v, want net.ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock a parked Read")
	}
	if err := c.Close(); err != nil { // idempotent
		t.Fatalf("second Close: %v", err)
	}
}

// TestConnPacketConn_DeliverDropsWhenFull checks the demux loop is never stalled
// by a slow connection: a full queue drops, as a kernel receive buffer would.
func TestConnPacketConn_DeliverDropsWhenFull(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 1)

	c.deliver([]byte("first"))
	c.deliver([]byte("dropped")) // queue is full; must not block
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got := string(buf[:n]); got != "first" {
		t.Fatalf("Read = %q, want %q", got, "first")
	}
	if len(c.in) != 0 {
		t.Fatalf("queue holds %d datagrams, want 0 (the second was dropped)", len(c.in))
	}
}
