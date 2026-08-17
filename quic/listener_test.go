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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startListener starts a Listener with a self-signed certificate and returns it
// with the pool that trusts it.
func startListener(t *testing.T) (*Listener, *x509.CertPool) {
	t.Helper()
	cert, pool := genServerCert(t)
	l, err := Listen("127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}},
		ServerTransportParams{MaxStreamsBidi: 16, MaxStreamsUni: 4})
	require.NoError(t, err, "Listen on the loopback")
	t.Cleanup(func() { _ = l.Close() })
	return l, pool
}

// dialListener dials l with a real client Conn and completes the handshake,
// returning the client and the accepted server connection.
func dialListener(t *testing.T, ctx context.Context, l *Listener, pool *x509.CertPool) (*Conn, *Conn) {
	t.Helper()
	uc, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
	require.NoError(t, err, "dial the listener over UDP")
	t.Cleanup(func() { _ = uc.Close() })

	clientTP := AppendTransportParams(nil, LocalTransportParams{
		InitialMaxData:                1 << 20,
		InitialMaxStreamDataBidiLocal: 1 << 20, // the server's send window for the response
		InitialMaxStreamDataUni:       1 << 20,
		InitialMaxStreamsUni:          4,
	})
	client, err := NewConn(uc, &tls.Config{ServerName: "example.com", RootCAs: pool}, clientTP)
	require.NoError(t, err, "NewConn against the listener")
	require.NoError(t, client.Establish(ctx), "client Establish against listener")
	sc, err := l.Accept(ctx)
	require.NoError(t, err, "Accept the established connection")
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
	require.NoError(t, hs.Start(context.Background()), "client handshake Start")
	sink := newMemSink()
	require.NoError(t, hs.Pump(sink), "client handshake Pump")
	hello := sink.crypto[tls.QUICEncryptionLevelInitial]
	require.NotEmpty(t, hello, "client handshake produced no ClientHello")
	clientKeys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(clientKeys)
	require.NoError(t, err, "NewSealer for the client Initial keys")
	pkt, err := buildInitialPacket(nil, sealer, dcid, nil, nil, 0, 4, 0, hello, InitialDatagramMinSize)
	require.NoError(t, err, "buildInitialPacket carrying the ClientHello")
	return pkt
}

// loopbackPair returns a shared socket (the listener's) and a peer socket
// standing in for a client, both on the loopback.
func loopbackPair(t *testing.T) (shared, peer *net.UDPConn) {
	t.Helper()
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
	shared, err := net.ListenUDP("udp", addr)
	require.NoError(t, err, "listen on the shared (listener) socket")
	t.Cleanup(func() { _ = shared.Close() })
	peer, err = net.ListenUDP("udp", addr)
	require.NoError(t, err, "listen on the peer socket")
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
	require.Truef(t, sc.isServer && sc.oneRTTSealer != nil,
		"accepted connection is not an established server connection (isServer=%v, 1-RTT sealer present=%v)",
		sc.isServer, sc.oneRTTSealer != nil)

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
	require.NoError(t, err, "client OpenStream")

	_, err = reqStream.Send([]byte("GET /"), true)
	require.NoError(t, err, "client Send request")
	require.NoError(t, <-served, "server side of the round trip")
	// Poll the client until the response lands on its stream.
	var got string
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		require.NoError(t, client.Poll(ctx), "client Poll while waiting for the response")
		if got = string(reqStream.Recv()); got != "" {
			break
		}
	}

	assert.Equalf(t, want, got, "client read response %q, want %q", got, want)
	// The server confirms the handshake with HANDSHAKE_DONE (RFC 9001 §4.1.2).
	// Until the client sees it, it holds its Handshake keys and cannot key-update.
	client.mu.Lock()
	confirmed := client.handshakeConfirmed
	client.mu.Unlock()
	assert.True(t, confirmed,
		"client never confirmed the handshake: the server sent no HANDSHAKE_DONE")
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
	routesUp := len(l.conns)
	l.mu.Unlock()

	err := sc.Close()

	require.NoError(t, err, "server Close")
	l.mu.Lock()
	routesAfter := len(l.conns)
	l.mu.Unlock()
	assert.Equalf(t, 1, routesUp, "listener holds %d routes with one connection up, want 1", routesUp)
	assert.Zerof(t, routesAfter,
		"listener still holds %d routes after the connection closed, want 0", routesAfter)
}

// TestListener_CloseIsPromptWithHandshakeInFlight pins Close waking a half-open
// handshake rather than waiting out listenerHandshakeTimeout.
func TestListener_CloseIsPromptWithHandshakeInFlight(t *testing.T) {
	l, pool := startListener(t)
	uc, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
	require.NoError(t, err, "dial the listener over UDP")
	defer func() { _ = uc.Close() }()
	// A genuine ClientHello starts the server handshake; then we go silent, so it
	// parks waiting for a Finished that never arrives.
	_, err = uc.Write(clientHelloInitial(t, pool, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
	require.NoError(t, err, "write the Initial that starts a half-open handshake")
	time.Sleep(300 * time.Millisecond) // let the listener park in completeHandshake

	start := time.Now()
	err = l.Close()
	elapsed := time.Since(start)

	require.NoError(t, err, "Close with a handshake in flight")
	assert.LessOrEqualf(t, elapsed, 3*time.Second,
		"Close took %v with a handshake in flight: it waited out the %v handshake timeout instead of waking it",
		elapsed, listenerHandshakeTimeout)
}

// TestListener_AbandonedHandshakesDoNotLeak pins the crypto/tls handshake
// goroutine being released when a handshake is abandoned. The listener answers
// every Initial without validation, so a peer that never finishes could otherwise
// grow the goroutine count without bound — a remotely triggerable leak.
func TestListener_AbandonedHandshakesDoNotLeak(t *testing.T) {
	l, pool := startListener(t)
	uc, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
	require.NoError(t, err, "dial the listener over UDP")
	defer func() { _ = uc.Close() }()
	runtime.GC()
	base := runtime.NumGoroutine()
	const n = 10

	for i := 0; i < n; i++ {
		dcid := []byte{byte(i), 2, 3, 4, 5, 6, 7, 8} // a distinct connection each time
		_, werr := uc.Write(clientHelloInitial(t, pool, dcid))
		require.NoErrorf(t, werr, "write Initial %d", i)
	}
	time.Sleep(500 * time.Millisecond) // let the handshakes start and park
	err = l.Close()                    // wakes them: every one is abandoned
	settled := false
	for i := 0; i < 50 && !settled; i++ { // let the abandoned handshakes release
		runtime.GC()
		if runtime.NumGoroutine() <= base+2 {
			settled = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.NoError(t, err, "Close with abandoned handshakes parked")
	assert.Truef(t, settled,
		"goroutines did not settle after %d abandoned handshakes: %d now vs %d before — the TLS handshakes leak",
		n, runtime.NumGoroutine(), base)
}

// TestConnPacketConn_WriteReachesPeer checks a write goes out of the shared
// socket addressed to this connection's peer.
func TestConnPacketConn_WriteReachesPeer(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 4)

	buf := make([]byte, 64)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(2*time.Second)), "peer SetReadDeadline")

	_, err := c.Write([]byte("hello"))

	require.NoError(t, err, "Write out of the shared socket")
	n, rerr := peer.Read(buf)
	require.NoError(t, rerr, "peer Read")
	assert.Equalf(t, "hello", string(buf[:n]),
		"peer read %q, want %q — the write must be addressed to this connection's peer",
		string(buf[:n]), "hello")
}

// TestConnPacketConn_DeliverThenRead checks a demuxed datagram is handed to the
// connection's Read.
func TestConnPacketConn_DeliverThenRead(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 4)

	buf := make([]byte, 64)

	c.deliver([]byte("datagram"))
	n, err := c.Read(buf)

	require.NoError(t, err, "Read a demuxed datagram")
	assert.Equalf(t, "datagram", string(buf[:n]), "Read = %q, want %q", string(buf[:n]), "datagram")
}

// TestConnPacketConn_ReadDeadlineExpired returns a timeout when the deadline is
// already in the past.
func TestConnPacketConn_ReadDeadlineExpired(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 4)

	require.NoError(t, c.SetReadDeadline(time.Now().Add(-time.Second)), "SetReadDeadline into the past")

	_, err := c.Read(make([]byte, 64))

	assert.Truef(t, errors.Is(err, os.ErrDeadlineExceeded),
		"Read = %v, want os.ErrDeadlineExceeded", err)
	var ne net.Error
	assert.Truef(t, errors.As(err, &ne) && ne.Timeout(),
		"Read error %v does not report Timeout() — the PTO path classifies on it", err)
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

	require.NoError(t, c.SetReadDeadline(time.Now().Add(-time.Second)), "poke the deadline into the past")

	select {
	case err := <-done:
		assert.Truef(t, errors.Is(err, os.ErrDeadlineExceeded),
			"parked Read = %v, want os.ErrDeadlineExceeded", err)
	case <-time.After(2 * time.Second):
		require.Fail(t, "parked Read did not wake when the deadline was poked into the past")
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

	err := c.Close()

	require.NoError(t, err, "Close with a Read parked")
	select {
	case rerr := <-done:
		assert.Truef(t, errors.Is(rerr, net.ErrClosed),
			"parked Read after Close = %v, want net.ErrClosed", rerr)
	case <-time.After(2 * time.Second):
		require.Fail(t, "Close did not unblock a parked Read")
	}
	assert.NoError(t, c.Close(), "second Close must be idempotent")
}

// TestConnPacketConn_DeliverDropsWhenFull checks the demux loop is never stalled
// by a slow connection: a full queue drops, as a kernel receive buffer would.
func TestConnPacketConn_DeliverDropsWhenFull(t *testing.T) {
	shared, peer := loopbackPair(t)
	c := newConnPacketConn(shared, peer.LocalAddr().(*net.UDPAddr), 1)

	buf := make([]byte, 64)

	c.deliver([]byte("first"))
	c.deliver([]byte("dropped")) // queue is full; must not block
	n, err := c.Read(buf)

	require.NoError(t, err, "Read the one queued datagram")
	assert.Equalf(t, "first", string(buf[:n]), "Read = %q, want %q", string(buf[:n]), "first")
	assert.Emptyf(t, c.in,
		"queue holds %d datagrams, want 0 (the second was dropped, as a kernel receive buffer would)",
		len(c.in))
}
