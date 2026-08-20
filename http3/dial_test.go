package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loopbackPair returns an unconnected server socket on the loopback interface
// and the quic.PacketConn adapter wrapping a client socket dialed to it. Both
// are closed when the test ends.
func loopbackPair(t *testing.T) (*net.UDPConn, *udpConn) {
	t.Helper()
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err, "listen on a loopback UDP socket")
	t.Cleanup(func() { _ = server.Close() })

	client, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err, "dial the loopback UDP socket")
	t.Cleanup(func() { _ = client.Close() })

	return server, &udpConn{c: client}
}

// TestUDPConn_Loopback checks the quic.PacketConn adapter moves datagrams both
// ways over a real connected UDP socket.
func TestUDPConn_Loopback(t *testing.T) {
	server, uc := loopbackPair(t)
	buf := make([]byte, 64)
	// Both reads are bounded so a datagram the adapter fails to move surfaces as
	// the assertion below rather than parking until the package timeout.
	require.NoError(t, server.SetReadDeadline(time.Now().Add(5*time.Second)), "bound the server read")
	require.NoError(t, uc.SetReadDeadline(time.Now().Add(5*time.Second)), "bound the client read")

	_, writeErr := uc.Write([]byte("ping"))
	n, caddr, serverReadErr := server.ReadFromUDP(buf)
	gotPing := string(buf[:n])
	_, serverWriteErr := server.WriteToUDP([]byte("pong"), caddr)
	n, readErr := uc.Read(buf)
	gotPong := string(buf[:n])
	closeErr := uc.Close()

	assert.NoError(t, writeErr, "udpConn.Write onto the connected socket")
	assert.NoError(t, serverReadErr, "the server never received the datagram udpConn.Write sent")
	assert.Equalf(t, "ping", gotPing,
		"server received %q, want ping: the adapter must hand the payload to the socket "+
			"unchanged, or every QUIC packet it sends is corrupt", gotPing)
	assert.NoError(t, serverWriteErr, "the server answering on the 4-tuple the client's datagram opened")
	assert.NoError(t, readErr, "udpConn.Read from the connected socket")
	assert.Equalf(t, "pong", gotPong,
		"client received %q, want pong: the adapter must surface the server's payload "+
			"unchanged, or every QUIC packet it receives is corrupt", gotPong)
	assert.NoError(t, closeErr, "Close must release the socket the QUIC engine owns")
}

// TestUDPConn_ReadDeadline verifies a stalled server surfaces a timeout error
// rather than blocking forever.
func TestUDPConn_ReadDeadline(t *testing.T) {
	_, uc := loopbackPair(t)
	require.NoError(t, uc.SetReadDeadline(time.Now().Add(50*time.Millisecond)),
		"arming the read deadline the QUIC engine bounds each read with (RFC 9002 §6.2)")

	_, err := uc.Read(make([]byte, 64))

	require.Error(t, err, "expected a read timeout error: a read that never returns "+
		"parks the QUIC reader goroutine, so no probe timeout can ever fire")
	ne, ok := errAsNet(err)
	require.Truef(t, ok, "err = %v, want a net timeout the engine can classify", err)
	assert.Truef(t, ne.Timeout(), "err = %v, want a net timeout", err)
}

func errAsNet(err error) (net.Error, bool) {
	var ne net.Error
	return ne, errors.As(err, &ne)
}

// TestDial_BadAddress checks Dial reports an error for an unresolvable address
// instead of panicking.
func TestDial_BadAddress(t *testing.T) {
	cfg := &tls.Config{ServerName: "x"}

	_, err := Dial(context.Background(), "not-a-valid-addr", cfg)

	assert.Error(t, err, "expected an error for an invalid address: a caller that gets "+
		"neither an error nor a working connection has nothing to act on")
}

// TestH3TLSConfig checks what h3TLSConfig imposes on the caller's config
// (RFC 9114 §3.1): the "h3" ALPN token and a TLS 1.3 floor, on a nil base as
// well as on one naming a lower minimum, with the caller's ServerName preserved.
func TestH3TLSConfig(t *testing.T) {
	base := &tls.Config{ServerName: "host", MinVersion: tls.VersionTLS10}

	fromNil := h3TLSConfig(nil)
	fromBase := h3TLSConfig(base)

	require.NotNil(t, fromNil, "a nil base must still yield a usable config")
	assert.Equalf(t, []string{"h3"}, fromNil.NextProtos,
		"nil base = %+v: without the h3 ALPN token the peer cannot select HTTP/3", fromNil)
	assert.Equalf(t, uint16(tls.VersionTLS13), fromNil.MinVersion,
		"nil base = %+v: QUIC is defined over TLS 1.3 only (RFC 9001 §4.2)", fromNil)
	assert.Equalf(t, uint16(tls.VersionTLS13), fromBase.MinVersion,
		"MinVersion = %#x, want TLS 1.3: a caller's lower floor must be raised, not honoured",
		fromBase.MinVersion)
	assert.Equalf(t, "host", fromBase.ServerName,
		"config = %+v: the caller's ServerName drives SNI and certificate verification", fromBase)
	assert.Equalf(t, []string{"h3"}, fromBase.NextProtos, "config = %+v", fromBase)
}

// errPC is a quic.PacketConn whose Read always fails, so a handshake over it
// errors out — exercising dialConn's establish-failure and close path.
type errPC struct{ closed bool }

func (p *errPC) Read([]byte) (int, error)    { return 0, errors.New("read failed") }
func (p *errPC) Write(b []byte) (int, error) { return len(b), nil }
func (p *errPC) Close() error                { p.closed = true; return nil }

func TestDialConn_EstablishError(t *testing.T) {
	pc := &errPC{}
	cfg := h3TLSConfig(&tls.Config{ServerName: "example.com"})

	_, err := dialConn(context.Background(), pc, cfg)

	assert.Error(t, err, "expected dialConn to fail when the handshake read errors")
	assert.True(t, pc.closed, "dialConn must close the PacketConn on failure: "+
		"otherwise every failed dial leaks a UDP socket for the process's lifetime")
}

// TestH3TLSConfig_CurvePreferences pins h3TLSConfig's third decision, which
// TestH3TLSConfig says nothing about: the single-Initial curve default is
// installed when the caller named no curves, and a caller who named their own
// keeps them (#795).
//
// Both directions matter, and a one-sided test is satisfied by a function that
// always overwrites. Losing the default is not cosmetic: Go offers the
// post-quantum X25519MLKEM768 key share by default (~1200 bytes), which pushes
// the ClientHello past a single ~1200-byte Initial datagram, and this client does
// not yet split a CRYPTO stream across Initial packets — so every handshake
// against a peer that drops the fragment fails.
func TestH3TLSConfig_CurvePreferences(t *testing.T) {
	singleInitial := []tls.CurveID{tls.X25519, tls.CurveP256}
	mine := []tls.CurveID{tls.CurveP384}
	cases := []struct {
		name string
		base *tls.Config
		want []tls.CurveID
	}{
		{"nil base", nil, singleInitial},
		{"base naming no curves", &tls.Config{ServerName: "h"}, singleInitial},
		{"base naming its own curves", &tls.Config{ServerName: "h", CurvePreferences: mine}, mine},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := h3TLSConfig(tc.base)

			require.NotNil(t, got, "h3TLSConfig must always yield a usable config")
			assert.Equalf(t, tc.want, got.CurvePreferences,
				"CurvePreferences = %v, want %v.\n"+
					"Without the default the ClientHello outgrows one Initial datagram and this "+
					"client cannot split a CRYPTO stream across Initials, so the handshake never "+
					"completes; overwriting a caller's own list silently discards their choice.",
				got.CurvePreferences, tc.want)
		})
	}
}
