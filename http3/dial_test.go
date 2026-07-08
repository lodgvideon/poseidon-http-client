package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"
)

// TestUDPConn_Loopback checks the quic.PacketConn adapter moves datagrams both
// ways over a real connected UDP socket.
func TestUDPConn_Loopback(t *testing.T) {
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	uc := &udpConn{c: client}

	if _, err := uc.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, caddr, err := server.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "ping" {
		t.Fatalf("server received %q, want ping", buf[:n])
	}

	if _, err := server.WriteToUDP([]byte("pong"), caddr); err != nil {
		t.Fatal(err)
	}
	n, err = uc.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "pong" {
		t.Fatalf("client received %q, want pong", buf[:n])
	}
	if err := uc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestUDPConn_ReadDeadline verifies a stalled server surfaces a timeout error
// rather than blocking forever.
func TestUDPConn_ReadDeadline(t *testing.T) {
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	uc := &udpConn{c: client}
	if err := uc.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	if _, err := uc.Read(make([]byte, 64)); err == nil {
		t.Fatal("expected a read timeout error")
	} else if ne, ok := errAsNet(err); !ok || !ne.Timeout() {
		t.Fatalf("err = %v, want a net timeout", err)
	}
}

func errAsNet(err error) (net.Error, bool) {
	var ne net.Error
	return ne, errors.As(err, &ne)
}

// TestDial_BadAddress checks Dial reports an error for an unresolvable address
// instead of panicking.
func TestDial_BadAddress(t *testing.T) {
	if _, err := Dial(context.Background(), "not-a-valid-addr", &tls.Config{ServerName: "x"}); err == nil {
		t.Fatal("expected an error for an invalid address")
	}
}

func TestH3TLSConfig(t *testing.T) {
	// A nil base yields a config with the h3 ALPN and TLS 1.3 floor.
	if got := h3TLSConfig(nil); len(got.NextProtos) != 1 || got.NextProtos[0] != "h3" ||
		got.MinVersion != tls.VersionTLS13 {
		t.Fatalf("nil base = %+v", got)
	}
	// A lower MinVersion is bumped; ServerName is preserved.
	got := h3TLSConfig(&tls.Config{ServerName: "host", MinVersion: tls.VersionTLS10})
	if got.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %#x, want TLS 1.3", got.MinVersion)
	}
	if got.ServerName != "host" || got.NextProtos[0] != "h3" {
		t.Fatalf("config = %+v", got)
	}
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
	if _, err := dialConn(context.Background(), pc, cfg); err == nil {
		t.Fatal("expected dialConn to fail when the handshake read errors")
	}
	if !pc.closed {
		t.Fatal("dialConn must close the PacketConn on failure")
	}
}
