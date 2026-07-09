package http3

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// udpSocketBuffer is the kernel receive-buffer size requested on the UDP socket
// so a server's send burst is buffered rather than dropped between reads.
const udpSocketBuffer = 4 << 20

// udpConn adapts a connected *net.UDPConn to quic.PacketConn. Read/Write operate
// on the connected socket, so datagrams only flow to and from the dialed server.
// SetReadDeadline lets the QUIC engine bound each read by the probe timeout
// (RFC 9002 §6.2) rather than a fixed interval.
type udpConn struct {
	c *net.UDPConn
}

func (u *udpConn) Read(b []byte) (int, error)        { return u.c.Read(b) }
func (u *udpConn) Write(b []byte) (int, error)       { return u.c.Write(b) }
func (u *udpConn) Close() error                      { return u.c.Close() }
func (u *udpConn) SetReadDeadline(t time.Time) error { return u.c.SetReadDeadline(t) }

// defaultSettings are the HTTP/3 SETTINGS a client advertises: the QPACK dynamic
// table is disabled (static-table-only codec), so its capacity and blocked
// streams are both zero (RFC 9204 §5).
var defaultSettings = []Setting{
	{SettingQPACKMaxTableCapacity, 0},
	{SettingQPACKBlockedStreams, 0},
}

// localTransportParams are the limits the client advertises so the server has
// credit to send the response and open its control/QPACK streams.
var localTransportParams = quic.LocalTransportParams{
	InitialMaxData:                quic.DefaultConnRecvWindow,
	InitialMaxStreamDataBidiLocal: quic.DefaultStreamRecvWindow,
	InitialMaxStreamDataUni:       quic.DefaultStreamRecvWindow,
	InitialMaxStreamsUni:          3,
}

// Dial establishes an HTTP/3 connection to addr ("host:port") over UDP and
// returns a ready Client. tlsConfig must set ServerName; the ALPN token "h3" and
// TLS 1.3 minimum are set automatically. ctx bounds the handshake.
func Dial(ctx context.Context, addr string, tlsConfig *tls.Config) (*Client, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	uc, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}
	// Enlarge the kernel receive buffer so a fast server burst is held rather
	// than dropped while the single-goroutine receive loop drains it one
	// datagram at a time (best-effort).
	_ = uc.SetReadBuffer(udpSocketBuffer)
	return dialConn(ctx, &udpConn{c: uc}, h3TLSConfig(tlsConfig))
}

// dialConn establishes a QUIC connection over pc, wires the client transport
// parameters, and returns a ready HTTP/3 Client. pc is closed on any failure.
func dialConn(ctx context.Context, pc quic.PacketConn, cfg *tls.Config) (*Client, error) {
	tp := quic.AppendTransportParams(nil, localTransportParams)
	conn, err := quic.NewConn(pc, cfg, tp)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	if err := conn.Establish(ctx); err != nil {
		_ = pc.Close()
		return nil, err
	}
	client, err := newClient(ctx, connAdapter{conn}, defaultSettings)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	return client, nil
}

// h3TLSConfig returns a copy of base with the HTTP/3 ALPN token and a TLS 1.3
// minimum applied (RFC 9114 §3.1).
func h3TLSConfig(base *tls.Config) *tls.Config {
	cfg := base.Clone()
	if cfg == nil {
		cfg = &tls.Config{}
	}
	cfg.NextProtos = []string{"h3"}
	if cfg.MinVersion < tls.VersionTLS13 {
		cfg.MinVersion = tls.VersionTLS13
	}
	// Keep the ClientHello inside one Initial packet. Go 1.24 offers the
	// post-quantum X25519MLKEM768 key share by default (~1200 bytes), which
	// pushes the ClientHello past a single ~1200-byte Initial datagram — and
	// this client does not yet split a CRYPTO stream across Initial packets, so
	// the oversized datagram would be fragmented and dropped by the peer.
	if cfg.CurvePreferences == nil {
		cfg.CurvePreferences = []tls.CurveID{tls.X25519, tls.CurveP256}
	}
	return cfg
}
