package http3

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// udpReadTimeout bounds each datagram read so a stalled or dead server surfaces
// an error instead of blocking a handshake or response forever.
const udpReadTimeout = 10 * time.Second

// udpConn adapts a connected *net.UDPConn to quic.PacketConn. Each Read applies
// a deadline; Write and Read operate on the connected socket, so datagrams only
// flow to and from the dialed server.
type udpConn struct {
	c           *net.UDPConn
	readTimeout time.Duration
}

func (u *udpConn) Read(b []byte) (int, error) {
	if u.readTimeout > 0 {
		_ = u.c.SetReadDeadline(time.Now().Add(u.readTimeout))
	}
	return u.c.Read(b)
}
func (u *udpConn) Write(b []byte) (int, error) { return u.c.Write(b) }
func (u *udpConn) Close() error                { return u.c.Close() }

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
	InitialMaxData:                1 << 20,
	InitialMaxStreamDataBidiLocal: 256 << 10,
	InitialMaxStreamDataUni:       256 << 10,
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
	return dialConn(ctx, &udpConn{c: uc, readTimeout: udpReadTimeout}, h3TLSConfig(tlsConfig))
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
	client, err := NewClient(conn, defaultSettings)
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
	return cfg
}
