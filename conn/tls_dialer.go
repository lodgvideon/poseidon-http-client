package conn

import (
	"context"
	"crypto/tls"
	"net"
)

// TLSDialer establishes TLS over TCP and verifies ALPN selected "h2".
// If Config is nil, a default TLS 1.2+ config with NextProtos=["h2"] is used.
type TLSDialer struct {
	Config    *tls.Config
	NetDialer *net.Dialer
}

// Dial implements Dialer.
func (d *TLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	cfg := d.Config
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}}
	}
	nd := d.NetDialer
	if nd == nil {
		nd = &net.Dialer{}
	}
	raw, err := nd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tc := tls.Client(raw, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	if tc.ConnectionState().NegotiatedProtocol != "h2" {
		_ = tc.Close()
		return nil, ErrTLSNoH2
	}
	return tc, nil
}
