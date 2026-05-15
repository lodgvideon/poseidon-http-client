package conn

import (
	"context"
	"crypto/tls"
	"net"
)

// Dialer abstracts how the underlying transport is established.
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// TLSDialer dials addr over TCP, runs TLS, and asserts ALPN h2.
// If Config is nil a defaulted *tls.Config with NextProtos=[h2] is used.
type TLSDialer struct {
	Config *tls.Config
}

// Dial dials addr over TCP, runs the TLS handshake with NextProtos
// containing "h2", and returns the negotiated *tls.Conn. Returns
// ErrALPNFailed if the peer did not select "h2".
func (d *TLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	cfg := d.Config
	if cfg == nil {
		cfg = &tls.Config{}
	} else {
		cfg = cfg.Clone()
	}
	hasH2 := false
	for _, p := range cfg.NextProtos {
		if p == "h2" {
			hasH2 = true
			break
		}
	}
	if !hasH2 {
		cfg.NextProtos = append([]string{"h2"}, cfg.NextProtos...)
	}

	td := &tls.Dialer{Config: cfg}
	c, err := td.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tc := c.(*tls.Conn)
	if tc.ConnectionState().NegotiatedProtocol != "h2" {
		_ = tc.Close()
		return nil, ErrALPNFailed
	}
	return tc, nil
}

// PlaintextDialer dials addr over TCP for H2C prior-knowledge connections
// (RFC 7540 §3.4). No TLS handshake or ALPN negotiation is performed.
// NewClientConn sends the HTTP/2 connection preface automatically.
type PlaintextDialer struct{}

// Dial dials addr over TCP and returns the raw connection.
func (d *PlaintextDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	var nd net.Dialer
	return nd.DialContext(ctx, "tcp", addr)
}

// Dial dials addr, runs the TLS handshake, asserts ALPN h2, and runs
// the HTTP/2 SETTINGS exchange. The returned Conn is ready for
// NewStream.
func Dial(ctx context.Context, addr string, opts ConnOptions) (*Conn, error) {
	opts = opts.defaulted()
	transport, err := opts.Dialer.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	c, err := NewClientConn(ctx, transport, opts)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	return c, nil
}
