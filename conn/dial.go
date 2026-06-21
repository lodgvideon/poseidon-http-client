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
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		cfg = cfg.Clone()
		if cfg.MinVersion == 0 {
			cfg.MinVersion = tls.VersionTLS12
		}
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

// FlexDialer dials addr over TCP + TLS offering both "h2" and "http/1.1"
// in ALPN so the server can choose its preferred protocol. After a
// successful dial, the caller determines the negotiated protocol via
// NegotiatedProtocol(conn). Use this with the client's ALPN-aware
// transport (NewClient with TransportALPN) rather than with conn.Dial,
// which asserts "h2" and returns ErrALPNFailed for "http/1.1" peers.
type FlexDialer struct {
	// Config is cloned at each Dial call. When nil, a safe default
	// (TLS 1.2+, NextProtos=["h2","http/1.1"]) is used.
	Config *tls.Config
}

// Dial dials addr over TCP, runs TLS, and returns the *tls.Conn with
// ALPN negotiated to either "h2" or "http/1.1". Returns ErrALPNFailed
// if the server selects neither protocol.
func (d *FlexDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	cfg := d.Config
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12} //nolint:gosec
	} else {
		cfg = cfg.Clone()
		if cfg.MinVersion == 0 {
			cfg.MinVersion = tls.VersionTLS12
		}
	}
	have := map[string]bool{}
	for _, p := range cfg.NextProtos {
		have[p] = true
	}
	var prepend []string
	if !have["h2"] {
		prepend = append(prepend, "h2")
	}
	if !have["http/1.1"] {
		prepend = append(prepend, "http/1.1")
	}
	if len(prepend) > 0 {
		cfg.NextProtos = append(prepend, cfg.NextProtos...)
	}

	td := &tls.Dialer{Config: cfg}
	c, err := td.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tc := c.(*tls.Conn)
	proto := tc.ConnectionState().NegotiatedProtocol
	if proto != "h2" && proto != "http/1.1" {
		_ = tc.Close()
		return nil, ErrALPNFailed
	}
	return tc, nil
}

// NegotiatedProtocol returns the ALPN-negotiated protocol of nc.
// Returns "" for plain-TCP connections (H2C / no TLS).
func NegotiatedProtocol(nc net.Conn) string {
	if tc, ok := nc.(*tls.Conn); ok {
		return tc.ConnectionState().NegotiatedProtocol
	}
	return ""
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
