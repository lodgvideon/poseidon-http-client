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

// h2TLS12CipherSuites returns the TLS 1.2 cipher suites an h2-only dialer offers:
// the six forward-secret AEAD suites that are NOT on RFC 9113 Appendix A's
// prohibited list. A fresh slice per call so a caller mutating the returned
// tls.Config cannot alias a shared backing array. The list mirrors Go's own
// preferred TLS 1.2 AEAD suites and includes the §9.2.2-mandated
// TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256.
func h2TLS12CipherSuites() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	}
}

// tlsClientConfig builds the effective *tls.Config for an HTTP/2-over-TLS dial: a
// clone of the caller's config (or a fresh one), with "h2" ensured in NextProtos
// for ALPN and the RFC 9113 §9.2 TLS 1.2 floor enforced. The floor is raised even
// when a caller pins an explicit lower MinVersion (TLS 1.0/1.1), not only when it
// is unset — HTTP/2 over TLS requires TLS 1.2 or higher.
func (d *TLSDialer) tlsClientConfig() *tls.Config {
	var cfg *tls.Config
	if d.Config == nil {
		cfg = &tls.Config{} //nolint:gosec // MinVersion is raised to TLS 1.2 below
	} else {
		cfg = d.Config.Clone()
	}
	if cfg.MinVersion < tls.VersionTLS12 {
		cfg.MinVersion = tls.VersionTLS12
	}
	// RFC 9113 §9.2.2: "A deployment of HTTP/2 over TLS 1.2 SHOULD NOT use any of
	// the prohibited cipher suites listed in Appendix A." This dialer negotiates
	// only h2 (it returns ErrALPNFailed otherwise), so §9.2.2's closing note — a
	// client MAY advertise prohibited suites to reach an HTTP/1.1 fallback — does
	// not apply; FlexDialer, which offers http/1.1 too, is deliberately left
	// unconstrained. Pin the TLS 1.2 offer to the forward-secret AEAD suites (a
	// superset of the mandated TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256), only when
	// the caller did not choose its own list. TLS 1.3 suites are not configurable
	// through this field and are unaffected.
	if cfg.CipherSuites == nil {
		cfg.CipherSuites = h2TLS12CipherSuites()
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
	return cfg
}

// Dial dials addr over TCP, runs the TLS handshake with NextProtos containing
// "h2", and returns the negotiated *tls.Conn. Returns ErrALPNFailed if the peer
// did not select "h2".
func (d *TLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	td := &tls.Dialer{Config: d.tlsClientConfig()}
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
