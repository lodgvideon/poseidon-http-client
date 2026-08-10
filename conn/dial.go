package conn

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
)

// Dialer abstracts how the underlying transport is established.
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// ALPNAsserter is implemented by Dialers that only ever return connections
// speaking one application protocol, so a caller can check the pairing before
// the first byte is written. AssertsALPN returns the ALPN token the dialer
// guarantees ("h2", "http/1.1", …), or "" for a dialer that may return any of
// several protocols (FlexDialer) or none at all (PlaintextDialer).
//
// client.NewClient uses this to reject a transport/dialer pairing that can only
// fail — an HTTP/1.1 transport handed an h2-asserting dialer, say — at
// construction rather than as a mangled exchange later. Custom dialers may
// implement it to get the same check.
type ALPNAsserter interface {
	AssertsALPN() string
}

// containsProto reports whether protos contains want.
func containsProto(protos []string, want string) bool {
	for _, p := range protos {
		if p == want {
			return true
		}
	}
	return false
}

// TLSDialer dials addr over TCP, runs TLS, and asserts ALPN h2.
// If Config is nil a defaulted *tls.Config with NextProtos=[h2] is used.
//
// "h2" is added to Config.NextProtos when the list does not already carry it,
// but a non-empty list that deliberately excludes it — NextProtos=["http/1.1"]
// — is a contradiction and Dial refuses it with ErrALPNConflict rather than
// silently rewriting the offer. For HTTP/1.1 over TLS use H1TLSDialer; to let
// the server choose, use FlexDialer with TransportALPN.
type TLSDialer struct {
	Config *tls.Config
}

// AssertsALPN reports that this dialer only ever returns h2 connections.
func (d *TLSDialer) AssertsALPN() string { return "h2" }

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
		cfg = &tls.Config{}
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
	if !containsProto(cfg.NextProtos, "h2") {
		cfg.NextProtos = append([]string{"h2"}, cfg.NextProtos...)
	}
	return cfg
}

// Dial dials addr over TCP, runs the TLS handshake with NextProtos containing
// "h2", and returns the negotiated *tls.Conn. Returns ErrALPNFailed if the peer
// did not select "h2", and ErrALPNConflict — without dialing — if Config pins a
// non-empty NextProtos that excludes "h2".
func (d *TLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	// An explicit list without "h2" means the caller wants some other protocol.
	// Prepending "h2" anyway would negotiate h2 against any h2-capable server and
	// then pass that connection to whatever codec the caller picked — the failure
	// surfaces much later, as a protocol error with no mention of ALPN. Refuse here.
	if d.Config != nil && len(d.Config.NextProtos) > 0 && !containsProto(d.Config.NextProtos, "h2") {
		return nil, fmt.Errorf("%w: TLSDialer asserts \"h2\" but Config.NextProtos is %q; use H1TLSDialer for HTTP/1.1 or FlexDialer to let the server choose",
			ErrALPNConflict, d.Config.NextProtos)
	}
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

// H1TLSDialer dials addr over TCP, runs TLS offering only the "http/1.1" ALPN
// token, and asserts the peer did not select anything else. It is the TLS
// counterpart to PlaintextDialer for the HTTP/1.1 transports
// (client.TransportH1SingleConn, TransportH1Pool, TransportH1Managed), which
// cannot use TLSDialer: that one asserts h2.
//
// Against a server offering both "h2" and "http/1.1" — the common HTTPS
// deployment — this is what keeps the server from choosing h2 and leaving the
// HTTP/1.1 codec writing request lines into a connection the peer frames as
// HTTP/2.
//
//	client.NewH1PoolClient(addr, &conn.H1TLSDialer{
//	    Config: &tls.Config{ServerName: "example.com", MinVersion: tls.VersionTLS12},
//	}, client.PoolOptions{MaxConnsPerHost: 8})
type H1TLSDialer struct {
	// Config is cloned at each Dial call. When nil, a config with TLS 1.2
	// minimum is used. NextProtos is set to ["http/1.1"] when empty; a list
	// containing "h2" is rejected with ErrALPNConflict rather than overridden.
	Config *tls.Config
}

// AssertsALPN reports that this dialer only ever returns http/1.1 connections.
func (d *H1TLSDialer) AssertsALPN() string { return "http/1.1" }

// Dial dials addr over TCP, runs the TLS handshake offering "http/1.1", and
// returns the negotiated *tls.Conn. Returns ErrALPNConflict — without dialing —
// if Config.NextProtos contains "h2", and ErrALPNNotHTTP11 if the peer selected
// some protocol other than "http/1.1". A peer that selects nothing (no ALPN) is
// accepted: TLS without ALPN implies HTTP/1.1.
func (d *H1TLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	if d.Config != nil && containsProto(d.Config.NextProtos, "h2") {
		return nil, fmt.Errorf("%w: H1TLSDialer asserts \"http/1.1\" but Config.NextProtos is %q; use TLSDialer for HTTP/2 or FlexDialer to let the server choose",
			ErrALPNConflict, d.Config.NextProtos)
	}
	var cfg *tls.Config
	if d.Config == nil {
		cfg = &tls.Config{}
	} else {
		cfg = d.Config.Clone()
	}
	if cfg.MinVersion == 0 {
		cfg.MinVersion = tls.VersionTLS12
	}
	if len(cfg.NextProtos) == 0 {
		cfg.NextProtos = []string{"http/1.1"}
	}

	td := &tls.Dialer{Config: cfg}
	c, err := td.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tc := c.(*tls.Conn)
	if p := tc.ConnectionState().NegotiatedProtocol; p != "" && p != "http/1.1" {
		_ = tc.Close()
		return nil, fmt.Errorf("%w: peer selected %q", ErrALPNNotHTTP11, p)
	}
	return tc, nil
}

// PlaintextDialer dials addr over TCP for H2C prior-knowledge connections
// (RFC 7540 §3.4). No TLS handshake or ALPN negotiation is performed.
// NewClientConn sends the HTTP/2 connection preface automatically.
//
// It carries no ALPN assertion: the same cleartext connection serves an H2C
// prior-knowledge peer and a plain HTTP/1.1 one, and which of the two is
// speaking is decided by the transport, not by the dial.
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

// AssertsALPN returns "" — FlexDialer offers both "h2" and "http/1.1" and
// returns whichever the server selects, so it asserts no single protocol.
func (d *FlexDialer) AssertsALPN() string { return "" }

// Dial dials addr over TCP, runs TLS, and returns the *tls.Conn with
// ALPN negotiated to either "h2" or "http/1.1". Returns ErrALPNFailed
// if the server selects neither protocol.
func (d *FlexDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	cfg := d.Config
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
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
