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
