package conn

import (
	"context"
	"net"
)

// Dialer establishes a transport-layer connection ready for HTTP/2.
// Implementations: TLSDialer (TLS+ALPN h2), H2CDialer (cleartext), or
// caller-supplied (mTLS, SNI override, custom net.Dial).
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}
