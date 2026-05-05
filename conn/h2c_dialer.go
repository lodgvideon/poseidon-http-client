package conn

import (
	"context"
	"net"
)

// H2CDialer establishes cleartext HTTP/2 over TCP — no TLS. Useful for
// load-test scenarios where TLS overhead must be excluded from
// measurements. NOT safe for production traffic over untrusted networks.
type H2CDialer struct {
	NetDialer *net.Dialer
}

// Dial implements Dialer.
func (d *H2CDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	nd := d.NetDialer
	if nd == nil {
		nd = &net.Dialer{}
	}
	return nd.DialContext(ctx, "tcp", addr)
}
