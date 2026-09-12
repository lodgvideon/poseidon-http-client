package pool

import (
	"context"
	"net"
)

// AddressDialer is implemented by a Dialer that wants the resolver's full
// Address — host, port, and any Selector-visible Attributes — instead of a
// flattened "host:port" string. Managed transports check for this the same
// way a conn.Dialer is checked for conn.ALPNAsserter, falling back to the
// plain Dial(ctx, addr string) when a dialer does not implement it.
//
// Only consulted on managed transports, where a Selector actually picks an
// Address (client.h1Pool under TransportH1Managed, internal/poolcore.Pool
// under the HTTP/2 TransportManaged). Non-managed transports dial a
// caller-supplied address with no Resolver involved and never construct an
// Address to offer, so DialAddress is never called there. HTTP/3 transports
// do not use this Dialer abstraction at all.
//
// A caller implementing DialAddress must treat addr.Attributes as read-only:
// it is the same map instance the Resolver/Selector hold, not a copy.
type AddressDialer interface {
	DialAddress(ctx context.Context, addr Address) (net.Conn, error)
}

// Dialer is the plain-string dial contract every conn.Dialer satisfies. It is
// declared here structurally, rather than imported, so this package does not
// depend on conn; conn.Dialer and any custom dialer satisfy it without
// naming it.
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// DialResolved dials resolved using d, preferring d's AddressDialer
// capability so a caller-configured dialer receives the resolver's Address
// (Attributes included) instead of a flattened string. Falls back to
// d.Dial(ctx, resolved.String()) when d does not implement AddressDialer.
func DialResolved(ctx context.Context, d Dialer, resolved Address) (net.Conn, error) {
	if ad, ok := d.(AddressDialer); ok {
		return ad.DialAddress(ctx, resolved)
	}
	return d.Dial(ctx, resolved.String())
}
