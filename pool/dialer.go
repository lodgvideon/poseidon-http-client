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
// Only consulted by the managed transports (TransportH1Managed,
// TransportManaged), where a Selector picks the Address. Non-managed
// transports dial a caller-supplied address with no Resolver involved and
// never construct an Address to offer, so DialAddress is never called
// there. HTTP/3 transports do not use this Dialer abstraction at all.
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

// DialerFunc adapts a plain function to Dialer, the same shape as
// conn.DialerFunc — declared again here, rather than imported, for the same
// reason as Dialer above. A value of this type also satisfies conn.Dialer:
// the two interfaces declare the identical single method, so Go accepts it
// on either side with no conversion at the call site.
type DialerFunc func(ctx context.Context, addr string) (net.Conn, error)

// Dial calls f.
func (f DialerFunc) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return f(ctx, addr)
}

// DialResolved dials resolved using d, preferring d's AddressDialer
// capability so a caller-configured dialer receives the resolver's Address
// (Attributes included) instead of a flattened string. Falls back to
// d.Dial(ctx, fallbackAddr) when d does not implement AddressDialer.
// fallbackAddr is used as given, never derived from resolved here: a caller
// that already has the flattened string (every WrapForAddressDialer closure
// does, via the addr its own caller dials with) passes it straight through
// instead of paying to rebuild it on every dial. d must be non-nil.
func DialResolved(ctx context.Context, d Dialer, resolved Address, fallbackAddr string) (net.Conn, error) {
	if ad, ok := d.(AddressDialer); ok {
		return ad.DialAddress(ctx, resolved)
	}
	return d.Dial(ctx, fallbackAddr)
}

// WrapForAddressDialer returns d wrapped so every dial it makes for resolved
// routes through DialResolved (#943) — shared by both managed pools that
// hold a conn.Dialer (client's H1 sub-pool, poolcore's H2 sub-pool), so the
// wrap-or-passthrough logic exists once, not once per protocol. Returns nil,
// unwrapped, when d is nil: callers rely on this to preserve their own
// nil-dialer behavior downstream (conn.Dial's own default, or a pre-existing
// nil-dialer panic at the caller's original dial site) rather than
// relocating it inside here.
//
// The wrap hides any capability interface d itself implements beyond
// AddressDialer (conn.ALPNAsserter today). Safe because the only consumer,
// client.validateDialerALPN, runs at NewClient time on the original,
// unwrapped dialer — a future capability checked at dial time would need to
// be re-exposed here too.
func WrapForAddressDialer(d Dialer, resolved Address) Dialer {
	if d == nil {
		return nil
	}
	return DialerFunc(func(ctx context.Context, addr string) (net.Conn, error) {
		return DialResolved(ctx, d, resolved, addr)
	})
}
