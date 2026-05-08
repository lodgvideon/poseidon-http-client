package client

import (
	"context"
	"net"
	"strconv"
)

// Address is one resolved backend endpoint.
type Address struct {
	// Host is the dial target — IP literal or DNS name. The pool
	// never re-resolves Host; the Resolver owns that.
	Host string
	Port int
	// Attributes carries optional metadata for user Selectors
	// (zone, weight, etc.). Built-in selectors ignore it.
	//
	// Note: Attributes must remain nil if the Address is used as a
	// map key (managedPool's sub-pool registry). Built-in resolvers
	// never set it.
	Attributes map[string]string
}

// String returns "host:port" using net.JoinHostPort (adds brackets
// around IPv6 literals automatically).
func (a Address) String() string {
	return net.JoinHostPort(a.Host, strconv.Itoa(a.Port))
}

// Resolver discovers backend addresses for a logical service. It
// must be goroutine-safe.
type Resolver interface {
	// Resolve returns the current address set. Implementations may
	// cache and serve from a TTL-backed cache. A non-nil err with
	// len(addrs) == 0 is a hard failure; non-nil err with
	// len(addrs) > 0 is a soft warning (use cached set).
	Resolve(ctx context.Context) ([]Address, error)

	// Watch streams address-set updates. The first message MUST be
	// the current full set. Subsequent messages MUST also be the
	// full set (not deltas). The channel is closed when ctx is
	// cancelled or on terminal error. Implementations without push
	// support return ErrWatchUnsupported; the managedPool falls back
	// to a ticker around Resolve in that case.
	Watch(ctx context.Context) (<-chan []Address, error)
}

// staticResolver returns a fixed address set; Watch sends it once
// then closes.
type staticResolver struct {
	addrs []Address
}

// StaticResolver constructs a Resolver that always returns the given
// fixed set. The slice is copied; subsequent caller mutation has no
// effect.
func StaticResolver(addrs ...Address) Resolver {
	cp := make([]Address, len(addrs))
	copy(cp, addrs)
	return &staticResolver{addrs: cp}
}

func (r *staticResolver) Resolve(_ context.Context) ([]Address, error) {
	return r.addrs, nil
}

func (r *staticResolver) Watch(_ context.Context) (<-chan []Address, error) {
	ch := make(chan []Address, 1)
	ch <- r.addrs
	close(ch)
	return ch, nil
}
