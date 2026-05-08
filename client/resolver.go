package client

import (
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
