package client

import (
	"math/rand"

	"github.com/lodgvideon/poseidon-http-client/pool"
)

// The connection-management vocabulary moved to package pool so grpc can reuse
// the pool without importing this package. These are aliases, not wrappers:
// client.Address and pool.Address are one type, so a Resolver written against
// either satisfies both and no caller had to change. See docs/adr/0001.

// Address is one resolved backend endpoint.
type Address = pool.Address

// Resolver discovers backend addresses for a logical service.
type Resolver = pool.Resolver

// DNSOptions tunes DNSResolver.
type DNSOptions = pool.DNSOptions

// Selector picks one address from a candidate set for the next dial.
type Selector = pool.Selector

// PickContext carries the per-request context a Selector may key on.
type PickContext = pool.PickContext

// PoolOptions configures the per-host connection pool.
type PoolOptions = pool.Options

// Stats is a snapshot of pool state.
type Stats = pool.Stats

// DrainMode governs sub-pool lifecycle when an address leaves the resolver set.
type DrainMode = pool.DrainMode

// DialEvent carries metadata for OnDial.
type DialEvent = pool.DialEvent

// ConnCloseEvent carries metadata for OnConnClose.
type ConnCloseEvent = pool.ConnCloseEvent

// ResolverUpdateEvent carries metadata for OnResolverUpdate.
type ResolverUpdateEvent = pool.ResolverUpdateEvent

// CloseReason identifies why a conn was closed/evicted.
type CloseReason = pool.CloseReason

// CloseReason values.
const (
	// CloseIdle is set when the conn was idle past PoolOptions.IdleTimeout.
	CloseIdle = pool.CloseIdle
	// CloseDead is set when conn.IsAlive returned false at eviction time.
	CloseDead = pool.CloseDead
	// CloseGoAway is set when the conn was evicted because the peer sent GOAWAY.
	CloseGoAway = pool.CloseGoAway
	// CloseManual is set when the conn was closed via Pool.Close / Client.Close.
	CloseManual = pool.CloseManual
	// CloseNotReusable is set when an HTTP/1.1 connection cannot be reused.
	CloseNotReusable = pool.CloseNotReusable
)

// DrainMode values.
const (
	// DrainGraceful refuses new acquires and closes the sub-pool once idle.
	DrainGraceful = pool.DrainGraceful
	// DrainHard closes every conn in the removed sub-pool immediately.
	DrainHard = pool.DrainHard
	// DrainLazy refuses new acquires and leaves closing to idle eviction.
	DrainLazy = pool.DrainLazy
)

var (
	// ErrWatchUnsupported is returned by a Resolver.Watch implementation that
	// does not support push-style updates.
	ErrWatchUnsupported = pool.ErrWatchUnsupported
	// ErrNoAddresses is returned when a Resolver yields zero addresses and has
	// no cached set to fall back on, or when a Selector receives an empty set.
	ErrNoAddresses = pool.ErrNoAddresses
	// ErrNilKeyFn is returned by Hash when keyFn is nil.
	ErrNilKeyFn = pool.ErrNilKeyFn
	// ErrPoolClosed is returned by Pool operations after Close.
	ErrPoolClosed = pool.ErrPoolClosed
	// ErrAcquireTimeout is returned when PoolOptions.AcquireTimeout elapses
	// before capacity becomes available.
	ErrAcquireTimeout = pool.ErrAcquireTimeout
	// ErrDialBackoff is returned when a recent dial failure on the pool is
	// still within the DialBackoff window.
	ErrDialBackoff = pool.ErrDialBackoff
)

// DialError wraps the underlying dial error and the address that failed.
type DialError = pool.DialError

// StaticResolver returns a Resolver serving a fixed address set.
func StaticResolver(addrs ...Address) Resolver { return pool.StaticResolver(addrs...) }

// DNSResolver returns a Resolver that looks host up in DNS and pairs every
// answer with port.
func DNSResolver(host string, port int, opts DNSOptions) Resolver {
	return pool.DNSResolver(host, port, opts)
}

// RoundRobin returns a Selector cycling through the candidate set.
func RoundRobin() Selector { return pool.RoundRobin() }

// Random returns a Selector picking uniformly at random. A nil rng uses the
// package-level source.
func Random(rng *rand.Rand) Selector { return pool.Random(rng) }

// Hash returns a Selector picking by a stable hash of keyFn's result, so the
// same key lands on the same backend while the set is unchanged.
func Hash(keyFn func(PickContext) string) (Selector, error) { return pool.Hash(keyFn) }
