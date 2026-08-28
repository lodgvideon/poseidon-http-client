package poolcore

import "github.com/lodgvideon/poseidon-http-client/pool"

// Re-exports of the vocabulary in package pool, so the machinery below can
// spell the types it has always spelled.
//
// These are aliases, not a second vocabulary: poolcore.Address IS pool.Address.
// They exist because this package was carved out of client, and rewriting every
// type reference in the move would have been a large blind edit of code whose
// behaviour must not change — `Stats` alone appears as a type, a method name and
// a channel element in the same declaration. New code here may spell either.
type (
	// Address is one resolved backend endpoint.
	Address = pool.Address
	// Resolver discovers backend addresses for a logical service.
	Resolver = pool.Resolver
	// Selector picks one address from a candidate set for the next dial.
	Selector = pool.Selector
	// PickContext carries per-pick hints to the Selector.
	PickContext = pool.PickContext
	// PoolOptions configures a per-host pool.
	PoolOptions = pool.Options
	// Stats is a snapshot of pool state.
	Stats = pool.Stats
	// DrainMode governs sub-pool lifecycle when an address leaves the set.
	DrainMode = pool.DrainMode
	// CloseReason identifies why a conn was closed or evicted.
	CloseReason = pool.CloseReason
	// DialEvent carries metadata for a completed dial attempt.
	DialEvent = pool.DialEvent
	// ConnCloseEvent carries metadata for a closed connection.
	ConnCloseEvent = pool.ConnCloseEvent
	// ResolverUpdateEvent carries metadata for an applied address set.
	ResolverUpdateEvent = pool.ResolverUpdateEvent
	// DialError wraps a dial failure and the address that produced it.
	DialError = pool.DialError
)

// Sentinel errors, re-exported for the same reason as the types above.
var (
	// ErrPoolClosed is returned by pool operations after Close.
	ErrPoolClosed = pool.ErrPoolClosed
	// ErrAcquireTimeout is returned when AcquireTimeout elapses.
	ErrAcquireTimeout = pool.ErrAcquireTimeout
	// ErrDialBackoff is returned inside the dial-backoff window.
	ErrDialBackoff = pool.ErrDialBackoff
	// ErrNoAddresses is returned when no backend is available.
	ErrNoAddresses = pool.ErrNoAddresses
	// ErrWatchUnsupported is returned by a Resolver without push updates.
	ErrWatchUnsupported = pool.ErrWatchUnsupported
)

// CloseReason values, re-exported.
const (
	// CloseIdle is set when the conn was idle past Options.IdleTimeout.
	CloseIdle = pool.CloseIdle
	// CloseDead is set when conn.IsAlive returned false at eviction time.
	CloseDead = pool.CloseDead
	// CloseGoAway is set when the peer sent GOAWAY.
	CloseGoAway = pool.CloseGoAway
	// CloseManual is set when the conn was closed via Close.
	CloseManual = pool.CloseManual
)

// DrainMode values, re-exported.
const (
	// DrainGraceful refuses new acquires and closes once idle.
	DrainGraceful = pool.DrainGraceful
	// DrainHard closes every conn in the removed sub-pool immediately.
	DrainHard = pool.DrainHard
	// DrainLazy leaves closing to idle eviction.
	DrainLazy = pool.DrainLazy
)

// RoundRobin returns a Selector cycling through the candidate set.
func RoundRobin() Selector { return pool.RoundRobin() }

// StaticResolver returns a Resolver serving a fixed address set.
func StaticResolver(addrs ...Address) Resolver { return pool.StaticResolver(addrs...) }
