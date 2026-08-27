package pool

import "time"

// Options configures the per-host connection pool.
//
// Defaults are applied by the pool constructors, not by NewClient, and they
// cover four of the seven fields: MaxConnsPerHost, HealthCheckPeriod,
// DialBackoff and DialTimeout. MaxStreamsPerConn, IdleTimeout and
// AcquireTimeout are defaulted NOWHERE, so a zero there is load-bearing: it
// disables idle eviction and leaves an acquire with no bound of its own. This
// comment used to say every zero value was replaced, and at NewClient, which
// is why the disabled-by-default half of the surface is easy to miss.
type Options struct {
	// MaxConnsPerHost caps live connections in this pool.
	// 0 → 1 (effectively single-conn).
	MaxConnsPerHost int

	// MaxStreamsPerConn is the soft cap on concurrent streams the
	// pool will assign to one connection. Effective cap is
	// min(this, peer SETTINGS_MAX_CONCURRENT_STREAMS) where the
	// peer value is observed via (*conn.Conn).PeerMaxConcurrentStreams.
	// 0 → use peer value (or local defaultMaxConcurrentStreams if peer unbounded).
	MaxStreamsPerConn int

	// IdleTimeout closes a conn that has been idle (active==0)
	// longer than this duration. 0 → never close on idle.
	IdleTimeout time.Duration

	// HealthCheckPeriod is the actor's tick interval for idle and
	// health-check sweeps. 0 → 30 * time.Second.
	HealthCheckPeriod time.Duration

	// DialBackoff refuses new dials within this window after a
	// dial failure on this pool. 0 → 1 * time.Second.
	DialBackoff time.Duration

	// AcquireTimeout bounds how long Acquire waits for capacity.
	// 0 → governed by ctx only.
	AcquireTimeout time.Duration

	// DialTimeout bounds how long a single dial attempt may block in
	// conn.Dial. Without this bound a dial against a black-hole host
	// hangs the dialOne goroutine indefinitely, leaking it across
	// pool.Close. 0 → 30 * time.Second default.
	DialTimeout time.Duration
}

// Stats is a snapshot of pool state.
type Stats struct {
	ActiveConns     int
	InFlightStreams int
	Waiters         int
	InFlightDials   int
	// Populated by managedPool.Stats(); zero for single-address pools.
	Addresses        int // number of addresses in the current resolved set
	DrainingSubpools int // sub-pools currently draining (removed from resolver set)
}

// DrainMode governs sub-pool lifecycle when an address is removed
// from the resolver's set.
type DrainMode int

const (
	// DrainGraceful refuses new acquires on the removed sub-pool;
	// existing in-flight requests complete naturally; sub-pool closes
	// when its active stream count reaches zero.
	DrainGraceful DrainMode = iota
	// DrainHard closes every conn in the removed sub-pool immediately;
	// in-flight streams surface as RST_STREAM(CANCEL).
	DrainHard
	// DrainLazy refuses new acquires and leaves closing to idle eviction, which
	// means Options.IdleTimeout decides whether the conns ever close at all.
	// That field defaults to 0, documented as "never close on idle" — so under
	// default options DrainLazy's removed sub-pool keeps its connections open
	// for the life of the pool, and "eventual" never arrives. Set IdleTimeout,
	// or use DrainGraceful, which closes once the sub-pool goes idle regardless.
	DrainLazy
)
