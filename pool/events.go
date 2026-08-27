package pool

import "time"

// Connection-lifecycle events. A pool reports these through an Observer; the
// request-lifecycle events, and the Hooks struct that carries both, stay in
// the packages that own a request.

// DialEvent carries metadata for OnDial.
type DialEvent struct {
	Addr     string
	Err      error
	Duration time.Duration
}

// ConnCloseEvent carries metadata for OnConnClose.
type ConnCloseEvent struct {
	Addr   string
	Reason CloseReason
}

// ResolverUpdateEvent carries metadata for OnResolverUpdate.
type ResolverUpdateEvent struct {
	Added, Removed []Address
	Total          int
}

// CloseReason identifies why a conn was closed/evicted.
type CloseReason int

// CloseReason values.
const (
	// CloseIdle is set when the conn was idle past PoolOptions.IdleTimeout.
	CloseIdle CloseReason = iota
	// CloseDead is set when conn.IsAlive returned false at eviction time.
	CloseDead
	// CloseGoAway is set when the conn was evicted because the peer sent GOAWAY.
	CloseGoAway
	// CloseManual is set when the conn was closed via Pool.Close / Client.Close.
	CloseManual
	// CloseNotReusable is set when a connection cannot be reused and is dropped
	// rather than returned: the peer asked for it with "Connection: close", or it
	// was left in a state that can no longer be framed. HTTP/1.1 only — the
	// multiplexed protocols have no per-response reuse decision.
	CloseNotReusable
)

// String returns a stable lowercase label for the reason. Handy for
// metric labels and log fields.
func (r CloseReason) String() string {
	switch r {
	case CloseIdle:
		return "idle"
	case CloseDead:
		return "dead"
	case CloseGoAway:
		return "goaway"
	case CloseManual:
		return "manual"
	case CloseNotReusable:
		return "not-reusable"
	}
	return "unknown"
}
