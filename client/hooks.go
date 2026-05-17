// client/hooks.go
package client

import "time"

// Hooks is an optional set of callbacks invoked on lifecycle events.
// All fields are optional; nil hooks are skipped at zero cost.
//
// Hooks must not block — request hooks fire on the caller's goroutine
// and pool hooks fire on the pool actor goroutine. A blocking hook
// will stall request processing or the pool actor.
//
// Hook panics propagate. Wrap callbacks with recover() if isolation
// is needed.
type Hooks struct {
	// OnRequestStart fires at the top of Client.Do / Client.DoStream
	// before transport acquire.
	OnRequestStart func(RequestStartEvent)

	// OnRequestComplete fires when Do returns (sync) or when DoStream
	// returns its initial StreamResponse (or an error).
	OnRequestComplete func(RequestCompleteEvent)

	// OnRetry fires inside the Retryer between attempts, before the
	// backoff sleep. The event carries the computed backoff duration.
	OnRetry func(RetryEvent)

	// OnDial fires after a transport dial completes (success or error).
	OnDial func(DialEvent)

	// OnConnClose fires when a conn is evicted from a pool.
	OnConnClose func(ConnCloseEvent)

	// OnResolverUpdate fires when managedPool applies a new address
	// set from the Resolver. Not fired for TransportSingleConn or
	// TransportPool.
	OnResolverUpdate func(ResolverUpdateEvent)
}

// RequestStartEvent carries metadata for OnRequestStart.
type RequestStartEvent struct {
	Method, Path, Authority string
	Attempt                 int // 0 for first try, ≥1 for retries
}

// RequestCompleteEvent carries metadata for OnRequestComplete.
type RequestCompleteEvent struct {
	Method, Path, Authority string
	Status                  int // 0 if no headers received
	Err                     error
	Latency                 time.Duration
	// BytesSent is the request body payload size in bytes (len(req.Body)).
	// It excludes HTTP/2 frame overhead and any trailer HEADERS frame.
	// BytesRecv is the total DATA payload received.
	BytesSent, BytesRecv int64
	Attempt                 int
}

// RetryEvent carries metadata for OnRetry.
type RetryEvent struct {
	Method, Path string
	Attempt      int
	Err          error
	Backoff      time.Duration
}

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
	}
	return "unknown"
}
