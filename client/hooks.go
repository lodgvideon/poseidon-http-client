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

	// OnConnClose fires whenever this client closes a connection: a pool
	// eviction, a single-connection transport tearing its conn down, or an
	// HTTP/1.1 connection dropped because it cannot be reused.
	//
	// It used to say "evicted from a pool", and the single-connection transports
	// behaved three different ways against that one sentence — HTTP/3 fired it,
	// HTTP/2 and HTTP/1.1 did not — so a dashboard saw connection churn on some
	// transports and silence on others doing identical work. One rule now: every
	// close this client performs is observable, and Reason says which kind it was.
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
	//
	// BytesRecv is the total DATA payload received — for Do. For DoStream it is
	// always 0, and that is not a gap in the plumbing: the event fires when
	// DoStream RETURNS, which is when the response headers arrive and before the
	// caller has read a single body byte. Zero is the true count at that instant.
	//
	// Latency has the same shape on that path: time to headers, not to the last
	// byte. Moving the event to StreamResponse.Close would fix both numbers and
	// break two other things — it would change what Latency means for every
	// existing consumer, and a caller that abandons a stream without closing it
	// would never be told the request finished at all. A streaming caller that
	// needs the byte count owns it: it is doing the reading.
	BytesSent, BytesRecv int64
	Attempt              int
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
