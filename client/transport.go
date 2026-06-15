package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// transport is the seam between Client and the underlying connection
// supply. C.1 ships exactly one impl (singleConn); a future C.2
// connection pool will implement the same interface.
type transport interface {
	// acquire returns a healthy *conn.Conn together with a release
	// function that the caller MUST call exactly once when the
	// associated request is fully drained or has errored. release is
	// safe to call from any goroutine. Errors include ErrClosed,
	// ErrRedialBackoff, *DialError, and ctx errors.
	acquire(ctx context.Context) (c *conn.Conn, release func(), err error)

	// close prevents further acquires and closes any underlying conn.
	// Idempotent.
	close() error

	// shutdown gracefully drains all in-flight requests and closes
	// the underlying conn(s) within the given timeout. After the
	// timeout, any remaining streams are force-closed. Idempotent.
	shutdown(gracefulTimeout time.Duration) error

	// warmup opens up to n connections in the background, returning
	// immediately. Errors during dial are surfaced through the
	// Client's metrics.OnDial hook; the method itself does not
	// block on per-dial success. n is capped at the underlying
	// transport's MaxConnsPerHost.
	warmup(n int)
}
