package client

import (
	"context"

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
}
