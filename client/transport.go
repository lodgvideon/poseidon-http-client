package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// pushLookuper resolves a promised push id to the stream carrying it.
//
// It is an interface rather than the func it used to be because the func was
// always the same method value, cn.LookupStream, and Go heap-allocates a closure
// for a method value on every evaluation. On the pooled H2 path that single
// expression was 12,593 of the 17,914 objects openExchange accounted for —
// roughly a fifth of the whole regime's allocation count — for a method on a
// connection that outlives the exchange.
//
// *conn.Conn satisfies it as it stands, and an interface holding one pointer
// needs no allocation to build, so the cost goes to zero rather than moving.
type pushLookuper interface {
	// LookupStream returns the stream for a promised push id, and whether it
	// is still live.
	LookupStream(id uint32) (conn.StreamRef, bool)
}

// transport is the seam between Client and the underlying connection
// supply. openExchange acquires a connection and opens a protocol-level
// exchange (H2 stream or H1.1 request slot) in one step.
type transport interface {
	// openExchange acquires a healthy connection, opens a new
	// request/response exchange on it, and returns the exchange ready
	// for SendHeaders. release MUST be called exactly once when the
	// exchange is fully drained or has errored. release is safe to call
	// from any goroutine.
	//
	// For H2 transports: s is a *conn.Stream and pushLookup is the
	// connection itself, enabling server-push handling. For H1.1
	// transports: s is a *h1Exchange and pushLookup is nil (H1.1 has no
	// server push).
	//
	// Errors include ErrClosed, ErrRedialBackoff, *DialError, and ctx errors.
	openExchange(ctx context.Context) (s protoStream, pushLookup pushLookuper, release func(), err error)

	// close prevents further exchange opens and closes any underlying
	// conn(s). Idempotent.
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
