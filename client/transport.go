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
	// For H2 transports: s is a conn.StreamRef — a handle to one lifetime
	// of a pooled stream, by value, not a *conn.Stream — and pushLookup is
	// the connection itself, enabling server-push handling. For H1.1
	// transports: s is a *h1Exchange and for H3 an *h3Exchange, and
	// pushLookup is nil for both (neither has server push here).
	//
	// Errors include ErrClosed, ErrRedialBackoff, *DialError, and ctx errors.
	openExchange(ctx context.Context) (s protoStream, pushLookup pushLookuper, rel releaser, err error)

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

// releaser hands an exchange's connection back to whatever owns it. It is an
// interface rather than a func() because the pool transports' release captures
// both the pool and the connection, and a capturing closure allocates on every
// request — 2 of the 7 allocations openExchange was measured at (#476). An
// interface holding a pointer that already exists on the heap costs nothing,
// which is the same move that removed cn.LookupStream in #477.
type releaser interface {
	// release returns the exchange's connection. It MUST be called exactly
	// once, when the exchange is fully drained or has errored, and is safe to
	// call from any goroutine.
	release()
}

// noopReleaser is for transports that own their connection for their whole
// lifetime and have nothing to hand back per exchange. A zero-size struct
// converts to an interface without allocating, so noRelease is free to return.
type noopReleaser struct{}

func (noopReleaser) release() {}

// noRelease is the shared value every no-op site returns.
var noRelease releaser = noopReleaser{}

// funcReleaser adapts an existing release func to releaser, for the transports
// whose acquire helper already hands one back. A func value is pointer-shaped,
// so it goes into the interface without allocating — the wrapper costs nothing
// and keeps this change to the transport boundary rather than rewriting every
// acquire helper's signature.
type funcReleaser func()

func (f funcReleaser) release() { f() }
