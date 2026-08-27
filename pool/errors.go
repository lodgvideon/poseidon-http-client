package pool

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the resolution and selection vocabulary.
//
// The messages keep their original "client: " prefix. They were minted in that
// package and are aliased back into it, so changing the text here would change
// what every existing caller's logs and any message-matching test see, for a
// package boundary the caller never asked about.
var (
	// ErrWatchUnsupported is returned by a Resolver.Watch implementation
	// that does not support push-style updates. The managedPool falls
	// back to a ticker around Resolve when it sees this error.
	ErrWatchUnsupported = errors.New("client: resolver does not support Watch")

	// ErrNoAddresses is returned when a Resolver yields zero addresses
	// AND has no cached set to fall back on, or when a Selector receives
	// an empty candidate set.
	ErrNoAddresses = errors.New("client: resolver returned no addresses")

	// ErrNilKeyFn is returned by Hash when keyFn is nil.
	ErrNilKeyFn = errors.New("client: Hash selector requires a non-nil keyFn")
)

// DialError wraps the underlying dial error and the address that
// failed. Returned from Do/DoStream when the lazy dial fails.
type DialError struct {
	Addr string
	Err  error
}

// Error implements the error interface.
func (e *DialError) Error() string {
	return fmt.Sprintf("client: dial %s: %v", e.Addr, e.Err)
}

// Unwrap exposes the underlying error for errors.Is / errors.As.
func (e *DialError) Unwrap() error { return e.Err }

// Pool-related errors.
var (
	// ErrPoolClosed is returned by Pool operations after Close.
	ErrPoolClosed = errors.New("client: pool closed")

	// ErrAcquireTimeout is returned when Options.AcquireTimeout
	// elapses before capacity becomes available.
	ErrAcquireTimeout = errors.New("client: acquire timeout")

	// ErrDialBackoff is returned when a recent dial failure on the
	// pool is still within the DialBackoff window.
	ErrDialBackoff = errors.New("client: dial backoff active")
)
