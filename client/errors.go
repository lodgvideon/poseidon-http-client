package client

import (
	"errors"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Sentinel errors returned (or wrapped via %w) from the client.
var (
	// ErrInvalidRequest indicates a Request failed up-front validation.
	// Wrapped errors carry a human-readable detail.
	ErrInvalidRequest = errors.New("client: invalid request")

	// ErrClosed is returned from Do, DoStream, or transport.acquire
	// after Client.Close has been called.
	ErrClosed = errors.New("client: closed")

	// ErrRedialBackoff is returned when a previous dial attempt
	// failed and the configured DialBackoff window has not elapsed.
	ErrRedialBackoff = errors.New("client: redial in backoff window")

	// ErrEmptyResponse is returned when the response HEADERS frame
	// did not contain a :status pseudo-header at all.
	ErrEmptyResponse = errors.New("client: response missing :status")

	// ErrInvalidStatus is returned when the :status pseudo-header is
	// present but does not parse as a non-negative integer. Distinct
	// from ErrEmptyResponse so retry logic can treat the two separately.
	ErrInvalidStatus = errors.New("client: response :status is not a valid integer")
)

// StreamResetError is returned from Do (or surfaced via DoStream's
// EventReset) when the peer sends RST_STREAM mid-response.
type StreamResetError struct {
	Code conn.ErrCode
}

// Error implements the error interface.
func (e *StreamResetError) Error() string {
	return fmt.Sprintf("client: stream reset by peer: %v", e.Code)
}

// Unwrap returns nil. Provided for structural consistency with [DialError]
// so error-handling code can uniformly call errors.Is/As on client errors.
func (e *StreamResetError) Unwrap() error { return nil }

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

// Pool-related errors. Used by the TransportPool transport.
var (
	// ErrPoolClosed is returned by Pool operations after Close.
	ErrPoolClosed = errors.New("client: pool closed")

	// ErrAcquireTimeout is returned when PoolOptions.AcquireTimeout
	// elapses before capacity becomes available.
	ErrAcquireTimeout = errors.New("client: acquire timeout")

	// ErrDialBackoff is returned when a recent dial failure on the
	// pool is still within the DialBackoff window.
	ErrDialBackoff = errors.New("client: dial backoff active")

	// ErrInvalidPoolOptions is returned by NewClient when Transport
	// and Pool are inconsistent.
	ErrInvalidPoolOptions = errors.New("client: invalid pool options")

	// ErrInvalidTransportKind is returned by NewClient when
	// ClientOptions.Transport is not a defined TransportKind.
	ErrInvalidTransportKind = errors.New("client: invalid transport kind")

	// ErrWatchUnsupported is returned by a Resolver.Watch implementation
	// that does not support push-style updates. The managedPool falls
	// back to a ticker around Resolve when it sees this error.
	ErrWatchUnsupported = errors.New("client: resolver does not support Watch")

	// ErrNoAddresses is returned when a Resolver yields zero addresses
	// AND has no cached set to fall back on, or when a Selector receives
	// an empty candidate set.
	ErrNoAddresses = errors.New("client: resolver returned no addresses")

	// ErrInvalidOptions is returned by NewClient when ClientOptions are
	// internally inconsistent (e.g. both Addr and Resolver supplied).
	ErrInvalidOptions = errors.New("client: invalid ClientOptions")

	// ErrBodyTooLarge is returned when the response body (compressed or
	// decompressed) exceeds the configured maximum size, preventing
	// memory-exhaustion attacks such as gzip bombs.
	ErrBodyTooLarge = errors.New("client: response body exceeds maximum size")

	// ErrNilKeyFn is returned by Hash when keyFn is nil.
	ErrNilKeyFn = errors.New("client: Hash selector requires a non-nil keyFn")

	// ErrTrailersUnsupportedH1 is returned when a request carrying trailers
	// is sent over an HTTP/1.1 connection. HTTP/1.1 request trailers require
	// chunked transfer-coding with a trailer section, which this fallback
	// transport does not implement; the request is rejected rather than
	// corrupting the connection with a second request line.
	ErrTrailersUnsupportedH1 = errors.New("client: HTTP/1.1 does not support request trailers")
)
