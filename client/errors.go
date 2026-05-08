package client

import (
	"errors"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/frame"
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
	Code frame.ErrCode
}

// Error implements the error interface.
func (e *StreamResetError) Error() string {
	return fmt.Sprintf("client: stream reset by peer: %v", e.Code)
}

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

	// ErrPoolExhausted is returned when MaxConnsPerHost is reached
	// AND every conn is at its effective stream cap AND the caller
	// declines to wait. Reserved for a future non-blocking acquire;
	// today acquires always queue and ctx / AcquireTimeout governs.
	ErrPoolExhausted = errors.New("client: pool exhausted")

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
)
