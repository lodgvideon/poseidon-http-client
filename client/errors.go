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
	// did not contain a parseable :status pseudo-header.
	ErrEmptyResponse = errors.New("client: response missing :status")
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
