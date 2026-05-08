// Package client — retry layer.
package client

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// isIdempotent reports whether req may be retried after a transport
// failure. The Idempotent field overrides the Method-based default.
func isIdempotent(req *Request) bool {
	if req.Idempotent != nil {
		return *req.Idempotent
	}
	switch req.Method {
	case "GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE":
		return true
	}
	return false
}

// builtinShouldRetry returns true for transport errors RFC 7540 or
// our pool layer explicitly permits to retry. ctx errors and terminal
// errors (pool/client closed, invalid request) return false so the
// retry loop short-circuits before consulting the user predicate.
func builtinShouldRetry(err error) bool {
	if err == nil {
		return false
	}
	var sre *StreamResetError
	if errors.As(err, &sre) && sre.Code == frame.ErrCodeRefusedStream {
		return true
	}
	if errors.Is(err, conn.ErrGoAway) {
		return true
	}
	var de *DialError
	if errors.As(err, &de) {
		return true
	}
	if errors.Is(err, ErrDialBackoff) {
		return true
	}
	return false
}

// defaultBackoff implements truncated exponential backoff with ±25%
// uniform jitter. Sequence (no jitter): attempt=0 → 0, 1 → 100ms,
// 2 → 200ms, 3 → 400ms, … capped at 5s base. rng must be non-nil.
func defaultBackoff(attempt int, rng *rand.Rand) time.Duration {
	if attempt <= 0 {
		return 0
	}
	const (
		base = 100 * time.Millisecond
		max  = 5 * time.Second
	)
	d := base << uint(attempt-1)
	if d > max || d <= 0 {
		d = max
	}
	delta := time.Duration(rng.Int63n(int64(d/2))) - d/4
	return d + delta
}

// RetryOptions configures the Retryer.
type RetryOptions struct {
	// MaxAttempts is the maximum total attempts (1 = no retry).
	// Zero → 3 default.
	MaxAttempts int

	// Backoff returns the wait before attempt i (0-indexed; 0 must
	// return 0). nil → defaultBackoff (100ms…5s + ±25% jitter).
	Backoff func(attempt int) time.Duration

	// IsRetryable supplements the built-in classification. Called for
	// any err / resp not auto-retried. nil → only built-ins retry.
	IsRetryable func(err error, resp *Response) bool

	// Rand seeds the jitter source for the default backoff. nil →
	// time-seeded *rand.Rand owned by the Retryer.
	Rand *rand.Rand
}

// retryDoer is the unexported seam Retryer drives. *Client satisfies
// it implicitly. Tests inject a fake to drive the loop without a real
// transport.
type retryDoer interface {
	Do(ctx context.Context, req *Request) (*Response, error)
	DoStream(ctx context.Context, req *Request) (*StreamResponse, error)
}

// Retryer wraps a transport with bounded automatic retry.
type Retryer struct {
	d     retryDoer
	opts  RetryOptions
	rng   *rand.Rand
	rngMu sync.Mutex
}

// NewRetryer constructs a Retryer wrapping c. Zero-value fields in
// opts are filled with defaults; non-zero values are preserved
// verbatim. The returned *Retryer is goroutine-safe.
func NewRetryer(c *Client, opts RetryOptions) *Retryer {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	rng := opts.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if opts.Backoff == nil {
		// Capture rng so the user can swap their own backoff without
		// pulling our jitter source.
		opts.Backoff = func(attempt int) time.Duration {
			return defaultBackoff(attempt, rng)
		}
	}
	return &Retryer{d: c, opts: opts, rng: rng}
}

// canRetry reports whether the Retryer should attempt more than one
// call. It does NOT include classification of any specific error —
// that happens inside the loop on each attempt.
func (r *Retryer) canRetry(req *Request) bool {
	if r.opts.MaxAttempts <= 1 {
		return false
	}
	if !isIdempotent(req) {
		return false
	}
	if req.BodyReader != nil {
		return false
	}
	return true
}

// Do issues req with retries on transient failures. Falls through to
// a single Client.Do call when retry is disabled by configuration or
// the request itself (non-idempotent / BodyReader / MaxAttempts<=1).
func (r *Retryer) Do(ctx context.Context, req *Request) (*Response, error) {
	if req == nil {
		return r.d.Do(ctx, req) // surface ErrInvalidRequest from validate
	}
	if !r.canRetry(req) {
		return r.d.Do(ctx, req)
	}
	return r.doLoop(ctx, req)
}

// doLoop is the actual retry loop. Pre: canRetry(req) == true.
func (r *Retryer) doLoop(ctx context.Context, req *Request) (*Response, error) {
	// Stub for now — Task 4 implements the loop body.
	return r.d.Do(ctx, req)
}
