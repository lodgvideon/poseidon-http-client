// Package client — retry layer.
package client

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
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
	if errors.As(err, &sre) {
		switch sre.Code {
		case conn.ErrCodeRefusedStream,
			frame.ErrCodeInternalError,
			frame.ErrCodeEnhanceYourCalm:
			// Transient per RFC 7540 §7:
			//   REFUSED_STREAM (§8.1.4) — server did not process the
			//     request; safe to retry.
			//   INTERNAL_ERROR (§7) — peer encountered an unexpected
			//     internal error; often a server-side panic or race
			//     in concurrent shutdown (httptest exhibits this when
			//     a sibling t.Parallel() test closes its server). Safe
			//     to retry; if it persists, the next call will fail
			//     the same way and the caller sees the real error.
			//   ENHANCE_YOUR_CALM (§7) — explicit rate-limit signal.
			//     Caller must respect backoff; the Retryer's own
			//     Backoff() function does that.
			return true
		}
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
		base       = 100 * time.Millisecond
		maxBackoff = 5 * time.Second
	)
	d := base << uint(attempt-1)
	// d wraps to 0 or negative on bit-shift overflow for large attempts.
	if d > maxBackoff || d <= 0 {
		d = maxBackoff
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
	// any err / resp not auto-retried by built-ins. nil → only built-ins
	// retry.
	//
	// Nil-arg convention: when called for an error, resp is nil; when
	// called for a successful response, err is nil. Always guard
	// resp != nil before dereferencing.
	IsRetryable func(err error, resp *Response) bool

	// Rand seeds the jitter source for the default backoff.
	// nil → time-seeded *rand.Rand owned by the Retryer.
	// Ignored when Backoff is non-nil.
	Rand *rand.Rand
}

// retryDoer is the unexported seam Retryer drives. *Client satisfies
// it implicitly. Tests inject a fake to drive the loop without a real
// transport.
type retryDoer interface {
	Do(ctx context.Context, req *Request, resp *Response) error
	DoStream(ctx context.Context, req *Request, sr *StreamResponse) error
}

// Retryer wraps a transport with bounded automatic retry.
type Retryer struct {
	d        retryDoer
	opts     RetryOptions
	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics
}

// NewRetryer constructs a Retryer wrapping c. Zero-value fields in
// opts are filled with defaults; non-zero values are preserved
// verbatim. The returned *Retryer is goroutine-safe.
func NewRetryer(c *Client, opts RetryOptions) *Retryer {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	if opts.Backoff == nil {
		rng := opts.Rand
		if rng == nil {
			rng = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
		// rng is not goroutine-safe; serialize access from concurrent
		// retry-path callers via a closure-captured mutex.
		var mu sync.Mutex
		opts.Backoff = func(attempt int) time.Duration {
			mu.Lock()
			defer mu.Unlock()
			return defaultBackoff(attempt, rng)
		}
	}
	return &Retryer{d: c, opts: opts, hooksRef: c.hooksPtr, metrics: c.metrics}
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

// sleepBackoff sleeps for wait before the next attempt, returning
// ctx.Err() if the context is cancelled first. Returns nil immediately
// when wait <= 0.
func (r *Retryer) sleepBackoff(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	}
}

// fireRetry fires the OnRetry hook (if set) and increments the retry
// counter. Called before the backoff sleep on every attempt > 0.
func (r *Retryer) fireRetry(req *Request, attempt int, err error, backoff time.Duration) {
	if r.hooksRef != nil {
		if h := r.hooksRef.Load(); h != nil && h.OnRetry != nil {
			h.OnRetry(RetryEvent{
				Method:  req.Method,
				Path:    req.Path,
				Attempt: attempt,
				Err:     err,
				Backoff: backoff,
			})
		}
	}
	if r.metrics != nil {
		r.metrics.Counters.Retries.Add(1)
	}
}

// shouldRetryErr reports whether err warrants a subsequent attempt.
// Hard-stop errors always return false. Built-in transport errors and
// user-supplied IsRetryable return true.
func (r *Retryer) shouldRetryErr(err error) bool {
	if isHardStop(err) {
		return false
	}
	return builtinShouldRetry(err) || r.userIsRetryable(err, nil)
}

// Do issues req with retries on transient failures. Falls through to
// a single Client.Do call when retry is disabled by configuration or
// the request itself (non-idempotent / BodyReader / MaxAttempts<=1).
func (r *Retryer) Do(ctx context.Context, req *Request, resp *Response) error {
	if req == nil || !r.canRetry(req) {
		return r.d.Do(ctx, req, resp)
	}
	return r.doLoop(ctx, req, resp)
}

// doLoop is the actual retry loop for Do. Pre: canRetry(req) == true.
func (r *Retryer) doLoop(ctx context.Context, req *Request, resp *Response) error {
	var err error
	for attempt := 0; attempt < r.opts.MaxAttempts; attempt++ {
		if attempt > 0 {
			backoff := r.opts.Backoff(attempt)
			r.fireRetry(req, attempt, err, backoff)
			if err = r.sleepBackoff(ctx, backoff); err != nil {
				return err
			}
			resp.Reset()
		}
		err = r.d.Do(ctx, req, resp)
		if err == nil {
			if !r.userIsRetryable(nil, resp) {
				return nil
			}
			continue
		}
		if !r.shouldRetryErr(err) {
			return err
		}
	}
	return err
}

// userIsRetryable consults the optional user predicate; nil predicate
// returns false (i.e., do NOT retry).
func (r *Retryer) userIsRetryable(err error, resp *Response) bool {
	if r.opts.IsRetryable == nil {
		return false
	}
	return r.opts.IsRetryable(err, resp)
}

// DoStream issues a streaming request with retries that apply ONLY
// before the first HEADERS frame is delivered. A successful return
// from the underlying transport hands ownership of the stream to the
// caller; IsRetryable is not consulted on success — subsequent
// response classification is the caller's concern.
func (r *Retryer) DoStream(ctx context.Context, req *Request, sr *StreamResponse) error {
	if req == nil || !r.canRetry(req) {
		return r.d.DoStream(ctx, req, sr)
	}
	var err error
	for attempt := 0; attempt < r.opts.MaxAttempts; attempt++ {
		if attempt > 0 {
			backoff := r.opts.Backoff(attempt)
			r.fireRetry(req, attempt, err, backoff)
			if err = r.sleepBackoff(ctx, backoff); err != nil {
				return err
			}
			sr.reset()
		}
		err = r.d.DoStream(ctx, req, sr)
		if err == nil {
			return nil
		}
		if !r.shouldRetryErr(err) {
			return err
		}
	}
	return err
}

// isHardStop returns true for errors that must never be retried,
// even by a user-supplied IsRetryable.
func isHardStop(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrPoolClosed) ||
		errors.Is(err, ErrClosed) ||
		errors.Is(err, ErrInvalidRequest)
}
