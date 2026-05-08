// Package client — retry layer.
package client

import (
	"errors"
	"math/rand"
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
