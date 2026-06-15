package client

import (
	"context"
	"sync"
	"time"
)

// rateLimiter is a simple token-bucket rate limiter. Tokens replenish
// at a fixed rate up to a maximum burst. Use Take(ctx) to acquire one
// token; blocks until a token is available or ctx is cancelled.
//
// The implementation is goroutine-safe (sync.Mutex + Cond). A
// monotonic clock is used to track elapsed time, so wall-clock
// adjustments do not affect rate.
type rateLimiter struct {
	mu         sync.Mutex
	cond       *sync.Cond
	rps        float64
	burst      float64
	tokens     float64
	lastRefill time.Time
}

// newRateLimiter constructs a rate limiter that allows up to rps
// operations per second, with a burst capacity of burst tokens. Both
// values must be positive.
func newRateLimiter(rps, burst float64) *rateLimiter {
	rl := &rateLimiter{
		rps:        rps,
		burst:      burst,
		tokens:     burst,
		lastRefill: time.Now(),
	}
	rl.cond = sync.NewCond(&rl.mu)
	return rl
}

// refillLocked adds tokens based on time elapsed since last refill.
// Caller must hold rl.mu.
func (rl *rateLimiter) refillLocked() {
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	if elapsed > 0 {
		rl.tokens += elapsed * rl.rps
		if rl.tokens > rl.burst {
			rl.tokens = rl.burst
		}
		rl.lastRefill = now
	}
}

// Take blocks until a token is available or ctx is cancelled. Returns
// the error from ctx.Err() on cancellation.
func (rl *rateLimiter) Take(ctx context.Context) error {
	// Fast path: check if a token is available without locking.
	rl.mu.Lock()
	rl.refillLocked()
	if rl.tokens >= 1 {
		rl.tokens--
		rl.mu.Unlock()
		return nil
	}
	// Slow path: wait for a token. Compute wait time and either sleep
	// (if no deadline) or use a goroutine with select on ctx.
	if ctx.Done() == nil {
		// Pathological: context without Done channel. Fall back to
		// blocking wait.
		for rl.tokens < 1 {
			rl.refillLocked()
			if rl.tokens < 1 {
				wait := time.Duration((1 - rl.tokens) / rl.rps * float64(time.Second))
				rl.mu.Unlock()
				time.Sleep(wait)
				rl.mu.Lock()
			}
		}
		rl.tokens--
		rl.mu.Unlock()
		return nil
	}
	// Cancellable wait loop.
	for {
		rl.refillLocked()
		if rl.tokens >= 1 {
			rl.tokens--
			rl.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - rl.tokens) / rl.rps * float64(time.Second))
		rl.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
			rl.mu.Lock()
		}
	}
}

// Allow reports whether a token is available right now (non-blocking).
func (rl *rateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.refillLocked()
	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}
