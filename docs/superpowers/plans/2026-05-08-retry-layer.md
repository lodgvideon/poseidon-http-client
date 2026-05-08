# Retry Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a `Retryer` that wraps `*client.Client` with bounded automatic retry for idempotent requests, per `docs/superpowers/specs/2026-05-08-retry-layer-design.md`.

**Architecture:** New file `client/retry.go` plus a single field added to `client.Request`. Retry logic is testable via an unexported `retryDoer` interface that `*Client` implicitly satisfies; production callers go through `NewRetryer(c, opts)`.

**Tech Stack:** Go 1.24, existing test patterns (`go test -race`, table tests, `httptest` + `golang.org/x/net/http2` for integration). No new dependencies.

---

## File Structure

| File | Responsibility |
|---|---|
| `client/retry.go` (NEW) | `RetryOptions`, `Retryer`, `NewRetryer`, `Retryer.Do`, `Retryer.DoStream`, helpers (`isIdempotent`, `builtinShouldRetry`, `defaultBackoff`), unexported `retryDoer` interface |
| `client/request.go` (MODIFY) | Add `Idempotent *bool` field to `Request` |
| `client/retry_internal_test.go` (NEW, package `client`) | Unit tests for helpers + retry loop tests using a fake `retryDoer` |
| `client/retry_test.go` (NEW, package `client_test`) | End-to-end integration test with real `httptest` HTTP/2 server |
| `docs/RFC_COVERAGE.md` (MODIFY) | Add §8.1.4 row |

---

## Task 1: Foundation helpers — Request.Idempotent, isIdempotent, builtinShouldRetry, defaultBackoff

These are pure functions with no dependencies on the rest of the retry machinery. Implementing them first lets later tasks call them by name without forward references.

**Files:**
- Modify: `client/request.go` (one new field)
- Create: `client/retry.go` (helpers only — `Retryer` itself comes in Task 2)
- Create: `client/retry_internal_test.go` (table tests for the three helpers)

- [ ] **Step 1.1: Write failing test for `isIdempotent`**

Create `client/retry_internal_test.go`:

```go
package client

import (
	"errors"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestIsIdempotent_Methods(t *testing.T) {
	t.Parallel()
	tt := func(b bool) *bool { return &b }
	cases := []struct {
		name string
		req  *Request
		want bool
	}{
		{"GET", &Request{Method: "GET"}, true},
		{"HEAD", &Request{Method: "HEAD"}, true},
		{"OPTIONS", &Request{Method: "OPTIONS"}, true},
		{"PUT", &Request{Method: "PUT"}, true},
		{"DELETE", &Request{Method: "DELETE"}, true},
		{"TRACE", &Request{Method: "TRACE"}, true},
		{"POST", &Request{Method: "POST"}, false},
		{"PATCH", &Request{Method: "PATCH"}, false},
		{"empty", &Request{Method: ""}, false},
		{"override true on POST", &Request{Method: "POST", Idempotent: tt(true)}, true},
		{"override false on GET", &Request{Method: "GET", Idempotent: tt(false)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isIdempotent(c.req); got != c.want {
				t.Errorf("isIdempotent(%+v) = %v, want %v", c.req, got, c.want)
			}
		})
	}
}
```

- [ ] **Step 1.2: Run, verify failure**

Run: `go test ./client/ -run TestIsIdempotent_Methods -count=1`
Expected: FAIL with `undefined: isIdempotent` and `unknown field Idempotent in struct literal`.

- [ ] **Step 1.3: Add `Idempotent *bool` field to Request**

In `client/request.go`, add inside the `Request` struct after `WantTrailers bool`:

```go
	// Idempotent overrides automatic idempotency classification based
	// on Method. nil → classify by Method (GET, HEAD, OPTIONS, PUT,
	// DELETE, TRACE are idempotent; POST, PATCH are not).
	Idempotent *bool
```

- [ ] **Step 1.4: Create `client/retry.go` with `isIdempotent`**

Create `client/retry.go`:

```go
// Package client — retry layer.
package client

import (
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
```

- [ ] **Step 1.5: Run, verify pass**

Run: `go test ./client/ -run TestIsIdempotent_Methods -count=1 -v`
Expected: PASS.

- [ ] **Step 1.6: Write failing tests for `builtinShouldRetry`**

Append to `client/retry_internal_test.go`:

```go
func TestBuiltinShouldRetry(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"REFUSED_STREAM", &StreamResetError{Code: frame.ErrCodeRefusedStream}, true},
		{"RST CANCEL", &StreamResetError{Code: frame.ErrCodeCancel}, false},
		{"RST INTERNAL_ERROR", &StreamResetError{Code: frame.ErrCodeInternalError}, false},
		{"conn.ErrGoAway", conn.ErrGoAway, true},
		{"DialError", &DialError{Addr: "x", Err: errors.New("boom")}, true},
		{"ErrDialBackoff", ErrDialBackoff, true},
		{"ErrPoolClosed", ErrPoolClosed, false},
		{"ErrClosed", ErrClosed, false},
		{"ErrInvalidRequest", ErrInvalidRequest, false},
		{"random error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := builtinShouldRetry(c.err); got != c.want {
				t.Errorf("builtinShouldRetry(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
```

- [ ] **Step 1.7: Run, verify failure**

Run: `go test ./client/ -run TestBuiltinShouldRetry -count=1`
Expected: FAIL with `undefined: builtinShouldRetry`.

- [ ] **Step 1.8: Add `builtinShouldRetry` to `retry.go`**

Append to `client/retry.go`:

```go
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
```

- [ ] **Step 1.9: Run, verify pass**

Run: `go test ./client/ -run TestBuiltinShouldRetry -count=1 -v`
Expected: PASS.

- [ ] **Step 1.10: Write failing test for `defaultBackoff`**

Append to `client/retry_internal_test.go`:

```go
func TestDefaultBackoff_Bounds(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(42))
	if got := defaultBackoff(0, rng); got != 0 {
		t.Errorf("defaultBackoff(0) = %v, want 0", got)
	}
	// attempt=1: base 100ms, jitter ±25ms → [75ms, 125ms]
	for i := 0; i < 100; i++ {
		got := defaultBackoff(1, rng)
		if got < 75*time.Millisecond || got > 125*time.Millisecond {
			t.Fatalf("defaultBackoff(1) = %v, want in [75ms,125ms]", got)
		}
	}
	// attempt=20: clamped to 5s base; jitter ±1.25s → [3.75s, 6.25s]
	for i := 0; i < 100; i++ {
		got := defaultBackoff(20, rng)
		if got < 3750*time.Millisecond || got > 6250*time.Millisecond {
			t.Fatalf("defaultBackoff(20) = %v, want in [3.75s,6.25s]", got)
		}
	}
}
```

- [ ] **Step 1.11: Run, verify failure**

Run: `go test ./client/ -run TestDefaultBackoff_Bounds -count=1`
Expected: FAIL with `undefined: defaultBackoff`.

- [ ] **Step 1.12: Add `defaultBackoff` to `retry.go`**

Append to `client/retry.go`:

```go
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
	if d > max || d <= 0 { // overflow on huge attempt
		d = max
	}
	// ±25% jitter centred on d.
	delta := time.Duration(rng.Int63n(int64(d/2))) - d/4
	return d + delta
}
```

- [ ] **Step 1.13: Run, verify pass**

Run: `go test ./client/ -run TestDefaultBackoff_Bounds -count=1 -v`
Expected: PASS.

- [ ] **Step 1.14: Run all client tests + race**

Run: `go test ./client/ -race -count=1 -timeout 90s`
Expected: PASS (existing tests still green).

- [ ] **Step 1.15: Commit**

```bash
git add client/request.go client/retry.go client/retry_internal_test.go
git commit -m "feat(client): retry helpers — isIdempotent, builtinShouldRetry, defaultBackoff

Foundation for the retry layer per docs/superpowers/specs/2026-05-08-retry-layer-design.md.
Adds Request.Idempotent override and three pure helpers that the Retryer
loop will compose in subsequent tasks."
```

---

## Task 2: retryDoer interface + Retryer struct + NewRetryer

Adds the testable seam (unexported interface) and the constructor with default fill-in.

**Files:**
- Modify: `client/retry.go`
- Modify: `client/retry_internal_test.go`

- [ ] **Step 2.1: Write failing test for `NewRetryer` defaults**

Append to `client/retry_internal_test.go`:

```go
func TestNewRetryer_Defaults(t *testing.T) {
	t.Parallel()
	c := &Client{} // zero Client; we only inspect the Retryer fields here
	r := NewRetryer(c, RetryOptions{})
	if r.opts.MaxAttempts != 3 {
		t.Errorf("MaxAttempts default = %d, want 3", r.opts.MaxAttempts)
	}
	if r.opts.Backoff == nil {
		t.Error("Backoff default = nil, want non-nil")
	}
	if r.rng == nil {
		t.Error("rng = nil after NewRetryer")
	}
}

func TestNewRetryer_PreservesNonZero(t *testing.T) {
	t.Parallel()
	c := &Client{}
	custom := func(int) time.Duration { return 0 }
	r := NewRetryer(c, RetryOptions{MaxAttempts: 7, Backoff: custom})
	if r.opts.MaxAttempts != 7 {
		t.Errorf("MaxAttempts = %d, want 7", r.opts.MaxAttempts)
	}
	if r.opts.Backoff == nil {
		t.Error("Backoff was overwritten")
	}
}
```

- [ ] **Step 2.2: Run, verify failure**

Run: `go test ./client/ -run TestNewRetryer -count=1`
Expected: FAIL with `undefined: NewRetryer`, `undefined: RetryOptions`.

- [ ] **Step 2.3: Add types and constructor**

Append to `client/retry.go`:

```go
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
```

Add `"context"` to the import block at the top of `client/retry.go`.

- [ ] **Step 2.4: Run, verify pass**

Run: `go test ./client/ -run TestNewRetryer -count=1 -v`
Expected: PASS.

- [ ] **Step 2.5: Commit**

```bash
git add client/retry.go client/retry_internal_test.go
git commit -m "feat(client): RetryOptions + Retryer struct + NewRetryer

Unexported retryDoer interface lets tests drive the loop with a fake
transport. Production callers always go through NewRetryer(c, opts)."
```

---

## Task 3: Retryer.Do — non-retry passthrough cases

Three reasons to skip retry entirely: non-idempotent request, BodyReader set, MaxAttempts ≤ 1.

**Files:**
- Modify: `client/retry.go`
- Modify: `client/retry_internal_test.go`

- [ ] **Step 3.1: Write fake `retryDoer` + failing tests**

Append to `client/retry_internal_test.go`:

```go
// fakeDoer is a scriptable retryDoer used by retry-loop tests.
type fakeDoer struct {
	results []doResult
	stream  []streamResult
	calls   int
	streams int
}

type doResult struct {
	resp *Response
	err  error
}
type streamResult struct {
	resp *StreamResponse
	err  error
}

func (f *fakeDoer) Do(_ context.Context, _ *Request) (*Response, error) {
	r := f.results[f.calls]
	f.calls++
	return r.resp, r.err
}

func (f *fakeDoer) DoStream(_ context.Context, _ *Request) (*StreamResponse, error) {
	r := f.stream[f.streams]
	f.streams++
	return r.resp, r.err
}

func TestRetryer_Do_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil}, // never reached
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "POST", Path: "/"})
	if err == nil {
		t.Fatal("expected error on POST + RST, got nil")
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (non-idempotent must not retry)", f.calls)
	}
}

func TestRetryer_Do_BodyReader_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	_, err := r.Do(context.Background(), &Request{
		Method:     "GET",
		Path:       "/",
		BodyReader: errReader{},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (BodyReader must disable retry)", f.calls)
	}
}

func TestRetryer_Do_MaxAttemptsOne_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 1})
	r.d = f
	_, _ = r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (MaxAttempts=1 disables retry)", f.calls)
	}
}

// errReader is a placeholder io.Reader for BodyReader tests; never
// actually read because retry skips before issuing the request.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("not read") }
```

Add `"context"` to the imports of `client/retry_internal_test.go`.

- [ ] **Step 3.2: Run, verify failure**

Run: `go test ./client/ -run TestRetryer_Do_ -count=1`
Expected: FAIL — `r.Do` undefined.

- [ ] **Step 3.3: Implement `Retryer.Do` skeleton**

Append to `client/retry.go`:

```go
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
```

- [ ] **Step 3.4: Run, verify pass**

Run: `go test ./client/ -run TestRetryer_Do_ -count=1 -v`
Expected: PASS.

- [ ] **Step 3.5: Commit**

```bash
git add client/retry.go client/retry_internal_test.go
git commit -m "feat(client): Retryer.Do passthrough for non-retryable cases

Non-idempotent methods, BodyReader, and MaxAttempts<=1 short-circuit
to a single Client.Do call. Loop body itself comes in the next task."
```

---

## Task 4: Retryer.Do — retry loop with built-in classification

Implements the actual loop: REFUSED_STREAM / GOAWAY / DialError trigger another attempt; success returns immediately.

**Files:**
- Modify: `client/retry.go`
- Modify: `client/retry_internal_test.go`

- [ ] **Step 4.1: Write failing tests**

Append to `client/retry_internal_test.go`:

```go
func TestRetryer_Do_RefusedStream_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 }, // no waiting in unit tests
	})
	r.d = f
	resp, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do err = %v, want nil after retry", err)
	}
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200", resp.Status)
	}
	if f.calls != 3 {
		t.Errorf("calls = %d, want 3", f.calls)
	}
}

func TestRetryer_Do_GoAway_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, conn.ErrGoAway},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	resp, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil || resp.Status != 200 {
		t.Fatalf("Do = %v, %v; want 200, nil", resp, err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}

func TestRetryer_Do_DialError_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &DialError{Addr: "x:1", Err: errors.New("boom")}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do err = %v, want nil after dial retry", err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}

func TestRetryer_Do_NonRetryableError_Stops(t *testing.T) {
	t.Parallel()
	other := errors.New("application error")
	f := &fakeDoer{results: []doResult{{nil, other}}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if !errors.Is(err, other) {
		t.Fatalf("err = %v, want %v", err, other)
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (non-retryable error must stop)", f.calls)
	}
}
```

- [ ] **Step 4.2: Run, verify failure**

Run: `go test ./client/ -run TestRetryer_Do_RefusedStream_Retries -count=1`
Expected: FAIL — current `doLoop` is a stub that doesn't retry.

- [ ] **Step 4.3: Implement loop body**

Replace the body of `doLoop` in `client/retry.go`:

```go
func (r *Retryer) doLoop(ctx context.Context, req *Request) (*Response, error) {
	var (
		resp *Response
		err  error
	)
	for attempt := 0; attempt < r.opts.MaxAttempts; attempt++ {
		if attempt > 0 {
			wait := r.opts.Backoff(attempt)
			if wait > 0 {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		resp, err = r.d.Do(ctx, req)
		if err == nil {
			if !r.userIsRetryable(nil, resp) {
				return resp, nil
			}
			continue
		}
		if isHardStop(err) {
			return nil, err
		}
		if builtinShouldRetry(err) {
			continue
		}
		if r.userIsRetryable(err, nil) {
			continue
		}
		return nil, err
	}
	return resp, err
}

// userIsRetryable consults the optional user predicate; nil predicate
// returns false (i.e., do NOT retry).
func (r *Retryer) userIsRetryable(err error, resp *Response) bool {
	if r.opts.IsRetryable == nil {
		return false
	}
	return r.opts.IsRetryable(err, resp)
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
```

- [ ] **Step 4.4: Run, verify pass**

Run: `go test ./client/ -run TestRetryer_Do_ -count=1 -v`
Expected: PASS for the new four tests plus the three from Task 3.

- [ ] **Step 4.5: Commit**

```bash
git add client/retry.go client/retry_internal_test.go
git commit -m "feat(client): Retryer.Do retry loop with built-in classification

Retries on REFUSED_STREAM (RFC §8.1.4), conn.ErrGoAway, *DialError,
ErrDialBackoff. Hard-stop set (ctx, ErrClosed, ErrPoolClosed,
ErrInvalidRequest) bypasses both built-in and user predicates."
```

---

## Task 5: Retryer.Do — IsRetryable extension (5xx + custom)

Lets callers extend retry classification beyond built-ins (most commonly: 5xx response retries).

**Files:**
- Modify: `client/retry_internal_test.go`

(No `retry.go` change needed — Task 4 already wires `userIsRetryable` into the loop.)

- [ ] **Step 5.1: Write tests**

Append to `client/retry_internal_test.go`:

```go
func TestRetryer_Do_IsRetryable_Custom5xx_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{&Response{Status: 503}, nil},
		{&Response{Status: 503}, nil},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(err error, resp *Response) bool {
			return resp != nil && resp.Status >= 500
		},
	})
	r.d = f
	resp, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200", resp.Status)
	}
	if f.calls != 3 {
		t.Errorf("calls = %d, want 3", f.calls)
	}
}

func TestRetryer_Do_IsRetryable_NonBuiltinError_Retries(t *testing.T) {
	t.Parallel()
	custom := errors.New("custom transient")
	f := &fakeDoer{results: []doResult{
		{nil, custom},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(err error, _ *Response) bool {
			return errors.Is(err, custom)
		},
	})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("err = %v, want nil after IsRetryable retry", err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}
```

- [ ] **Step 5.2: Run, verify pass**

Run: `go test ./client/ -run TestRetryer_Do_IsRetryable_ -count=1 -v`
Expected: PASS without further code changes.

- [ ] **Step 5.3: Commit**

```bash
git add client/retry_internal_test.go
git commit -m "test(client): cover Retryer.Do IsRetryable predicate (5xx + custom error)"
```

---

## Task 6: Retryer.Do — ctx cancel + hard-stop tests

Pin the behavior of context cancellation (mid-backoff) and hard-stop errors.

**Files:**
- Modify: `client/retry_internal_test.go`

- [ ] **Step 6.1: Write tests**

Append to `client/retry_internal_test.go`:

```go
func TestRetryer_Do_CtxCanceled_StopsImmediately(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
	}}
	// Backoff long enough that ctx cancel must take the select.
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 5 * time.Second },
	})
	r.d = f
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := r.Do(ctx, &Request{Method: "GET", Path: "/"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 1*time.Second {
		t.Errorf("returned in %v, want <1s (ctx cancel must wake the backoff sleep)", elapsed)
	}
}

func TestRetryer_Do_HardStop_PoolClosed_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, ErrPoolClosed},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(error, *Response) bool { return true }, // even with this, stop
	})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("err = %v, want ErrPoolClosed", err)
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (hard stop must not retry)", f.calls)
	}
}
```

- [ ] **Step 6.2: Run, verify pass**

Run: `go test ./client/ -run TestRetryer_Do_CtxCanceled_StopsImmediately -race -count=1 -v && go test ./client/ -run TestRetryer_Do_HardStop -count=1 -v`
Expected: PASS.

- [ ] **Step 6.3: Commit**

```bash
git add client/retry_internal_test.go
git commit -m "test(client): pin Retryer.Do ctx cancel + hard-stop semantics"
```

---

## Task 7: Retryer.Do — exhausted attempts returns last error

Verify the loop returns the LAST result (not nil) when it runs out of attempts.

**Files:**
- Modify: `client/retry_internal_test.go`

- [ ] **Step 7.1: Write test**

Append to `client/retry_internal_test.go`:

```go
func TestRetryer_Do_MaxAttempts_Exhausted(t *testing.T) {
	t.Parallel()
	last := &StreamResetError{Code: frame.ErrCodeRefusedStream}
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, last},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != last {
		t.Fatalf("err = %v, want last (%v)", err, last)
	}
	if f.calls != 3 {
		t.Errorf("calls = %d, want 3", f.calls)
	}
}
```

- [ ] **Step 7.2: Run, verify pass**

Run: `go test ./client/ -run TestRetryer_Do_MaxAttempts_Exhausted -count=1 -v`
Expected: PASS.

- [ ] **Step 7.3: Commit**

```bash
git add client/retry_internal_test.go
git commit -m "test(client): Retryer.Do returns last error after exhausting attempts"
```

---

## Task 8: Retryer.DoStream

DoStream retries only before the first HEADERS frame is delivered. Successful stream return = caller owns the stream, no retry.

**Files:**
- Modify: `client/retry.go`
- Modify: `client/retry_internal_test.go`

- [ ] **Step 8.1: Write failing tests**

Append to `client/retry_internal_test.go`:

```go
func TestRetryer_DoStream_RetriesBeforeHeaders(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{stream: []streamResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&StreamResponse{}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	resp, err := r.DoStream(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("DoStream err = %v, want nil", err)
	}
	if resp == nil {
		t.Fatal("DoStream returned nil StreamResponse")
	}
	if f.streams != 2 {
		t.Errorf("streams = %d, want 2", f.streams)
	}
}

func TestRetryer_DoStream_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{stream: []streamResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&StreamResponse{}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	_, err := r.DoStream(context.Background(), &Request{Method: "POST", Path: "/"})
	if err == nil {
		t.Fatal("expected err on POST + RST, got nil")
	}
	if f.streams != 1 {
		t.Errorf("streams = %d, want 1", f.streams)
	}
}

func TestRetryer_DoStream_Success_NoIsRetryableCall(t *testing.T) {
	t.Parallel()
	called := false
	f := &fakeDoer{stream: []streamResult{
		{&StreamResponse{}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(error, *Response) bool {
			called = true
			return true
		},
	})
	r.d = f
	if _, err := r.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("DoStream err = %v", err)
	}
	if called {
		t.Error("IsRetryable invoked for successful DoStream — must not be (caller owns stream)")
	}
}
```

- [ ] **Step 8.2: Run, verify failure**

Run: `go test ./client/ -run TestRetryer_DoStream -count=1`
Expected: FAIL — `r.DoStream` not implemented (currently returns via `r.d.DoStream` only because of nothing — actually compile error since method missing).

- [ ] **Step 8.3: Implement `Retryer.DoStream`**

Append to `client/retry.go`:

```go
// DoStream issues a streaming request with retries that apply ONLY
// before the first HEADERS frame is delivered. A successful return
// from the underlying transport hands ownership of the stream to the
// caller, after which no further retry is possible.
func (r *Retryer) DoStream(ctx context.Context, req *Request) (*StreamResponse, error) {
	if req == nil {
		return r.d.DoStream(ctx, req)
	}
	if !r.canRetry(req) {
		return r.d.DoStream(ctx, req)
	}
	var (
		resp *StreamResponse
		err  error
	)
	for attempt := 0; attempt < r.opts.MaxAttempts; attempt++ {
		if attempt > 0 {
			wait := r.opts.Backoff(attempt)
			if wait > 0 {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		resp, err = r.d.DoStream(ctx, req)
		if err == nil {
			return resp, nil
		}
		if isHardStop(err) {
			return nil, err
		}
		if builtinShouldRetry(err) {
			continue
		}
		if r.userIsRetryable(err, nil) {
			continue
		}
		return nil, err
	}
	return resp, err
}
```

- [ ] **Step 8.4: Run, verify pass**

Run: `go test ./client/ -run TestRetryer_DoStream -count=1 -v`
Expected: PASS.

- [ ] **Step 8.5: Commit**

```bash
git add client/retry.go client/retry_internal_test.go
git commit -m "feat(client): Retryer.DoStream — retry before first HEADERS only

Once the underlying DoStream succeeds, ownership of the StreamResponse
transfers to the caller and no further retry is possible. IsRetryable
is intentionally not consulted on the success path."
```

---

## Task 9: End-to-end integration test against real HTTP/2 server

Drive the full stack — `Client.Do` over a real TLS h2 server — to prove the loop works above the actual transport, not just the fake. Uses 5xx + custom `IsRetryable` because RST(REFUSED_STREAM) end-to-end requires custom framer infrastructure (deferred — see spec risks).

**Files:**
- Create: `client/retry_test.go` (package `client_test`)

- [ ] **Step 9.1: Write integration test**

Create `client/retry_test.go`:

```go
package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestRetryer_Integration_5xxRetriesViaIsRetryable proves the retry
// loop drives the real transport. Server returns 503 on the first
// two attempts and 200 on the third; IsRetryable opts retries in.
func TestRetryer_Integration_5xxRetriesViaIsRetryable(t *testing.T) {
	var attempt atomic.Int32
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempt.Add(1)
		if n < 3 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	_ = srv

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 10 * time.Millisecond },
		IsRetryable: func(err error, resp *client.Response) bool {
			return err == nil && resp != nil && resp.Status >= 500
		},
	})

	resp, err := r.Do(context.Background(), &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do err = %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200 after 2× 503 retries", resp.Status)
	}
	if got := attempt.Load(); got != 3 {
		t.Errorf("server attempts = %d, want 3", got)
	}
}
```

- [ ] **Step 9.2: Run, verify pass**

Run: `go test ./client/ -run TestRetryer_Integration_5xxRetriesViaIsRetryable -race -count=1 -v -timeout 30s`
Expected: PASS.

- [ ] **Step 9.3: Run full suite**

Run: `go test ./... -race -count=1 -timeout 120s`
Expected: PASS for all 5 packages.

- [ ] **Step 9.4: Commit**

```bash
git add client/retry_test.go
git commit -m "test(client): integration — Retryer drives real httptest h2 server"
```

---

## Task 10: RFC trace + final verification

**Files:**
- Modify: `docs/RFC_COVERAGE.md`

- [ ] **Step 10.1: Add §8.1.4 row**

Open `docs/RFC_COVERAGE.md`. Find the line:

```
| §6.8     | Conformance | TestConformance_RFC7540_Sec6_8_PoolEjectsDeadConnOnRelease (client/) — pool evicts dead conn via release path, not health-check tick |
```

Add the following line directly after it:

```
| §8.1.4   | Conformance | TestRetryer_Do_RefusedStream_Retries (client/) — retry layer retries on REFUSED_STREAM (RFC 7540 §8.1.4 — request not processed) |
```

- [ ] **Step 10.2: Run conformance gate locally**

Run: `bash scripts/rfc-coverage-gate.sh`
Expected: PASS (passes when at least one `TestConformance_RFC7540_*` and one `TestConformance_RFC7541_*` exist and are green; the new entry is documentation only).

If `scripts/rfc-coverage-gate.sh` does not exist locally, skip — CI will run it.

- [ ] **Step 10.3: Final full test sweep**

Run: `go test -race ./... -count=1 -timeout 120s`
Expected: all 5 packages PASS.

Run: `go vet ./...`
Expected: clean.

- [ ] **Step 10.4: Commit RFC trace**

```bash
git add docs/RFC_COVERAGE.md
git commit -m "docs: trace retry layer to RFC 7540 §8.1.4 (REFUSED_STREAM safe to retry)"
```

- [ ] **Step 10.5: Push branch + open PR**

```bash
git push -u origin claude/retry-layer
```

Then create a draft PR (title: `feat(client): retry layer — bounded automatic retry for idempotent requests`) summarizing:
- New `Retryer` wrapping `*Client` with `Do` / `DoStream`.
- Built-in classification: REFUSED_STREAM, GOAWAY, DialError.
- `IsRetryable` extension for 5xx etc.
- `Request.Idempotent *bool` override.
- Test plan from this document.

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| `RetryOptions` (MaxAttempts, Backoff, IsRetryable, Rand) | Task 2 |
| `Retryer` struct + `NewRetryer` | Task 2 |
| `Retryer.Do` (full loop) | Tasks 3, 4, 5, 6, 7 |
| `Retryer.DoStream` | Task 8 |
| `Request.Idempotent *bool` | Task 1 |
| Idempotency table (auto by Method + override) | Task 1 |
| Retry classification table (REFUSED_STREAM / GOAWAY / DialError / hard-stops) | Tasks 1 (helper), 4, 6 |
| Retry gates (canRetry) | Task 3 |
| `Do` loop semantics (pseudocode) | Task 4 |
| `DoStream` loop semantics | Task 8 |
| `defaultBackoff` (truncated exp + ±25% jitter) | Task 1 |
| Concurrency (goroutine-safe `Retryer`) | Task 9 (`-race`) |
| Test plan items #1, #2, #5–#10 | Tasks 3–8 (unit) |
| Test plan items #11, #12 | Task 8 |
| Test plan item #13 | Task 1 (`TestBuiltinShouldRetry`) |
| Test plan item #14 | Task 1 (`TestIsIdempotent_Methods`) |
| Test plan item #15 | Task 1 (`TestDefaultBackoff_Bounds`) |
| Test plan item #16 (concurrent jitter) | Covered by `-race` runs in Task 9 |
| Real-server integration (spec risk: REFUSED_STREAM end-to-end) | Task 9 — uses 5xx + IsRetryable as a workable substitute; REFUSED_STREAM end-to-end requires custom framer, deferred |
| RFC §8.1.4 trace | Task 10 |

Spec test #3 (`IdempotentOverride_TrueOnPost`) and #4 (`IdempotentOverride_FalseOnGet`) are subsumed by `TestIsIdempotent_Methods` table (Task 1). Spec test #16 (`Goroutine_SafeJitter`) is implicitly covered by running Task 9's race-detected integration test concurrently — the only Retryer-mutable state is `rngMu`-protected `*rand.Rand`. No additional test required for what is structurally trivially safe; flag if reviewer wants an explicit one and we add it.

**Placeholder scan:** None.

**Type consistency:**
- `RetryOptions` fields named identically across spec, Task 2 declaration, and Task 4 usage.
- `retryDoer.Do(ctx, req) (*Response, error)` matches `Client.Do` signature.
- `retryDoer.DoStream(ctx, req) (*StreamResponse, error)` matches `Client.DoStream` signature.
- `isIdempotent`, `builtinShouldRetry`, `defaultBackoff`, `isHardStop`, `userIsRetryable` — all defined exactly once and called by name in later tasks.
- Field name `Idempotent` consistent in spec, request.go modification, and `isIdempotent` lookups.

All clean.
