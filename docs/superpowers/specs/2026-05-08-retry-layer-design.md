# Retry Layer — Design Spec

**Date:** 2026-05-08
**Phase:** C (post-C.2)
**Status:** approved, awaiting plan

## Problem

`client.Client` returns transport-level errors verbatim
(`*StreamResetError`, `ErrGoAway`, `*DialError`, `ErrDialBackoff`) and
HTTP error responses (e.g. 5xx) as `*Response{Status: 503}`. Load
generators that target real services need automatic, bounded retry on
transient failures. Hand-rolling a retry loop per call is awkward and
error-prone — particularly the idempotency classification and the
distinction between fatal (ctx cancel, pool closed) and transient
errors.

## Goal

Provide an opt-in retry layer wrapping `*client.Client` that:

1. Retries idempotent requests on RFC-permitted transport errors
   (RST_STREAM `REFUSED_STREAM`, GOAWAY, dial errors).
2. Allows callers to extend retry classification (e.g. 5xx) via a
   pluggable `IsRetryable` callback.
3. Respects context cancellation, request body re-readability, and
   non-idempotent methods.
4. Keeps `client.Client` itself unchanged — retry lives in a separate
   file in package `client`, exposed as `Retryer`.

## Non-goals

- Per-pool / cross-call retry budget. Each `Do` / `DoStream` is
  independent.
- Request hedging (parallel speculative attempts).
- `Retry-After` header parsing.
- Idempotency keys (`Idempotency-Key` header).

## API

### Files

- `client/retry.go` — new file; contains `Retryer`, `RetryOptions`,
  `defaultBackoff`, `builtinShouldRetry`, `isIdempotent`.
- `client/client.go` — single addition: `Request.Idempotent *bool`
  field for explicit idempotency override.
- `client/retry_test.go` — unit + integration tests.
- `docs/RFC_COVERAGE.md` — adds row for §8.1.4 (REFUSED_STREAM safe to
  retry).

### Public surface

```go
// RetryOptions configures the Retryer.
type RetryOptions struct {
    // MaxAttempts is the maximum total attempts (1 = no retry).
    // Zero → 3 default.
    MaxAttempts int

    // Backoff returns the wait duration before attempt i (0-indexed).
    // attempt=0 must return 0. nil → defaultBackoff (truncated
    // exponential 100ms, 200ms, 400ms… clamped at 5s, ±25% jitter).
    Backoff func(attempt int) time.Duration

    // IsRetryable supplements the built-in classification. Called for
    // any error/response not auto-retried by the built-ins. Returning
    // true causes the next attempt; false stops the loop and returns
    // the current err/resp. nil → only built-ins retry.
    //
    // For successful (err == nil) responses, builtin returns false
    // unconditionally — pass IsRetryable to retry on 5xx etc.
    IsRetryable func(err error, resp *Response) bool

    // Rand seeds the jitter source for the default backoff. nil →
    // time-seeded *rand.Rand owned by the Retryer.
    Rand *rand.Rand
}

// Retryer wraps a *Client with an automatic retry loop for idempotent
// requests.
type Retryer struct {
    c    *Client
    opts RetryOptions
    rng  *rand.Rand // jitter source; protected by rngMu
    rngMu sync.Mutex
}

// NewRetryer constructs a Retryer. Zero values in opts are filled
// with defaults. The returned *Retryer is goroutine-safe.
func NewRetryer(c *Client, opts RetryOptions) *Retryer

// Do issues req with retries. Behaves like Client.Do for
// non-idempotent requests or when retry is otherwise disabled.
func (r *Retryer) Do(ctx context.Context, req *Request) (*Response, error)

// DoStream issues a streaming request with retries that apply ONLY
// before the first HEADERS frame is delivered. Once StreamResponse is
// returned successfully, no further retry occurs — caller owns the
// response stream.
func (r *Retryer) DoStream(ctx context.Context, req *Request) (*StreamResponse, error)
```

### Request changes

```go
type Request struct {
    // ... existing fields unchanged ...

    // Idempotent overrides the automatic idempotency classification
    // based on Method. nil → classify by method (GET, HEAD, OPTIONS,
    // PUT, DELETE, TRACE are idempotent; POST, PATCH are not).
    Idempotent *bool
}
```

## Decision tables

### Idempotency

```
isIdempotent(req):
  if req.Idempotent != nil: return *req.Idempotent
  switch req.Method:
    "GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE": return true
  return false
```

### Retry classification

| Condition | Built-in retry | Reason |
|---|---|---|
| `*StreamResetError{Code: ErrCodeRefusedStream}` | Yes | RFC 7540 §8.1.4 — explicitly safe |
| `errors.Is(err, conn.ErrGoAway)` | Yes | Stream not processed; another conn may serve |
| `*DialError`, `ErrDialBackoff` | Yes | Network transient |
| `errors.Is(err, context.Canceled)` | Never | Caller intent |
| `errors.Is(err, context.DeadlineExceeded)` | Never | Caller intent |
| `errors.Is(err, ErrPoolClosed)` | Never | Terminal |
| `errors.Is(err, ErrClosed)` | Never | Terminal |
| `errors.Is(err, ErrInvalidRequest)` | Never | Won't change on retry |
| `*StreamResetError` other code | Via `IsRetryable` | Caller-defined |
| `resp.Status >= 500` | Via `IsRetryable` | Caller-defined |
| Anything else | Via `IsRetryable` | Caller-defined |

### Retry gates (no retry if any fail)

| Gate | Reason |
|---|---|
| `req.Idempotent` resolves to false | Non-idempotent — replay unsafe |
| `req.BodyReader != nil` | Body cannot be re-read |
| `r.opts.MaxAttempts <= 1` | Disabled by config |
| `req` is `nil` or `ErrInvalidRequest` from validate | Bad input |

## Loop semantics

Helpers used in the pseudocode below:

```
userIsRetryable(err, resp):
  if r.opts.IsRetryable == nil: return false
  return r.opts.IsRetryable(err, resp)

isHardStop(err):
  return errors.Is(err, context.Canceled) ||
         errors.Is(err, context.DeadlineExceeded) ||
         errors.Is(err, ErrPoolClosed) ||
         errors.Is(err, ErrClosed) ||
         errors.Is(err, ErrInvalidRequest)
```

### `Do`

```
if !canRetry(req): return r.c.Do(ctx, req)

var resp *Response
var err error
for attempt = 0; attempt < MaxAttempts; attempt++:
    if attempt > 0:
        select:
          case <-time.After(r.backoff(attempt)):
          case <-ctx.Done():
            return nil, ctx.Err()
    resp, err = r.c.Do(ctx, req)
    if err == nil:
        if !r.userIsRetryable(nil, resp):
            return resp, nil
        continue
    if isHardStop(err):  // ctx, ErrClosed, ErrPoolClosed, ErrInvalidRequest
        return nil, err
    if builtinShouldRetry(err):
        continue
    if r.userIsRetryable(err, nil):
        continue
    return nil, err
return resp, err  // budget exhausted; last result wins
```

### `DoStream`

Identical loop, with two adjustments:

1. The success branch returns `*StreamResponse` immediately — no
   `IsRetryable` call. Caller owns the stream from this point;
   subsequent 5xx response classification is the caller's concern.
2. Errors before the first HEADERS frame retry; errors mid-stream are
   not the Retryer's concern (DoStream returns them via the stream
   event channel).

## Default backoff

```
defaultBackoff(attempt int) time.Duration:
  if attempt <= 0: return 0
  base := 100ms * 2^(attempt-1)
  if base > 5s: base = 5s
  jitter := uniformRand(-base/4, +base/4)
  return base + jitter
```

Sequence (no jitter): 0, 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s,
5s, 5s, …

Jitter source is `*rand.Rand` (math/rand) — secrecy not required;
the goal is desynchronizing thundering herds. Protected by
`rngMu sync.Mutex` for goroutine safety.

## Concurrency

`Retryer` is goroutine-safe. The wrapped `*Client` is already
goroutine-safe. The only Retryer-owned mutable state is the jitter
RNG, guarded by `rngMu`.

## Test plan

`client/retry_test.go`:

| # | Test | What it pins |
|---|---|---|
| 1 | `TestRetryer_Do_RefusedStream_Retries` | Server first sends RST(REFUSED_STREAM), then 200 → final 200 returned, attempt count = 2 |
| 2 | `TestRetryer_Do_NonIdempotent_NoRetry` | POST + RST → 1 attempt, error returned |
| 3 | `TestRetryer_Do_IdempotentOverride_TrueOnPost` | POST with `Idempotent: ptr(true)` retries |
| 4 | `TestRetryer_Do_IdempotentOverride_FalseOnGet` | GET with `Idempotent: ptr(false)` does not retry |
| 5 | `TestRetryer_Do_BodyReader_NoRetry` | BodyReader set + RST → 1 attempt |
| 6 | `TestRetryer_Do_Custom5xx_Retries` | `IsRetryable` returns true for `resp.Status == 503` → retried |
| 7 | `TestRetryer_Do_CtxCanceled_StopsImmediately` | ctx cancel mid-backoff → returns ctx.Err quickly |
| 8 | `TestRetryer_Do_MaxAttempts_Exhausted` | All attempts RST → last err returned |
| 9 | `TestRetryer_Do_BackoffCutByCtx` | Long backoff truncated by ctx deadline |
| 10 | `TestRetryer_Do_HardStop_PoolClosed` | Closed pool → no retry |
| 11 | `TestRetryer_DoStream_RetriesBeforeHeaders` | Server RSTs first call, 200 second → DoStream returns headers |
| 12 | `TestRetryer_DoStream_AfterHeaders_NoRetry` | Successful headers + mid-stream reset → not retried |
| 13 | `TestBuiltinShouldRetry_Classification` | Table covering each built-in classification |
| 14 | `TestIsIdempotent_AllMethods` | Table covering Method ∈ {GET,HEAD,OPTIONS,PUT,DELETE,TRACE,POST,PATCH,empty} + Idempotent override |
| 15 | `TestDefaultBackoff_BoundsAndJitter` | attempt=0 → 0; growth pattern; clamp at 5s; jitter within ±25% |
| 16 | `TestRetryer_Goroutine_SafeJitter` | Concurrent Do calls — race detector clean |

Coverage target: ≥80% for new `retry.go` (per project policy).

## RFC trace policy

Add row to `docs/RFC_COVERAGE.md`:

| Section | Type | Test |
|---|---|---|
| §8.1.4 | Conformance | `TestRetryer_Do_RefusedStream_Retries` (client/) — REFUSED_STREAM safe to retry |

## Open questions

None.

## Risks

- **Test infrastructure:** `httptest` + `http2.Server` does not expose
  a primitive for "send RST_STREAM with code REFUSED_STREAM on first
  request, then accept". May need a small custom HTTP/2 server using
  `golang.org/x/net/http2` framer directly, or use the conn package's
  test helpers. To investigate during plan-writing.
- **DoStream timing:** classifying "before first HEADERS" cleanly
  requires no error reaching the caller until DoStream returns. The
  existing DoStream is already structured this way (returns once
  initial HEADERS arrive), so this should map directly.
