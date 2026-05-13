# C.4 — Metrics & Observability — Design Spec

**Status:** approved (2026-05-08)
**Phase:** C.4
**Depends on:** C.1 (Client), C.2 (Pool), C.3 (managedPool)

## Goals

1. Surface request-level lifecycle events (start, complete, retry) so users
   can wire request logging, tracing, and per-request metrics.
2. Surface pool/conn lifecycle events (dial, close, resolver update) so
   users can observe transport health and capacity changes.
3. Provide a built-in pull-based metrics view (counters + latency
   histograms) so basic observability needs are met without external deps.
4. Ship first-party OpenTelemetry and Prometheus adapter modules that
   wire the above into the user's existing collector — without forcing
   those deps onto users who do not want them.

## Non-goals

- No event-bus / channel-based event stream (lossy under backpressure,
  harder to consume correctly).
- No fine-grained per-frame events (HEADERS, DATA, etc.); coarse
  request/conn lifecycle only.
- No tracing context propagation in core (adapters can do it).
- No structured logging primitives.

## Constraints

- Hot path must remain zero-alloc when no hooks are set. The codec
  bench gate (0 B/op, 0 allocs/op) is sacred.
- Per-event nil-check must be branch-predictor friendly (the common
  case is hook == nil).
- Adapters must not pull external deps into the core go.mod.
- Hooks must not block (documented; not enforced).
- Hook panics propagate (no silent recover).

## API

### `client.Hooks`

```go
type Hooks struct {
    OnRequestStart    func(RequestStartEvent)
    OnRequestComplete func(RequestCompleteEvent)
    OnRetry           func(RetryEvent)

    OnDial      func(DialEvent)
    OnConnClose func(ConnCloseEvent)

    OnResolverUpdate func(ResolverUpdateEvent)
}
```

All fields optional; nil fields are skipped at every call site.

### Event types

```go
type RequestStartEvent struct {
    Method, Path, Authority string
    Attempt                 int
}

type RequestCompleteEvent struct {
    Method, Path, Authority string
    Status                  int
    Err                     error
    Latency                 time.Duration
    BytesSent, BytesRecv    int64
    Attempt                 int
}

type RetryEvent struct {
    Method, Path string
    Attempt      int
    Err          error
    Backoff      time.Duration
}

type DialEvent struct {
    Addr     string
    Err      error
    Duration time.Duration
}

type ConnCloseEvent struct {
    Addr   string
    Reason CloseReason
}

type ResolverUpdateEvent struct {
    Added, Removed []Address
    Total          int
}

type CloseReason int
const (
    CloseIdle CloseReason = iota
    CloseDead
    CloseGoAway
    CloseManual
)
```

All events are passed by value. Sizes target ≤128 bytes so escape
analysis can keep them on the caller's stack.

### Wiring

```go
type ClientOptions struct {
    // ... existing fields ...
    Hooks *Hooks  // optional
}

// SetHooks atomically replaces the active hook set on a running Client.
// Used by adapter packages that wire after NewClient.
func (c *Client) SetHooks(h *Hooks)
```

The active `*Hooks` is stored in an `atomic.Pointer[Hooks]` on
`Client`, `Pool`, and `managedPool`. Read at every call site via
`c.hooks.Load()`.

### Counters & Histograms

```go
type Counters struct {
    RequestsStarted   atomic.Int64
    RequestsSucceeded atomic.Int64
    RequestsErrored   atomic.Int64
    Retries           atomic.Int64
    DialsAttempted    atomic.Int64
    DialsFailed       atomic.Int64
    ConnsClosed       atomic.Int64
    GoAwaysReceived   atomic.Int64
}

// CountersSnapshot is a frozen, value-copyable view of Counters.
// Returned by Counters.Snapshot() and embedded in MetricsSnapshot.
// (Counters itself contains atomic.Int64 which is not safe to copy.)
type CountersSnapshot struct {
    RequestsStarted, RequestsSucceeded, RequestsErrored int64
    Retries                                              int64
    DialsAttempted, DialsFailed                          int64
    ConnsClosed                                          int64
    GoAwaysReceived                                      int64
}
func (c *Counters) Snapshot() CountersSnapshot

type Histogram struct {
    buckets [64]atomic.Int64
    sum     atomic.Int64
    count   atomic.Int64
}
func (h *Histogram) Observe(d time.Duration)
func (h *Histogram) Snapshot() HistogramSnapshot

type HistogramSnapshot struct {
    Buckets [64]int64
    Sum     int64
    Count   int64
}
func (s HistogramSnapshot) Mean() time.Duration
func (s HistogramSnapshot) Quantile(q float64) time.Duration

// Metrics is the live, lock-free aggregate. Accessible via Client.Metrics()
// for callers that want to read counters directly via atomic .Load(); for a
// frozen value-copy use Client.MetricsSnapshot().
type Metrics struct {
    Counters Counters
    Latency  struct {
        Request Histogram
        Dial    Histogram
        Acquire Histogram
    }
}

type MetricsSnapshot struct {
    Counters CountersSnapshot
    Latency  struct {
        Request, Dial, Acquire HistogramSnapshot
    }
}

func (c *Client) Metrics() *Metrics             // live struct (DO NOT copy)
func (c *Client) MetricsSnapshot() MetricsSnapshot // value-safe copy
```

Histogram bucket `i` holds observations with `floor(log2(ns)) == i`.
64 buckets cover 1ns..2^63ns. `Quantile(q)` returns the upper edge of
the bucket containing the q-th observation (bucket-edge approximation
— exact percentiles are the adapter's job).

## Hot-path integration

| Event | Site | File |
|-------|------|------|
| OnRequestStart    | top of `Client.Do` / `Client.DoStream` | `client.go` |
| OnRequestComplete | end of `Do`; deferred in `DoStream`     | `client.go` |
| OnRetry           | retry loop in `Retryer.Do`              | `retry.go`  |
| OnDial            | `pool.dialOne` after dial; `singleConn.dial` | `pool.go`, `single_conn.go` |
| OnConnClose       | `pool.evict` (passes `Reason`)          | `pool.go`   |
| OnResolverUpdate  | `managedPool.applySet` post-diff        | `managed_pool.go` |

Counters & histograms always update at these sites (no nil-check).
Hook fields nil-checked.

Counter increment for the no-hook path: one `atomic.AddInt64` per
event.  Histogram `Observe`: one `bits.Len64` + 3 `atomic.AddInt64`.
Both lock-free.

Standard call-site pattern:

```go
ev := RequestCompleteEvent{...}
m := &c.metrics
m.Counters.RequestsStarted.Add(1)
if err == nil {
    m.Counters.RequestsSucceeded.Add(1)
} else {
    m.Counters.RequestsErrored.Add(1)
}
m.Latency.Request.Observe(latency)
if h := c.hooks.Load(); h != nil && h.OnRequestComplete != nil {
    h.OnRequestComplete(ev)
}
```

## Repository layout

Core (no new deps):
```
client/
  hooks.go         // Hooks struct, event types, CloseReason
  metrics.go       // Counters, Histogram, MetricsSnapshot
  hooks_test.go
  metrics_test.go
```

Adapters as separate Go modules:
```
adapters/
  otel/
    go.mod         // requires go.opentelemetry.io/otel/metric
    otel.go
    otel_test.go
  prom/
    go.mod         // requires github.com/prometheus/client_golang
    prom.go
    prom_test.go
```

Adapter modules use `replace ../.. => github.com/lodgvideon/...` during
dev; on release the replace is dropped and adapters version-bump
independently of core.

### Adapter signatures

```go
// adapters/otel
type Options struct {
    Namespace string // metric name prefix; default "poseidon"
}
func Wire(c *client.Client, mp metric.MeterProvider, opts Options) (shutdown func(context.Context) error, err error)

// adapters/prom
type Options struct {
    Namespace    string        // metric name prefix; default "poseidon"
    ScrapePeriod time.Duration // histogram bucket pull period; 0 → 1s default
}
func Wire(c *client.Client, reg prometheus.Registerer, opts Options) (stop func(), err error)
```

Both call `client.SetHooks` for event-driven counter/histogram
recordings. The Prom adapter additionally starts a scrape goroutine
that pulls `client.MetricsSnapshot()` at `ScrapePeriod` and updates
the registered prom Histogram via observed bucket deltas. OTel adapter
is fully event-driven (meter records on each hook fire); no scrape
goroutine.

## Error / panic policy

- Hooks run inline on the caller's goroutine for request events and
  on the pool actor for pool events.
- Hooks must not block; documented contract, not enforced.
- Hook panics propagate. No `recover` in core. Users wrap their own
  hooks if they want isolation.
- This keeps the no-hook path branch-clean and avoids a silent error
  channel inside the library.

## Testing

### `client/`

- `hooks_test.go`: each hook field exercised via in-test recorder.
  Assert event payload (method, path, status, latency > 0). Nil-hooks
  path: no panic, no recorded calls. `SetHooks` after `NewClient`
  applies before next `Do`.
- `metrics_test.go`:
  - Counter atomicity: N goroutines × M increments → exact total.
  - Histogram boundary cases: `Observe(1ns)` → bucket 0;
    `Observe(1023ns)` → bucket 9; `Observe(1024ns)` → bucket 10.
  - `Quantile(0.5)`, `Quantile(0.99)` against known distribution.
  - `MetricsSnapshot` is a coherent value-copy.
- Integration: extend `integration_test.go` with one new test —
  attach Hooks, do 10 requests against httptest h2 server, assert
  all counter values + hook call counts match expected.
- Benchmarks: `BenchmarkDo_NoHooks` vs `BenchmarkDo_WithHooks`.
  Bench-gate enforces zero new allocations on `NoHooks`.

### Adapters

- `adapters/otel/otel_test.go`: SDK in-memory metric reader, do 5
  requests, gather, assert counter readings == 5.
- `adapters/prom/prom_test.go`: in-memory registry, do 5 requests,
  trigger one scrape via `Wire`'s scrape goroutine OR call its
  internal pull directly, gather metric families, assert values.
- New CI workflow `adapters.yml` matrix runs `go test ./...` per
  adapter module.

### Bench baseline

Add C.4 row to `docs/BENCH_BASELINE.md`:
- `BenchmarkDo_NoHooks` — same B/op + allocs/op as pre-C.4 baseline
- `BenchmarkDo_WithHooks` — record current numbers, gate on
  ≤2 allocs/op (event struct may escape in some hook configurations).

## RFC trace

C.4 is observability — non-conformance. No new
`TestConformance_RFC7540_*` rows. `docs/RFC_COVERAGE.md` not amended
unless a specific test pins RFC behavior.

## Migration & compatibility

- All new API is additive. `ClientOptions.Hooks` is optional with
  zero-value default = no hooks.
- `Counters` / `Histogram` / `MetricsSnapshot` are new types.
- `*Client.Metrics()` and `*Client.MetricsSnapshot()` are new methods.
- No existing test breaks; no existing user code requires changes.

## Open issues

None at design time. Implementation may surface alloc edges in the
WithHooks path that require event-pool reuse — handled in writing-plans.
