# C.4 Metrics & Observability — Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add zero-overhead-when-disabled hooks, lock-free counters, and lock-free latency histograms to `client/`, wired at six call sites, with full test coverage and benchmarks.

**Architecture:** `client/hooks.go` defines the `Hooks` callback struct and event types. `client/metrics.go` defines `Counters`, `Histogram`, and `MetricsSnapshot`. `Client` owns `*atomic.Pointer[Hooks]` (so `SetHooks` is thread-safe) and `*Metrics`; `Pool` and `managedPool` get pointers to the same instances so events surface from any layer. Hot path: `if h := c.hooks.Load(); h != nil && h.OnX != nil { h.OnX(...) }` plus unconditional atomic counter increments.

**Tech Stack:** Go 1.24, `sync/atomic`, `math/bits` (no new deps).

**Out of scope:** OTel + Prom adapter modules — covered by follow-up plan `2026-05-08-c4-metrics-adapters.md`.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `client/hooks.go` | NEW | `Hooks` struct, all event types, `CloseReason` enum |
| `client/metrics.go` | NEW | `Counters`, `CountersSnapshot`, `Histogram`, `HistogramSnapshot`, `Metrics`, `MetricsSnapshot` |
| `client/client.go` | MODIFY | `ClientOptions.Hooks`, `Client` fields, `NewClient` wiring, `SetHooks`, `Metrics`, `MetricsSnapshot`, hot-path integration in `Do`/`DoStream` |
| `client/pool.go` | MODIFY | `newPool` signature accepts hooks/metrics; OnDial/OnConnClose call sites; counter increments; refactor `evict*` to take `CloseReason` |
| `client/pool_transport.go` | MODIFY | `newPoolTransport` signature accepts hooks/metrics |
| `client/managed_pool.go` | MODIFY | `newManagedPool`/`buildManagedPool` accept hooks/metrics; `applySet` fires `OnResolverUpdate`; threads through to sub-pools |
| `client/single_conn.go` | MODIFY | `singleConn` struct gets hooks/metrics; OnDial site after `conn.Dial` |
| `client/retry.go` | MODIFY | `OnRetry` site in `doLoop` and `DoStream`; retry counter |
| `client/hooks_test.go` | NEW | Per-hook fire test, nil-safe test, SetHooks test |
| `client/metrics_test.go` | NEW | Counter atomicity, histogram bucket math, snapshot coherence |
| `client/integration_test.go` | MODIFY | New integration test exercising all hooks against httptest h2 server |
| `client/bench_metrics_test.go` | NEW | `BenchmarkDo_NoHooks` vs `BenchmarkDo_WithHooks` |
| `docs/BENCH_BASELINE.md` | MODIFY | C.4 row with benchmark numbers |
| `README.md` | MODIFY | Phase line for C.4 |

---

## Task 1: Add `client/hooks.go`

**Files:** Create `client/hooks.go`

- [ ] **Step 1: Create the file**

```go
// client/hooks.go
package client

import "time"

// Hooks is an optional set of callbacks invoked on lifecycle events.
// All fields are optional; nil hooks are skipped at zero cost.
//
// Hooks must not block — request hooks fire on the caller's goroutine
// and pool hooks fire on the pool actor goroutine. A blocking hook
// will stall request processing or the pool actor.
//
// Hook panics propagate. Wrap callbacks with recover() if isolation
// is needed.
type Hooks struct {
	// OnRequestStart fires at the top of Client.Do / Client.DoStream
	// before transport acquire.
	OnRequestStart func(RequestStartEvent)

	// OnRequestComplete fires when Do returns (sync) or when DoStream
	// returns its initial StreamResponse (or an error).
	OnRequestComplete func(RequestCompleteEvent)

	// OnRetry fires inside the Retryer between attempts after the
	// backoff sleep returns nil.
	OnRetry func(RetryEvent)

	// OnDial fires after a transport dial completes (success or error).
	OnDial func(DialEvent)

	// OnConnClose fires when a conn is evicted from a pool.
	OnConnClose func(ConnCloseEvent)

	// OnResolverUpdate fires when managedPool applies a new address
	// set from the Resolver. Not fired for TransportSingleConn or
	// TransportPool.
	OnResolverUpdate func(ResolverUpdateEvent)
}

// RequestStartEvent carries metadata for OnRequestStart.
type RequestStartEvent struct {
	Method, Path, Authority string
	Attempt                 int // 0 for first try, ≥1 for retries
}

// RequestCompleteEvent carries metadata for OnRequestComplete.
type RequestCompleteEvent struct {
	Method, Path, Authority string
	Status                  int // 0 if no headers received
	Err                     error
	Latency                 time.Duration
	BytesSent, BytesRecv    int64
	Attempt                 int
}

// RetryEvent carries metadata for OnRetry.
type RetryEvent struct {
	Method, Path string
	Attempt      int
	Err          error
	Backoff      time.Duration
}

// DialEvent carries metadata for OnDial.
type DialEvent struct {
	Addr     string
	Err      error
	Duration time.Duration
}

// ConnCloseEvent carries metadata for OnConnClose.
type ConnCloseEvent struct {
	Addr   string
	Reason CloseReason
}

// ResolverUpdateEvent carries metadata for OnResolverUpdate.
type ResolverUpdateEvent struct {
	Added, Removed []Address
	Total          int
}

// CloseReason identifies why a conn was closed/evicted.
type CloseReason int

// CloseReason values.
const (
	// CloseIdle is set when the conn was idle past PoolOptions.IdleTimeout.
	CloseIdle CloseReason = iota
	// CloseDead is set when conn.IsAlive returned false at eviction time.
	CloseDead
	// CloseGoAway is set when the conn was evicted because the peer sent GOAWAY.
	CloseGoAway
	// CloseManual is set when the conn was closed via Pool.Close / Client.Close.
	CloseManual
)

// String returns a stable lowercase label for the reason. Handy for
// metric labels and log fields.
func (r CloseReason) String() string {
	switch r {
	case CloseIdle:
		return "idle"
	case CloseDead:
		return "dead"
	case CloseGoAway:
		return "goaway"
	case CloseManual:
		return "manual"
	}
	return "unknown"
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./client/`
Expected: success (types unused but defined).

- [ ] **Step 3: Commit**

```bash
git add client/hooks.go
git commit -m "feat(client): C.4 — Hooks struct + event types"
```

---

## Task 2: Add `client/metrics.go`

**Files:** Create `client/metrics.go`, `client/metrics_test.go`

- [ ] **Step 1: Write failing tests first (`client/metrics_test.go`)**

```go
// client/metrics_test.go
package client

import (
	"sync"
	"testing"
	"time"
)

func TestCounters_AtomicityUnderLoad(t *testing.T) {
	t.Parallel()
	var c Counters
	const goroutines, perG = 64, 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				c.RequestsStarted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := c.RequestsStarted.Load(); got != goroutines*perG {
		t.Errorf("RequestsStarted = %d, want %d", got, goroutines*perG)
	}
}

func TestCounters_Snapshot(t *testing.T) {
	t.Parallel()
	var c Counters
	c.RequestsStarted.Store(7)
	c.DialsAttempted.Store(2)
	c.GoAwaysReceived.Store(1)
	s := c.Snapshot()
	if s.RequestsStarted != 7 || s.DialsAttempted != 2 || s.GoAwaysReceived != 1 {
		t.Errorf("snapshot mismatch: %+v", s)
	}
}

func TestHistogram_BucketBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ns      int64
		wantIdx int
	}{
		{1, 0},      // [1,2)
		{2, 1},      // [2,4)
		{3, 1},      // [2,4)
		{1023, 9},   // [512,1024)
		{1024, 10},  // [1024,2048)
		{1 << 30, 30},
	}
	for _, tc := range cases {
		var h Histogram
		h.Observe(time.Duration(tc.ns))
		if got := h.buckets[tc.wantIdx].Load(); got != 1 {
			t.Errorf("Observe(%d ns): bucket[%d] = %d, want 1", tc.ns, tc.wantIdx, got)
		}
		// Adjacent buckets must be 0.
		for i := 0; i < 64; i++ {
			if i == tc.wantIdx {
				continue
			}
			if got := h.buckets[i].Load(); got != 0 {
				t.Errorf("Observe(%d ns): bucket[%d] = %d, want 0 (only [%d] should be set)",
					tc.ns, i, got, tc.wantIdx)
			}
		}
	}
}

func TestHistogram_ObserveBelowOne(t *testing.T) {
	t.Parallel()
	// 0 and negative durations clamp to bucket 0.
	var h Histogram
	h.Observe(0)
	h.Observe(-5)
	if got := h.buckets[0].Load(); got != 2 {
		t.Errorf("bucket[0] = %d, want 2", got)
	}
}

func TestHistogram_Snapshot(t *testing.T) {
	t.Parallel()
	var h Histogram
	for i := 0; i < 100; i++ {
		h.Observe(100 * time.Microsecond)
	}
	s := h.Snapshot()
	if s.Count != 100 {
		t.Errorf("Count = %d, want 100", s.Count)
	}
	if s.Sum != int64(100*100*time.Microsecond) {
		t.Errorf("Sum = %d, want %d", s.Sum, int64(100*100*time.Microsecond))
	}
	if mean := s.Mean(); mean != 100*time.Microsecond {
		t.Errorf("Mean = %v, want %v", mean, 100*time.Microsecond)
	}
}

func TestHistogram_Quantile(t *testing.T) {
	t.Parallel()
	// 90 observations in bucket 9 (≤1023ns); 10 in bucket 19 (≤1Ms).
	var h Histogram
	for i := 0; i < 90; i++ {
		h.Observe(500 * time.Nanosecond) // bucket 8
	}
	for i := 0; i < 10; i++ {
		h.Observe(time.Millisecond) // bucket 19 (1ms = 10^6 ns; log2(10^6) ≈ 19.93 → bucket 19)
	}
	s := h.Snapshot()
	q50 := s.Quantile(0.5)
	if q50 < 256*time.Nanosecond || q50 > 1024*time.Nanosecond {
		t.Errorf("Quantile(0.5) = %v, want bucket 8 upper edge (≤1024ns)", q50)
	}
	q99 := s.Quantile(0.99)
	if q99 < 524288*time.Nanosecond {
		t.Errorf("Quantile(0.99) = %v, want bucket 19 (≥524288ns)", q99)
	}
}

func TestHistogram_QuantileEmpty(t *testing.T) {
	t.Parallel()
	var h Histogram
	s := h.Snapshot()
	if got := s.Quantile(0.5); got != 0 {
		t.Errorf("Quantile on empty = %v, want 0", got)
	}
	if got := s.Mean(); got != 0 {
		t.Errorf("Mean on empty = %v, want 0", got)
	}
}
```

- [ ] **Step 2: Run tests — confirm they fail to compile**

Run: `go test ./client/ -run='TestCounters|TestHistogram' -count=1`
Expected: FAIL — "undefined: Counters" / "undefined: Histogram".

- [ ] **Step 3: Create `client/metrics.go`**

```go
// client/metrics.go
package client

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// Counters is the lock-free integer-counter struct embedded in Metrics.
// All fields are updated via atomic.Add and read via Snapshot for a
// value-safe copy. Direct field access is goroutine-safe but the
// resulting tuple is not torn-coherent across reads — use Snapshot
// when you need a consistent view.
type Counters struct {
	RequestsStarted   atomic.Int64
	RequestsSucceeded atomic.Int64 // status code received (any)
	RequestsErrored   atomic.Int64 // Do returned non-nil err
	Retries           atomic.Int64
	DialsAttempted    atomic.Int64
	DialsFailed       atomic.Int64
	ConnsClosed       atomic.Int64 // sum across all CloseReason values
	GoAwaysReceived   atomic.Int64
}

// CountersSnapshot is a frozen, value-copyable view of Counters.
// Returned by Counters.Snapshot.
type CountersSnapshot struct {
	RequestsStarted   int64
	RequestsSucceeded int64
	RequestsErrored   int64
	Retries           int64
	DialsAttempted    int64
	DialsFailed       int64
	ConnsClosed       int64
	GoAwaysReceived   int64
}

// Snapshot returns a value-copyable view. Each field is read with one
// atomic.Load; field-to-field consistency is best-effort (counters may
// have been updated between Loads).
func (c *Counters) Snapshot() CountersSnapshot {
	return CountersSnapshot{
		RequestsStarted:   c.RequestsStarted.Load(),
		RequestsSucceeded: c.RequestsSucceeded.Load(),
		RequestsErrored:   c.RequestsErrored.Load(),
		Retries:           c.Retries.Load(),
		DialsAttempted:    c.DialsAttempted.Load(),
		DialsFailed:       c.DialsFailed.Load(),
		ConnsClosed:       c.ConnsClosed.Load(),
		GoAwaysReceived:   c.GoAwaysReceived.Load(),
	}
}

// Histogram is a lock-free log2-bucket latency histogram.
//
// Bucket i holds observations with floor(log2(ns)) == i. 64 buckets
// span [1ns, 2^63 ns). One Observe is one bits.Len64 + 3 atomic.Add;
// no allocation. 0 / negative durations clamp to bucket 0.
type Histogram struct {
	buckets [64]atomic.Int64
	sum     atomic.Int64 // ns
	count   atomic.Int64
}

// Observe records a single duration.
func (h *Histogram) Observe(d time.Duration) {
	n := int64(d)
	if n < 1 {
		n = 1
	}
	idx := bits.Len64(uint64(n)) - 1
	h.buckets[idx].Add(1)
	h.sum.Add(n)
	h.count.Add(1)
}

// HistogramSnapshot is a frozen view of Histogram state.
type HistogramSnapshot struct {
	Buckets [64]int64
	Sum     int64 // ns
	Count   int64
}

// Snapshot copies the current bucket counts, sum, and count.
func (h *Histogram) Snapshot() HistogramSnapshot {
	var s HistogramSnapshot
	for i := range h.buckets {
		s.Buckets[i] = h.buckets[i].Load()
	}
	s.Sum = h.sum.Load()
	s.Count = h.count.Load()
	return s
}

// Mean returns the arithmetic mean of all observations, or 0 on empty.
func (s HistogramSnapshot) Mean() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return time.Duration(s.Sum / s.Count)
}

// Quantile returns the upper edge of the bucket containing the q-th
// observation (0 ≤ q ≤ 1). Bucket-edge approximation: precise to a
// factor of 2. Returns 0 if no observations recorded.
func (s HistogramSnapshot) Quantile(q float64) time.Duration {
	if s.Count == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	target := int64(float64(s.Count) * q)
	if target == 0 {
		target = 1
	}
	var cum int64
	for i, n := range s.Buckets {
		cum += n
		if cum >= target {
			// Upper edge of bucket i is 2^(i+1) - 1 ns.
			return time.Duration((int64(1) << (i + 1)) - 1)
		}
	}
	// Should be unreachable when count > 0; guard anyway.
	return time.Duration((int64(1) << 63) - 1)
}

// Metrics aggregates Counters and per-event-class latency histograms
// for a single Client. Pass-through pointer; do not value-copy.
type Metrics struct {
	Counters Counters
	Latency  struct {
		Request Histogram
		Dial    Histogram
		Acquire Histogram
	}
}

// MetricsSnapshot is a frozen, value-copyable view of Metrics.
type MetricsSnapshot struct {
	Counters CountersSnapshot
	Latency  struct {
		Request HistogramSnapshot
		Dial    HistogramSnapshot
		Acquire HistogramSnapshot
	}
}

// Snapshot copies counters and histograms into a value-safe struct.
func (m *Metrics) Snapshot() MetricsSnapshot {
	var s MetricsSnapshot
	s.Counters = m.Counters.Snapshot()
	s.Latency.Request = m.Latency.Request.Snapshot()
	s.Latency.Dial = m.Latency.Dial.Snapshot()
	s.Latency.Acquire = m.Latency.Acquire.Snapshot()
	return s
}
```

- [ ] **Step 4: Run tests — confirm pass**

Run: `go test ./client/ -run='TestCounters|TestHistogram' -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client/metrics.go client/metrics_test.go
git commit -m "feat(client): C.4 — Counters + lock-free Histogram + Snapshot"
```

---

## Task 3: Wire `*atomic.Pointer[Hooks]` and `*Metrics` into `Client`

**Files:** Modify `client/client.go`

- [ ] **Step 1: Open `client/client.go` and locate `ClientOptions` struct**

Add a new field after `DrainMode`:
```go
	// Hooks is an optional set of lifecycle callbacks. nil → no hooks
	// fire. May be replaced at runtime via Client.SetHooks.
	Hooks *Hooks
```

- [ ] **Step 2: Locate the `Client` struct (~line 73) and add fields**

Replace:
```go
type Client struct {
	tr        transport
	authority string
}
```
With:
```go
type Client struct {
	tr        transport
	authority string
	hooks     atomic.Pointer[Hooks]
	metrics   *Metrics
}
```

Add `"sync/atomic"` to the import block at the top of the file if not already present.

- [ ] **Step 3: Modify `NewClient` to initialise `metrics` and store hooks**

Locate `NewClient` (around line 80). After validation, before the transport-construction switch, add:
```go
	metrics := &Metrics{}
```

When constructing each transport, pass `opts.Hooks` and `metrics`. Update the calls (the actual function signatures will change in Tasks 4–5):

For `TransportSingleConn`:
```go
		tr = &singleConn{
			addr:     opts.Addr,
			connOpts: opts.ConnOpts,
			backoff:  opts.DialBackoff,
			hooksRef: nil, // set after Client is built
			metrics:  metrics,
		}
```

For `TransportPool`:
```go
		tr = newPoolTransport(opts.Addr, opts.ConnOpts, *opts.Pool, nil, metrics)
```

For `TransportManaged`:
```go
		mp, err := newManagedPool(opts.Resolver, opts.Selector, opts.DrainMode, opts.ConnOpts, po, nil, metrics)
```

Replace the final return:
```go
	c := &Client{tr: tr, authority: deriveAuthority(opts.Addr), metrics: metrics}
	c.hooks.Store(opts.Hooks)
	// Backfill the hooks reference into transports that need it.
	switch t := tr.(type) {
	case *singleConn:
		t.hooksRef = &c.hooks
	case *poolTransport:
		t.p.hooksRef = &c.hooks
	case *managedTransport:
		t.mp.hooksRef = &c.hooks
	}
	return c, nil
```

- [ ] **Step 4: Add public methods at the bottom of `client.go`**

```go
// SetHooks atomically replaces the active hook set. Pass nil to
// disable hooks. Safe to call concurrently with Do/DoStream.
func (c *Client) SetHooks(h *Hooks) {
	c.hooks.Store(h)
}

// Metrics returns the live metrics struct. The returned pointer is
// stable for the lifetime of the Client; do not value-copy.
// Use MetricsSnapshot for a value-safe view.
func (c *Client) Metrics() *Metrics {
	return c.metrics
}

// MetricsSnapshot returns a frozen, value-copyable view of metrics.
func (c *Client) MetricsSnapshot() MetricsSnapshot {
	return c.metrics.Snapshot()
}
```

- [ ] **Step 5: Build will fail — wiring Pool, managedPool, singleConn happens in Task 4**

Run: `go build ./client/`
Expected: FAIL — undefined fields on transports. That's correct; resolved by Task 4.

- [ ] **Step 6: Hold off on commit until Task 4 makes build pass**

---

## Task 4: Wire hooks/metrics through `Pool` + `singleConn` + `managedPool`

**Files:** Modify `client/pool.go`, `client/pool_transport.go`, `client/single_conn.go`, `client/managed_pool.go`, `client/managed_transport.go`

- [ ] **Step 1: Modify `client/pool.go` — add fields to `Pool` struct (~line 117)**

Add after `closeOnce`:
```go
	// hooksRef points at Client.hooks; nil-safe via Load. metrics is
	// shared with Client and other pools (managed sub-pools).
	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics
```

- [ ] **Step 2: Modify `client/pool.go` — change `newPool` signature**

Find:
```go
func newPool(addr string, connOpts conn.ConnOptions, opts PoolOptions) *Pool {
```
Replace with:
```go
func newPool(addr string, connOpts conn.ConnOptions, opts PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) *Pool {
```

In the `Pool{}` literal inside the function, add:
```go
		hooksRef:   hooksRef,
		metrics:    metrics,
```

If `metrics == nil`, allocate a private one to keep code uniform:
```go
	if metrics == nil {
		metrics = &Metrics{}
	}
```

- [ ] **Step 3: Modify `client/pool_transport.go` — change `newPoolTransport` signature**

Find:
```go
func newPoolTransport(addr string, connOpts conn.ConnOptions, opts PoolOptions) *poolTransport {
```
Replace:
```go
func newPoolTransport(addr string, connOpts conn.ConnOptions, opts PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) *poolTransport {
```
And update the body to pass `hooksRef, metrics` into `newPool(...)`.

- [ ] **Step 4: Modify `client/single_conn.go` — add fields**

```go
type singleConn struct {
	addr     string
	connOpts conn.ConnOptions
	backoff  time.Duration

	mu         sync.Mutex
	cur        *conn.Conn
	dialErr    error
	lastDialAt time.Time
	closed     bool
	dialing    chan struct{}

	// hooksRef points at Client.hooks; metrics is shared with Client.
	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics
}
```

Add `"sync/atomic"` to imports.

- [ ] **Step 5: Modify `client/managed_pool.go` — add fields to `managedPool`**

Add after `closed`:
```go
	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics
```

Add `"sync/atomic"` import (already present likely).

- [ ] **Step 6: Modify `client/managed_pool.go` — change `newManagedPool` and `buildManagedPool` signatures**

```go
func newManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*managedPool, error) {
	mp, err := buildManagedPool(r, s, dm, co, po, hooksRef, metrics)
	...
}

func buildManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) (*managedPool, error) {
	if metrics == nil {
		metrics = &Metrics{}
	}
	...
	mp := &managedPool{
		// existing fields ...
		hooksRef: hooksRef,
		metrics:  metrics,
	}
	...
}
```

- [ ] **Step 7: Modify `client/managed_pool.go` — `getOrCreateSubPool` passes through to `newPool`**

In the body where `newPool(key, mp.connOpts, mp.poolOpts)` is called, change to:
```go
		p:    newPool(key, mp.connOpts, mp.poolOpts, mp.hooksRef, mp.metrics),
```

- [ ] **Step 8: Update existing internal tests that call `newManagedPool` / `buildManagedPool`**

Run: `grep -n 'newManagedPool\|buildManagedPool\|newPoolTransport\|newPool(' client/*_test.go`

For each call site, append `, nil, nil` (zero-value hooks + auto-allocated metrics).

- [ ] **Step 9: Build + test**

Run: `go build ./client/`
Expected: PASS.

Run: `go test -race -count=1 ./client/`
Expected: PASS.

- [ ] **Step 10: Commit Tasks 3+4 together**

```bash
git add client/client.go client/pool.go client/pool_transport.go \
        client/single_conn.go client/managed_pool.go \
        client/managed_pool_internal_test.go client/managed_transport_test.go
git commit -m "feat(client): C.4 — wire Hooks + Metrics through Client/Pool/managed"
```

---

## Task 5: Hot-path — `OnRequestStart` + `OnRequestComplete` in `Client.Do`

**Files:** Modify `client/client.go`, create test in `client/hooks_test.go`

- [ ] **Step 1: Create the test scaffold (`client/hooks_test.go`)**

```go
// client/hooks_test.go
package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newH2TestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, srv.Listener.Addr().String()
}

func TestHooks_OnRequestStartAndComplete(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var startN, completeN atomic.Int32
	var lastStatus atomic.Int32
	hooks := &client.Hooks{
		OnRequestStart: func(e client.RequestStartEvent) {
			startN.Add(1)
			if e.Method != "GET" || e.Path != "/x" {
				t.Errorf("RequestStartEvent = %+v", e)
			}
		},
		OnRequestComplete: func(e client.RequestCompleteEvent) {
			completeN.Add(1)
			lastStatus.Store(int32(e.Status))
			if e.Latency <= 0 {
				t.Errorf("Latency = %v, want > 0", e.Latency)
			}
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	resp, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/x"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
	if startN.Load() != 1 {
		t.Errorf("OnRequestStart fired %d times, want 1", startN.Load())
	}
	if completeN.Load() != 1 {
		t.Errorf("OnRequestComplete fired %d times, want 1", completeN.Load())
	}
	if lastStatus.Load() != 200 {
		t.Errorf("complete event status = %d, want 200", lastStatus.Load())
	}
}

func TestHooks_NilSafe(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		// Hooks intentionally nil.
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do (nil hooks): %v", err)
	}
}

func TestHooks_SetHooksAfterNewClient(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var n atomic.Int32
	c.SetHooks(&client.Hooks{
		OnRequestComplete: func(client.RequestCompleteEvent) { n.Add(1) },
	})
	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if n.Load() != 1 {
		t.Errorf("OnRequestComplete after SetHooks fired %d times, want 1", n.Load())
	}
}
```

- [ ] **Step 2: Run test — confirm fail**

Run: `go test ./client/ -run='TestHooks_OnRequestStartAndComplete' -count=1`
Expected: FAIL — hook never fires (no integration yet).

- [ ] **Step 3: Modify `Client.Do` (in `client/client.go`)**

Replace the `Do` method body:
```go
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	start := time.Now()
	authority := req.Authority
	if authority == "" {
		authority = c.authority
	}
	if h := c.hooks.Load(); h != nil && h.OnRequestStart != nil {
		h.OnRequestStart(RequestStartEvent{
			Method: req.Method, Path: req.Path, Authority: authority, Attempt: 0,
		})
	}
	c.metrics.Counters.RequestsStarted.Add(1)

	resp, err := c.do(ctx, req)

	latency := time.Since(start)
	c.metrics.Latency.Request.Observe(latency)
	var status int
	var bytesRecv int64
	if resp != nil {
		status = resp.Status
		bytesRecv = resp.BytesReceived
	}
	if err == nil {
		c.metrics.Counters.RequestsSucceeded.Add(1)
	} else {
		c.metrics.Counters.RequestsErrored.Add(1)
	}
	if h := c.hooks.Load(); h != nil && h.OnRequestComplete != nil {
		h.OnRequestComplete(RequestCompleteEvent{
			Method: req.Method, Path: req.Path, Authority: authority,
			Status: status, Err: err, Latency: latency,
			BytesSent: int64(len(req.Body)), BytesRecv: bytesRecv,
			Attempt: 0,
		})
	}
	return resp, err
}
```

Move the existing implementation into a new private method `c.do`:
```go
// do is the inner Do without hook/metric instrumentation. Allows the
// public Do to wrap with timing + hooks without affecting the
// happy-path code.
func (c *Client) do(ctx context.Context, req *Request) (*Response, error) {
	cn, release, err := c.tr.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	s, err := cn.NewStream(ctx)
	if err != nil {
		return nil, err
	}

	hdrs := buildHeaders(req, c.authority)
	endStream := len(req.Body) == 0 && req.BodyReader == nil
	if err := s.SendHeaders(ctx, hdrs, endStream); err != nil {
		_ = s.Close()
		return nil, err
	}
	if !endStream {
		if err := writeRequestBody(ctx, s, req); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	resp, err := drainResponse(ctx, s, req)
	if err != nil {
		_ = s.Close()
	}
	return resp, err
}
```

Add `"time"` to imports if not present.

- [ ] **Step 4: Run test — confirm pass**

Run: `go test ./client/ -run='TestHooks_' -count=1 -race`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add client/client.go client/hooks_test.go
git commit -m "feat(client): C.4 — OnRequestStart/Complete in Client.Do + counters"
```

---

## Task 6: Hot-path — same in `Client.DoStream`

**Files:** Modify `client/client.go`, append to `client/hooks_test.go`

- [ ] **Step 1: Append failing test**

```go
func TestHooks_DoStream_OnRequestStartAndComplete(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var startN, completeN atomic.Int32
	hooks := &client.Hooks{
		OnRequestStart:    func(client.RequestStartEvent) { startN.Add(1) },
		OnRequestComplete: func(client.RequestCompleteEvent) { completeN.Add(1) },
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	sr, err := c.DoStream(context.Background(), &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	_ = sr.Close()
	if startN.Load() != 1 {
		t.Errorf("OnRequestStart fired %d times, want 1", startN.Load())
	}
	if completeN.Load() != 1 {
		t.Errorf("OnRequestComplete fired %d times, want 1 (hook fires on initial HEADERS, before body drain)", completeN.Load())
	}
}
```

- [ ] **Step 2: Run — confirm fail (hook fires only once for Start, not Complete)**

Run: `go test ./client/ -run='TestHooks_DoStream' -count=1`
Expected: FAIL on completeN.

- [ ] **Step 3: Modify `Client.DoStream`**

Wrap similarly to `Do`:
```go
func (c *Client) DoStream(ctx context.Context, req *Request) (*StreamResponse, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	start := time.Now()
	authority := req.Authority
	if authority == "" {
		authority = c.authority
	}
	if h := c.hooks.Load(); h != nil && h.OnRequestStart != nil {
		h.OnRequestStart(RequestStartEvent{
			Method: req.Method, Path: req.Path, Authority: authority, Attempt: 0,
		})
	}
	c.metrics.Counters.RequestsStarted.Add(1)

	sr, err := c.doStream(ctx, req)

	latency := time.Since(start)
	c.metrics.Latency.Request.Observe(latency)
	var status int
	if sr != nil {
		status = sr.Status
	}
	if err == nil {
		c.metrics.Counters.RequestsSucceeded.Add(1)
	} else {
		c.metrics.Counters.RequestsErrored.Add(1)
	}
	if h := c.hooks.Load(); h != nil && h.OnRequestComplete != nil {
		h.OnRequestComplete(RequestCompleteEvent{
			Method: req.Method, Path: req.Path, Authority: authority,
			Status: status, Err: err, Latency: latency,
			BytesSent: int64(len(req.Body)),
			Attempt:   0,
		})
	}
	return sr, err
}
```

Extract the existing body to private `doStream`:
```go
func (c *Client) doStream(ctx context.Context, req *Request) (*StreamResponse, error) {
	cn, release, err := c.tr.acquire(ctx)
	// ... existing body unchanged ...
}
```

- [ ] **Step 4: Run test — confirm pass**

Run: `go test ./client/ -run='TestHooks_DoStream' -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client/client.go client/hooks_test.go
git commit -m "feat(client): C.4 — OnRequestStart/Complete in DoStream"
```

---

## Task 7: Hot-path — `OnRetry` in Retryer

**Files:** Modify `client/retry.go`, append to `client/hooks_test.go`

- [ ] **Step 1: Append failing test**

Add `"time"` to the import block at the top of `client/hooks_test.go` if not present.

```go
func TestHooks_OnRetry(t *testing.T) {
	t.Parallel()
	// Server: first request 500, subsequent 200.
	var attempts atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()

	var retryN atomic.Int32
	hooks := &client.Hooks{
		OnRetry: func(e client.RetryEvent) {
			retryN.Add(1)
			if e.Attempt < 1 {
				t.Errorf("retry attempt = %d, want >= 1", e.Attempt)
			}
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 10 * time.Millisecond },
		IsRetryable: func(_ error, resp *client.Response) bool { return resp != nil && resp.Status == 503 },
	})
	resp, err := r.Do(context.Background(), &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Retryer.Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
	if retryN.Load() != 1 {
		t.Errorf("OnRetry fired %d times, want 1", retryN.Load())
	}
}
```

- [ ] **Step 2: Run — confirm fail**

Run: `go test ./client/ -run='TestHooks_OnRetry' -count=1`
Expected: FAIL on retryN == 0.

- [ ] **Step 3: Modify `Retryer.doLoop` to fire hook + counter**

In `client/retry.go`, modify the `doLoop` (~line 191):
```go
func (r *Retryer) doLoop(ctx context.Context, req *Request) (*Response, error) {
	var (
		resp *Response
		err  error
	)
	for attempt := 0; attempt < r.opts.MaxAttempts; attempt++ {
		if attempt > 0 {
			backoff := r.opts.Backoff(attempt)
			if h := r.d.hooks.Load(); h != nil && h.OnRetry != nil {
				h.OnRetry(RetryEvent{
					Method:  req.Method,
					Path:    req.Path,
					Attempt: attempt,
					Err:     err,
					Backoff: backoff,
				})
			}
			r.d.metrics.Counters.Retries.Add(1)
			if backoff > 0 {
				t := time.NewTimer(backoff)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
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
		if !r.shouldRetryErr(err) {
			return nil, err
		}
	}
	return resp, err
}
```

Note: `Retryer.d` is `*Client`. Access to `c.hooks` and `c.metrics` requires they be exposed. Since we're in the same package, lowercase fields are accessible directly (`r.d.hooks`, `r.d.metrics`). Verify by grep.

Apply the equivalent change inside `Retryer.DoStream`'s loop (the `attempt > 0` branch).

- [ ] **Step 4: Run test — confirm pass**

Run: `go test ./client/ -run='TestHooks_OnRetry' -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client/retry.go client/hooks_test.go
git commit -m "feat(client): C.4 — OnRetry hook + retry counter"
```

---

## Task 8: Hot-path — `OnDial` in Pool + singleConn

**Files:** Modify `client/pool.go`, `client/single_conn.go`, append to `client/hooks_test.go`

- [ ] **Step 1: Append failing test**

```go
func TestHooks_OnDial(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var dialN atomic.Int32
	hooks := &client.Hooks{
		OnDial: func(e client.DialEvent) {
			dialN.Add(1)
			if e.Addr != addr {
				t.Errorf("DialEvent.Addr = %q, want %q", e.Addr, addr)
			}
			if e.Duration <= 0 {
				t.Errorf("Duration = %v, want > 0", e.Duration)
			}
			if e.Err != nil {
				t.Errorf("Err = %v, want nil", e.Err)
			}
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
		Transport: client.TransportPool,
		Pool:      &client.PoolOptions{MaxConnsPerHost: 2},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if dialN.Load() != 1 {
		t.Errorf("OnDial fired %d times, want 1", dialN.Load())
	}
}
```

- [ ] **Step 2: Run — confirm fail**

Run: `go test ./client/ -run='TestHooks_OnDial' -count=1`
Expected: FAIL on dialN == 0.

- [ ] **Step 3: Modify `pool.dialOne` (in `client/pool.go`, ~line 349)**

Wrap the dial in timing + hook:
```go
func (p *Pool) dialOne() {
	dialStart := time.Now()
	p.metrics.Counters.DialsAttempted.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.DialTimeout)
	defer cancel()
	c, err := conn.Dial(ctx, p.addr, p.connOpts)
	dur := time.Since(dialStart)
	p.metrics.Latency.Dial.Observe(dur)
	if err != nil {
		p.metrics.Counters.DialsFailed.Add(1)
	}
	if h := p.hooksRef; h != nil {
		if loaded := h.Load(); loaded != nil && loaded.OnDial != nil {
			loaded.OnDial(DialEvent{Addr: p.addr, Err: err, Duration: dur})
		}
	}
	if err != nil {
		// existing error wrap logic
		mc := &managedConn{c: nil, dialedAt: time.Now()}
		_ = mc
		select {
		case p.dialDoneCh <- dialResult{err: &DialError{Addr: p.addr, Err: err}}:
		case <-p.closedCh:
		}
		return
	}
	mc := &managedConn{c: c, dialedAt: time.Now()}
	select {
	case p.dialDoneCh <- dialResult{mc: mc}:
	case <-p.closedCh:
		_ = c.Close()
	}
}
```

Verify the existing pre-edit body matches this skeleton — adapt without breaking working logic. Read `client/pool.go:349-380` first to confirm.

- [ ] **Step 4: Modify `singleConn.acquire` (in `client/single_conn.go`)**

Around the dial (line 71):
```go
		s.dialing = make(chan struct{})
		ch := s.dialing
		s.mu.Unlock()

		dialStart := time.Now()
		s.metrics.Counters.DialsAttempted.Add(1)
		dialed, dialErr := conn.Dial(ctx, s.addr, s.connOpts)
		dur := time.Since(dialStart)
		s.metrics.Latency.Dial.Observe(dur)
		if dialErr != nil {
			s.metrics.Counters.DialsFailed.Add(1)
		}
		if hr := s.hooksRef; hr != nil {
			if h := hr.Load(); h != nil && h.OnDial != nil {
				h.OnDial(DialEvent{Addr: s.addr, Err: dialErr, Duration: dur})
			}
		}

		s.mu.Lock()
		// ... existing post-dial logic unchanged ...
```

Add `"time"` to imports if not present.

- [ ] **Step 5: Run test + race suite**

Run: `go test ./client/ -run='TestHooks_OnDial' -count=1 -race`
Expected: PASS.

Run: `go test -race -count=1 ./client/`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add client/pool.go client/single_conn.go client/hooks_test.go
git commit -m "feat(client): C.4 — OnDial + dial counters/latency"
```

---

## Task 9: Hot-path — `OnConnClose` with `CloseReason`

**Files:** Modify `client/pool.go`, append test to `client/hooks_test.go`

- [ ] **Step 1: Append test**

```go
func TestHooks_OnConnClose_Idle(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var closeEvents []client.ConnCloseEvent
	var mu sync.Mutex
	hooks := &client.Hooks{
		OnConnClose: func(e client.ConnCloseEvent) {
			mu.Lock()
			closeEvents = append(closeEvents, e)
			mu.Unlock()
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			IdleTimeout:       50 * time.Millisecond,
			HealthCheckPeriod: 25 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(closeEvents)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(closeEvents) == 0 {
		t.Fatalf("OnConnClose never fired; expected at least 1 (idle eviction)")
	}
	if closeEvents[0].Reason != client.CloseIdle {
		t.Errorf("close reason = %v, want CloseIdle", closeEvents[0].Reason)
	}
	if closeEvents[0].Addr != addr {
		t.Errorf("close addr = %q, want %q", closeEvents[0].Addr, addr)
	}
}

// Add to imports: "sync", "time"
```

- [ ] **Step 2: Run — confirm fail**

Run: `go test ./client/ -run='TestHooks_OnConnClose_Idle' -count=1 -timeout=10s`
Expected: FAIL — no events.

- [ ] **Step 3: Refactor `evict*` to take `CloseReason`**

In `client/pool.go`, replace the three eviction helpers:

```go
// evict removes target from conns and closes its underlying conn.
// Fires OnConnClose with the supplied reason.
func (p *Pool) evict(conns []*managedConn, target *managedConn, reason CloseReason) []*managedConn {
	out := conns[:0]
	for _, mc := range conns {
		if mc == target {
			p.notifyClose(reason)
			_ = mc.c.Close()
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictIdle removes conns idle past PoolOptions.IdleTimeout.
func (p *Pool) evictIdle(conns []*managedConn) []*managedConn {
	if p.opts.IdleTimeout <= 0 {
		return conns
	}
	now := time.Now()
	out := conns[:0]
	for _, mc := range conns {
		if mc.active == 0 && now.Sub(mc.lastUsed) > p.opts.IdleTimeout {
			p.notifyClose(CloseIdle)
			_ = mc.c.Close()
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDead removes conns whose IsAlive returns false. Distinguishes
// peer GOAWAY (CloseGoAway) from local-side death (CloseDead) using
// (*conn.Conn).IsAlive's sub-state.
func (p *Pool) evictDead(conns []*managedConn) []*managedConn {
	out := conns[:0]
	for _, mc := range conns {
		if !mc.c.IsAlive() {
			reason := CloseDead
			if mc.c.GoAwayReceived() {
				reason = CloseGoAway
				p.metrics.Counters.GoAwaysReceived.Add(1)
			}
			p.notifyClose(reason)
			_ = mc.c.Close()
			continue
		}
		out = append(out, mc)
	}
	return out
}

// notifyClose increments ConnsClosed and fires OnConnClose.
func (p *Pool) notifyClose(reason CloseReason) {
	p.metrics.Counters.ConnsClosed.Add(1)
	if hr := p.hooksRef; hr != nil {
		if h := hr.Load(); h != nil && h.OnConnClose != nil {
			h.OnConnClose(ConnCloseEvent{Addr: p.addr, Reason: reason})
		}
	}
}
```

Update the only `evict()` caller (the release path ~line 250):
```go
			if !msg.mc.c.IsAlive() {
				reason := CloseDead
				if msg.mc.c.GoAwayReceived() {
					reason = CloseGoAway
					p.metrics.Counters.GoAwaysReceived.Add(1)
				}
				conns = p.evict(conns, msg.mc, reason)
			}
```

In the closeCh case (~line 285), fire the manual reason:
```go
		case <-p.closeCh:
			for _, w := range waiters {
				p.replyAcquire(w, nil, ErrPoolClosed)
			}
			waiters = nil
			for _, mc := range conns {
				p.notifyClose(CloseManual)
				_ = mc.c.Close()
			}
			return
```

- [ ] **Step 4: Add `GoAwayReceived` accessor on `*conn.Conn`**

Check `conn/conn.go` — `goAwayReceived` is a private `atomic.Bool`. Need a public method.

Run: `grep -n 'goAwayReceived\|GoAwayReceived' conn/conn.go`

Add at the bottom of `conn.go` near `IsAlive`:
```go
// GoAwayReceived reports whether the peer has sent a GOAWAY frame.
// Used by upstream pools to distinguish CloseGoAway from CloseDead.
func (c *Conn) GoAwayReceived() bool {
	return c.goAwayReceived.Load()
}
```

- [ ] **Step 5: Build + run test + race**

Run: `go build ./...`
Expected: PASS.

Run: `go test -race ./client/ -run='TestHooks_OnConnClose' -count=1 -timeout=15s`
Expected: PASS.

Run: `go test -race -count=1 ./...`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add client/pool.go client/hooks_test.go conn/conn.go
git commit -m "feat(client): C.4 — OnConnClose with CloseReason + GoAwayReceived"
```

---

## Task 10: Hot-path — `OnResolverUpdate`

**Files:** Modify `client/managed_pool.go`, append to `client/hooks_test.go`

- [ ] **Step 1: Append test (uses scriptedResolver from internal test pattern)**

Internal test, so it goes in a NEW external test file `client/managed_hooks_test.go` (avoid mixing internal scripted resolver with external `client_test` package — instead spin a small custom resolver in test).

Actually use a real DNS-style resolver: `client.StaticResolver` doesn't change set after creation, but the test can SetHooks then push a new set via a custom resolver.

```go
// client/managed_hooks_test.go
package client_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// pushResolver: test-only Resolver whose Watch channel is driven by push().
type pushResolver struct {
	initial []client.Address
	updates chan []client.Address
}

func newPushResolver(initial []client.Address) *pushResolver {
	return &pushResolver{initial: initial, updates: make(chan []client.Address, 4)}
}

func (r *pushResolver) Resolve(_ context.Context) ([]client.Address, error) { return r.initial, nil }

func (r *pushResolver) Watch(ctx context.Context) (<-chan []client.Address, error) {
	out := make(chan []client.Address, 1)
	out <- r.initial
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case set, ok := <-r.updates:
				if !ok {
					return
				}
				select {
				case out <- set:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (r *pushResolver) push(set []client.Address) { r.updates <- set }

func startTLSAddr(t *testing.T) (*httptest.Server, client.Address) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return srv, client.Address{Host: host, Port: port}
}

func TestHooks_OnResolverUpdate(t *testing.T) {
	t.Parallel()
	_, a1 := startTLSAddr(t)
	_, a2 := startTLSAddr(t)

	res := newPushResolver([]client.Address{a1})
	var updates atomic.Int32
	var sawAdd atomic.Bool
	hooks := &client.Hooks{
		OnResolverUpdate: func(e client.ResolverUpdateEvent) {
			if updates.Add(1) > 1 && len(e.Added) == 1 && e.Added[0] == a2 {
				sawAdd.Store(true)
			}
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  res,
		Selector:  client.RoundRobin(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	res.push([]client.Address{a1, a2})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sawAdd.Load() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("OnResolverUpdate never observed addr2 added; updates fired = %d", updates.Load())
}
```

- [ ] **Step 2: Run — confirm fail**

Run: `go test ./client/ -run='TestHooks_OnResolverUpdate' -count=1`
Expected: FAIL.

- [ ] **Step 3: Modify `managedPool.applySet` (in `client/managed_pool.go`)**

Compute Added/Removed during the diff and fire hook after lock release:

```go
func (mp *managedPool) applySet(next []Address) {
	mp.mu.Lock()
	prev := make(map[string]struct{}, len(mp.addrs))
	for _, a := range mp.addrs {
		prev[a.String()] = struct{}{}
	}
	nextSet := make(map[string]struct{}, len(next))
	for _, a := range next {
		nextSet[a.String()] = struct{}{}
	}
	var (
		toDrain []*subPoolState
		added   []Address
		removed []Address
	)
	for _, a := range next {
		if _, ok := prev[a.String()]; !ok {
			added = append(added, a)
		}
	}
	for _, a := range mp.addrs {
		if _, ok := nextSet[a.String()]; ok {
			continue
		}
		removed = append(removed, a)
		if s, ok := mp.subPools[a.String()]; ok && !s.draining {
			s.draining = true
			toDrain = append(toDrain, s)
		}
	}
	mp.addrs = append(mp.addrs[:0:0], next...)
	total := len(next)
	mp.mu.Unlock()

	for _, s := range toDrain {
		mp.beginDrain(s)
	}
	if len(added) > 0 || len(removed) > 0 {
		if hr := mp.hooksRef; hr != nil {
			if h := hr.Load(); h != nil && h.OnResolverUpdate != nil {
				h.OnResolverUpdate(ResolverUpdateEvent{
					Added: added, Removed: removed, Total: total,
				})
			}
		}
	}
}
```

- [ ] **Step 4: Run test + race suite**

Run: `go test ./client/ -run='TestHooks_OnResolverUpdate' -count=1 -race`
Expected: PASS.

Run: `go test -race -count=1 ./client/`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add client/managed_pool.go client/managed_hooks_test.go
git commit -m "feat(client): C.4 — OnResolverUpdate with Added/Removed diff"
```

---

## Task 11: Acquire-wait latency histogram

**Files:** Modify `client/pool.go`

- [ ] **Step 1: Add helper test in `client/metrics_test.go`**

```go
func TestMetrics_AcquireLatencyRecorded(t *testing.T) {
	// Tested via integration in Task 13's full-flow test; here just
	// confirm the histogram exists and is empty by default.
	var m Metrics
	if m.Latency.Acquire.Snapshot().Count != 0 {
		t.Error("acquire histogram not zero on init")
	}
}
```

- [ ] **Step 2: Modify `client/pool.go` `acquire` (the public `acquire` method that bridges to actor)**

Find:
```go
func (p *Pool) acquire(ctx context.Context) (*managedConn, error) {
	// ... existing body ...
}
```

Time the wait: capture `start := time.Now()` at the top, observe `time.Since(start)` before each return on the success path:

```go
func (p *Pool) acquire(ctx context.Context) (*managedConn, error) {
	start := time.Now()
	// ... existing wait + reply logic ...
	// On success path, BEFORE returning the mc:
	p.metrics.Latency.Acquire.Observe(time.Since(start))
	return mc, nil
}
```

Read the existing `acquire` body first (`grep -n 'func (p \*Pool) acquire' client/pool.go`) and apply the timing surgically.

- [ ] **Step 3: Run race suite**

Run: `go test -race -count=1 ./client/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add client/pool.go client/metrics_test.go
git commit -m "feat(client): C.4 — acquire-wait latency histogram"
```

---

## Task 12: Integration test — all hooks fire end-to-end

**Files:** Append to `client/hooks_test.go`

- [ ] **Step 1: Add test**

```go
func TestHooks_AllHooks_EndToEnd(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var (
		startN, completeN, dialN, closeN atomic.Int32
	)
	hooks := &client.Hooks{
		OnRequestStart:    func(client.RequestStartEvent)    { startN.Add(1) },
		OnRequestComplete: func(client.RequestCompleteEvent) { completeN.Add(1) },
		OnDial:            func(client.DialEvent)            { dialN.Add(1) },
		OnConnClose:       func(client.ConnCloseEvent)       { closeN.Add(1) },
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
		Transport: client.TransportPool,
		Pool:      &client.PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
			t.Fatalf("Do[%d]: %v", i, err)
		}
	}
	_ = c.Close()

	if got := startN.Load(); got != 5 {
		t.Errorf("OnRequestStart fired %d times, want 5", got)
	}
	if got := completeN.Load(); got != 5 {
		t.Errorf("OnRequestComplete fired %d times, want 5", got)
	}
	if dialN.Load() != 1 {
		t.Errorf("OnDial fired %d times, want 1", dialN.Load())
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && closeN.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if closeN.Load() != 1 {
		t.Errorf("OnConnClose fired %d times, want 1 (manual close)", closeN.Load())
	}

	// Verify counters too.
	snap := c.MetricsSnapshot()
	if snap.Counters.RequestsStarted != 5 {
		t.Errorf("RequestsStarted = %d, want 5", snap.Counters.RequestsStarted)
	}
	if snap.Counters.RequestsSucceeded != 5 {
		t.Errorf("RequestsSucceeded = %d, want 5", snap.Counters.RequestsSucceeded)
	}
	if snap.Counters.DialsAttempted != 1 {
		t.Errorf("DialsAttempted = %d, want 1", snap.Counters.DialsAttempted)
	}
	if snap.Counters.ConnsClosed != 1 {
		t.Errorf("ConnsClosed = %d, want 1", snap.Counters.ConnsClosed)
	}
	if snap.Latency.Request.Count != 5 {
		t.Errorf("Latency.Request.Count = %d, want 5", snap.Latency.Request.Count)
	}
}
```

- [ ] **Step 2: Run race suite**

Run: `go test -race -count=1 ./client/ -run='TestHooks_AllHooks_EndToEnd'`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add client/hooks_test.go
git commit -m "test(client): C.4 — end-to-end hooks + counters integration"
```

---

## Task 13: Benchmark — NoHooks vs WithHooks

**Files:** Create `client/bench_metrics_test.go`

- [ ] **Step 1: Write benchmark**

```go
// client/bench_metrics_test.go
package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func benchSetup(b *testing.B, hooks *client.Hooks) (*client.Client, func()) {
	b.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	return c, func() {
		_ = c.Close()
		srv.Close()
	}
}

func BenchmarkDo_NoHooks(b *testing.B) {
	c, cleanup := benchSetup(b, nil)
	defer cleanup()
	req := &client.Request{Method: "GET", Path: "/"}
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := c.Do(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDo_WithHooks(b *testing.B) {
	hooks := &client.Hooks{
		OnRequestStart:    func(client.RequestStartEvent) {},
		OnRequestComplete: func(client.RequestCompleteEvent) {},
	}
	c, cleanup := benchSetup(b, hooks)
	defer cleanup()
	req := &client.Request{Method: "GET", Path: "/"}
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := c.Do(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}
```

- [ ] **Step 2: Run benchmarks and capture numbers**

Run: `go test -bench='BenchmarkDo_' -benchmem -benchtime=2s -run='^$' ./client/ | tee /tmp/c4_bench.txt`

Record the `B/op` and `allocs/op` for each.

- [ ] **Step 3: Update `docs/BENCH_BASELINE.md`**

Append a C.4 section:

```markdown
## C.4 — observability path

Benchmarks against an httptest h2 server (latency dominated by socket I/O,
not by hook overhead).

| Bench                  | ns/op | B/op | allocs/op |
|------------------------|------:|-----:|----------:|
| BenchmarkDo_NoHooks    | <fill from bench>   | <fill> | <fill>    |
| BenchmarkDo_WithHooks  | <fill from bench>   | <fill> | <fill>    |

Gate: `BenchmarkDo_NoHooks.allocs/op` must equal the C.2 `BenchmarkDo`
baseline; the nil-hook path adds two atomic.Add ops + one Histogram.Observe
(no allocations).
```

Replace `<fill>` with the actual numbers from `/tmp/c4_bench.txt`.

- [ ] **Step 4: Commit**

```bash
git add client/bench_metrics_test.go docs/BENCH_BASELINE.md
git commit -m "bench(client): C.4 — Do_NoHooks vs Do_WithHooks baselines"
```

---

## Task 14: README phases update

**Files:** Modify `README.md`

- [ ] **Step 1: Update the Phase line**

In `README.md`, find the line:
```
- **C.3 / C.4 — discovery, stats** *(planned)*: service-discovery
  resolver, metrics callbacks.
```

Replace with:
```
- **C.3 — service discovery** *(this release)*: per-address sub-pool
  fan-out via `Resolver` + `Selector`. Built-in `StaticResolver`,
  `DNSResolver` with TTL cache + Watch, and `RoundRobin` / `Random` /
  `Hash` selectors. DrainGraceful / DrainHard / DrainLazy modes.
- **C.4 — metrics & hooks** *(this release)*: lifecycle `Hooks`
  (`OnRequestStart`, `OnRequestComplete`, `OnRetry`, `OnDial`,
  `OnConnClose`, `OnResolverUpdate`); lock-free counters and log-bucket
  latency histograms exposed via `(*Client).Metrics()` and
  `(*Client).MetricsSnapshot()`. Zero hot-path overhead when
  `ClientOptions.Hooks == nil`.
```

Also update the `**Status:**` line near the top to reflect C.4 complete.

- [ ] **Step 2: Run full lint + test**

Run: `go test -race -count=1 ./...`
Expected: PASS.

Run: `/tmp/golangci-v164/golangci-lint run ./...`
Expected: PASS (exit 0).

- [ ] **Step 3: Commit and push**

```bash
git add README.md
git commit -m "docs: README — C.3 + C.4 phases marked released"
git push -u origin claude/c4-metrics-observability
```

- [ ] **Step 4: Open draft PR**

Use `mcp__github__create_pull_request` with `draft: true`. Title:
`feat(client): C.4 — metrics & observability hooks (core)`. Body
summarises spec link, hook list, perf gate result, and follow-up
plan reference.

---

## Self-Review Checklist (run after all tasks)

- [ ] All 14 tasks complete and committed
- [ ] `go test -race -count=1 ./...` passes
- [ ] `golangci-lint v1.64 run ./...` passes
- [ ] Conformance gate: `go test -run=Conformance ./...` passes
- [ ] `BenchmarkDo_NoHooks` allocs/op unchanged from C.2 baseline
- [ ] PR drafted and pushed
