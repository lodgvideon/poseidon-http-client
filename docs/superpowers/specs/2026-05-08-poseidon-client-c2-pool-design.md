# Poseidon Client — Phase C.2: Per-Host Connection Pool — Design

**Status:** Draft
**Date:** 2026-05-08
**Predecessor:** [C.1 — public client API](2026-05-07-poseidon-client-c1-design.md)
**Successor (planned):** C.3 — service discovery resolver

## Goal

Add a per-host connection pool to the `client` package so a single
`*client.Client` can drive a load generator across multiple HTTP/2
connections to the same host:port, growing connection count under
saturation and shedding idle / dead connections in the background.

C.2 is the second sub-phase of Phase C. C.1 shipped `client.Client`
with a single-connection lazy-dial transport. C.2 adds a pool
transport that is selected explicitly via `ClientOptions.Transport`.
The single-connection path remains the default to keep existing
behavior bit-identical.

## Non-goals

- Multi-host pooling, key partitioning beyond `host:port` (deferred to
  C.3 service discovery — that phase will likely restructure how
  pools are keyed and looked up).
- TLS-identity-aware partitioning. The `tls.Config` is set once per
  pool via `ConnOpts` and shared across all connections in that pool.
- Adaptive autotune of `MaxConnsPerHost` / `MaxStreamsPerConn`. Both
  are caller-configured.
- Connection warm-up (pre-dial N at construction). The pool is lazy.
  May add `WarmUp` flag in a later sub-phase if benchmarks justify it.
- HTTP/3.

## Design choices and rationale

| Decision | Choice | Reasoning |
|---|---|---|
| Pool sizing | Lazy grow up to `MaxConnsPerHost` | Adapts to load. Don't pay for idle conns; don't refuse traffic if peer cap allows more. |
| Stream selection | Least-loaded (lowest `active` count) | Even spread under bursty load. Round-robin can pile onto a flow-controlled conn that has no send credit. |
| Eviction | Active background tick: idle timeout + `IsAlive` health check | Catches half-dead conns (peer dropped TCP without GOAWAY) before next caller hits them. |
| Pool key | `host:port` only | Single-target load generator is the common case. Multi-host belongs to C.3. |
| Concurrency model | **Single actor goroutine owning all pool state**; communication via channels | No shared-state mutex. Easier to reason about lifecycle; idle / health ticks live in the same `select` as acquire / release. Trade-off: serializes acquire / release through one goroutine. Bench will quantify; mutex variant remains a future option if contention shows up. |
| Transport selection | Explicit enum on `ClientOptions.Transport` (`TransportSingleConn` default, `TransportPool`) + `Pool *PoolOptions` field required for pool | Caller picks the strategy; no implicit switching based on field presence. Forward-compatible — new strategies can be added without breaking. |

## Public API

### `ClientOptions` extensions

```go
package client

// TransportKind selects which transport strategy a Client uses.
type TransportKind int

const (
    // TransportSingleConn keeps C.1 behavior: one *conn.Conn shared
    // by every request, lazy-dial, conn-only auto-redial after
    // GOAWAY / IsAlive=false.
    TransportSingleConn TransportKind = iota

    // TransportPool routes requests through *Pool. PoolOptions
    // must be non-nil.
    TransportPool
)

type ClientOptions struct {
    Addr      string
    ConnOpts  conn.ConnOptions

    // Transport selects the transport strategy.
    // Zero value = TransportSingleConn.
    Transport TransportKind

    // Pool is required iff Transport == TransportPool.
    // Ignored otherwise (validation: non-nil Pool with
    // TransportSingleConn returns ErrInvalidPoolOptions).
    Pool *PoolOptions

    // existing C.1 fields preserved unchanged
    DialBackoff time.Duration
}
```

`NewClient` validates:

- `Transport == TransportPool && Pool == nil` → `ErrInvalidPoolOptions`.
- `Transport == TransportSingleConn && Pool != nil` → `ErrInvalidPoolOptions`. Strict, no silent ignore.
- `Transport` value outside the defined enum → `ErrInvalidTransportKind`.

### `PoolOptions`

```go
package client

// PoolOptions configures the pool transport. Zero values are
// replaced with sensible defaults at NewClient.
type PoolOptions struct {
    // MaxConnsPerHost caps live connections in this pool.
    // 0 → 1 (effectively single-conn).
    MaxConnsPerHost int

    // MaxStreamsPerConn is the soft cap on concurrent streams the
    // pool will assign to one connection. The effective cap is
    // min(this, peer SETTINGS_MAX_CONCURRENT_STREAMS) where the
    // peer value is observed from the conn's *Conn after handshake.
    // 0 → use peer value only (or local default 100 if peer is
    // unbounded).
    MaxStreamsPerConn int

    // IdleTimeout closes a conn that has been idle (active==0)
    // longer than this duration. 0 → never close on idle.
    IdleTimeout time.Duration

    // HealthCheckPeriod is the actor's tick interval for idle and
    // health-check sweeps. 0 → 30 * time.Second.
    HealthCheckPeriod time.Duration

    // DialBackoff refuses new dials within this window after a
    // dial failure on this pool. Returns ErrDialBackoff to waiters
    // until the window expires. 0 → 1 * time.Second.
    DialBackoff time.Duration

    // AcquireTimeout bounds how long Acquire waits for capacity.
    // 0 → governed by ctx only.
    AcquireTimeout time.Duration
}
```

### `Pool`

```go
package client

// Pool is a per-host pool of *conn.Conn instances. The zero value
// is not usable; construct via newPool (internal). The pool is
// owned by a Client and is not currently exported as a standalone
// constructor — that is reserved for a later sub-phase.
type Pool struct {
    // unexported fields: actor channels, opts, conn slice
}

// Stats is a snapshot of pool state. Returned by Pool.Stats().
type Stats struct {
    ActiveConns     int
    InFlightStreams int
    Waiters         int
    InFlightDials   int
}

func (p *Pool) Stats() Stats        // request via actor; safe concurrently
func (p *Pool) Close() error        // graceful: drain waiters, close conns
```

`Pool` is constructed only through `NewClient` in C.2. A standalone
`NewPool` constructor may be added later if a use case emerges.

### Errors

```go
var (
    ErrPoolClosed         = errors.New("client: pool closed")
    ErrPoolExhausted      = errors.New("client: pool exhausted: cap reached and all conns saturated")
    ErrAcquireTimeout     = errors.New("client: acquire timeout")
    ErrDialBackoff        = errors.New("client: dial backoff active")
    ErrInvalidPoolOptions = errors.New("client: invalid pool options")
    ErrInvalidTransportKind = errors.New("client: invalid transport kind")
)
```

`ErrPoolExhausted` only surfaces when `MaxConnsPerHost` reached AND
every conn is at its stream cap AND the caller declines to wait
(future field; for now waiters always queue, ctx / `AcquireTimeout`
governs).

## Internal architecture

### Actor goroutine

```go
type Pool struct {
    opts        PoolOptions
    connOpts    conn.ConnOptions
    addr        string

    acquireCh   chan acquireReq
    releaseCh   chan releaseMsg
    statsCh     chan chan Stats
    dialDoneCh  chan dialResult
    closeCh     chan struct{}
    closedCh    chan struct{}
}

type managedConn struct {
    c        *conn.Conn
    active   int
    lastUsed time.Time
    dialedAt time.Time
}

type acquireReq struct {
    ctx   context.Context
    reply chan acquireResp
}
type acquireResp struct {
    mc  *managedConn
    err error
}

type releaseMsg struct {
    mc  *managedConn
    err error // non-nil → caller's request failed; pool re-checks IsAlive
}

type dialResult struct {
    mc  *managedConn
    err error
}
```

Single goroutine `Pool.run()` owns `[]*managedConn`, the pending
`waiters` slice, `inFlightDials int`, and `lastDialErrAt time.Time`.
All state mutation happens in this goroutine. No mutexes.

### Select loop

```
for {
    select {
    case req := <-acquireCh:
        // 1. Find least-loaded live mc with active < effectiveCap.
        //    If found → mc.active++, send acquireResp{mc:mc}.
        // 2. Else if can dial (len(conns)+inFlightDials < MaxConnsPerHost
        //    && time.Since(lastDialErrAt) > DialBackoff):
        //    spawn dial goroutine, inFlightDials++, queue req.
        // 3. Else: queue req as waiter.
    case msg := <-releaseCh:
        msg.mc.active--; msg.mc.lastUsed = now
        if msg.err != nil && !msg.mc.c.IsAlive() {
            evict(msg.mc)
        }
        // Try to satisfy oldest waiter with any conn now under cap.
        servePendingWaiters()
    case dr := <-dialDoneCh:
        inFlightDials--
        if dr.err != nil {
            lastDialErrAt = now
            failOldestWaiter(dr.err)
        } else {
            conns = append(conns, dr.mc); dr.mc.active = 1
            handToOldestWaiter(dr.mc) // or release-back if no waiters
        }
    case respCh := <-statsCh:
        respCh <- snapshot()
    case <-tick.C:
        evictIdle(); healthCheck()
    case <-closeCh:
        drainWaiters(ErrPoolClosed)
        closeAllConns()
        close(closedCh)
        return
    }
}
```

### Dial goroutine

```go
func (p *Pool) dialOne(ctx context.Context) {
    c, err := conn.Dial(ctx, p.addr, p.connOpts)
    if err != nil {
        p.dialDoneCh <- dialResult{err: err}
        return
    }
    p.dialDoneCh <- dialResult{mc: &managedConn{c: c, dialedAt: time.Now()}}
}
```

`ctx` is derived from the longest pending waiter's ctx OR from a
fresh `context.Background()` with `AcquireTimeout`. Decision: use
`context.Background()` so the dial outlives any single waiter
cancellation — once the conn lands it can serve the next waiter.

### Effective stream cap per conn

```go
func effectiveStreamCap(opts PoolOptions, c *conn.Conn) int {
    peerCap := c.PeerMaxConcurrentStreams() // already exposed in B.2.5
    local := opts.MaxStreamsPerConn
    if local <= 0 && peerCap <= 0 {
        return 100
    }
    if local <= 0 {
        return peerCap
    }
    if peerCap <= 0 {
        return local
    }
    if peerCap < local {
        return peerCap
    }
    return local
}
```

If peer dynamically lowers `MAX_CONCURRENT_STREAMS` mid-life via
SETTINGS, the next acquire on that conn will simply see a smaller
cap and route elsewhere. Already-active streams are unaffected
(RFC 7540 §6.5.2).

### Eviction

`evictIdle()`:

```
for each mc in conns:
    if mc.active == 0 && now.Sub(mc.lastUsed) > opts.IdleTimeout:
        mc.c.Close()
        remove from conns
```

`healthCheck()`:

```
for each mc in conns:
    if !mc.c.IsAlive():
        mc.c.Close() // idempotent
        remove from conns
        // Active streams on a dead conn fail with their own conn-layer error.
        // The pool simply forgets it.
```

Both run on the same `HealthCheckPeriod` tick.

### Lease handle

```go
type Lease struct {
    pool *Pool
    mc   *managedConn
}

func (l *Lease) Conn() *conn.Conn   { return l.mc.c }
func (l *Lease) Release(reqErr error) {
    if l.mc == nil { return }
    l.pool.releaseCh <- releaseMsg{mc: l.mc, err: reqErr}
    l.mc = nil
}
```

`poolTransport` (internal) calls `Acquire`, runs the request via
`l.Conn()`, and `defer l.Release(err)` so accounting is exact even
on panic.

### Transport interface (internal, unchanged from C.1)

Both `singleConn` and `poolTransport` already implement the existing
internal `transport` interface introduced in C.1. C.2 only adds the
new struct and switches `NewClient` on `TransportKind`.

## Data flow — example: `Do` under pool transport

1. `Client.Do(ctx, req)` → `poolTransport.do(ctx, req)`.
2. `poolTransport.do` builds `acquireReq{ctx, reply}`, sends to
   `Pool.acquireCh`, receives `acquireResp`.
3. On `acquireResp.err != nil` → return error.
4. On success: run request against `lease.Conn()` exactly as the
   C.1 single-conn path does (HEADERS + DATA + Recv).
5. `defer lease.Release(reqErr)`. Pool actor decrements `active`,
   re-checks alive on error, hands conn to next waiter if any.

## Concurrency invariants

- `managedConn.active`, `managedConn.lastUsed`, `len(conns)`,
  `inFlightDials`, `waiters` are touched ONLY by the actor.
  Test under `-race`.
- `Pool.Close` is idempotent. Closing while requests are in flight:
  request goroutines hold their lease → `Release` finds the actor
  goroutine has exited → fall back to `mc.c.Close()` directly under
  a `sync.Once` per mc to avoid double-close.
- A request that's already running on a `*conn.Conn` is unaffected
  by pool close — `conn.Conn.Close` cancels in-flight streams via
  the existing B-layer mechanism.

## Failure modes

| Case | Behavior |
|---|---|
| Dial error | `inFlightDials--`, `lastDialErrAt = now`. Oldest waiter sees the dial err. Subsequent waiters within `DialBackoff` see `ErrDialBackoff`. |
| Peer GOAWAY | Conn-layer marks `IsAlive=false`. Next health tick evicts. New streams on existing conns continue. New acquires either pick another conn or trigger a fresh dial. |
| `MaxConnsPerHost` reached + saturated + new acquire | Waiter queues. Resolves on next release / new conn / ctx cancel / `AcquireTimeout`. |
| Pool close mid-flight | Active `Lease.Release` calls succeed (channel send may race with shutdown — guarded by `select` on `closedCh`). Pending waiters get `ErrPoolClosed`. Pending dials are best-effort cancelled (their goroutines complete, results dropped). |
| Conn dies while serving stream | Stream returns conn-layer error. Pool's `releaseMsg.err != nil` triggers `IsAlive` check, evicts conn. |

## Migration / compatibility

C.2 is purely additive. `Transport` defaults to `TransportSingleConn`,
so existing C.1 code continues to work bit-identical. The
`Client.Do` / `Client.DoStream` API is unchanged. The internal
`transport` interface gains a second implementation but is not
exported.

`(*conn.Conn).IsAlive` (added in C.1) is reused. C.2 may need
`(*conn.Conn).PeerMaxConcurrentStreams() int` if not already
exported by B.2.5. Verify at implementation start; if not exported,
add it under the same one-line policy as `IsAlive`.

## Testing

### Unit (`client/pool_test.go`)

- `TestPool_Acquire_LazyDial_FirstUse` — cold pool, first acquire dials.
- `TestPool_Acquire_GrowsToCap_WhenSaturated` — saturate first conn at effective cap → next acquire dials a second conn.
- `TestPool_Acquire_BlocksWhenAtCapAndAllSaturated` — `MaxConnsPerHost=2`, both saturated → 3rd acquire waits.
- `TestPool_Acquire_RespectsCtxCancel` — waiter ctx cancel → returns `ctx.Err()`.
- `TestPool_Acquire_RespectsAcquireTimeout` — `AcquireTimeout` shorter than ctx → `ErrAcquireTimeout`.
- `TestPool_Acquire_LeastLoadedSelection` — three conns with different `active` counts, pool picks lowest.
- `TestPool_Release_HandsConnToWaiter` — fill, queue waiter, release → waiter resolves.
- `TestPool_Release_DeadConn_Evicted` — release with non-nil err and `IsAlive()==false` → evicted.
- `TestPool_DialFailure_Backoff` — first dial errors → next acquire within `DialBackoff` returns `ErrDialBackoff`.
- `TestPool_ConcurrentDialDedup` — N concurrent acquires on cold pool with `MaxConnsPerHost=2` → exactly 2 dials.
- `TestPool_IdleEviction` — conn idle > `IdleTimeout`, `active==0` → tick closes it.
- `TestPool_HealthCheck_DropsDead` — `IsAlive=false` on conn → tick evicts.
- `TestPool_GoAwayMidFlight_NewAcquireRedials` — peer GOAWAY → conn drained → next acquire gets fresh conn.
- `TestPool_Close_DrainsWaiters_WithErrPoolClosed`.
- `TestPool_Close_AbortsInFlightDials`.
- `TestPool_Stats_Snapshot` — concurrent `Stats()` returns coherent counts.

### Client wiring (`client/client_test.go` extensions)

- `TestClient_NewClient_Pool_RequiresPoolOptions` — `Transport=Pool, Pool=nil` → `ErrInvalidPoolOptions`.
- `TestClient_NewClient_SingleConn_RejectsPoolOptions` — `Transport=SingleConn, Pool=non-nil` → `ErrInvalidPoolOptions`.
- `TestClient_NewClient_InvalidTransportKind` — out-of-range → `ErrInvalidTransportKind`.
- `TestClient_Pool_DoConcurrent_LoadSpread` — 200 concurrent reqs with `MaxConnsPerHost=4`, `MaxStreamsPerConn=10` → `Stats().ActiveConns >= 2` (load actually spread).

### Integration (`client/integration_test.go`)

Against `httptest.NewUnstartedServer` + `EnableHTTP2=true`:

- `TestIntegration_ClientPool_ConcurrentRequests_TwoConns` — 50 concurrent requests, `MaxConnsPerHost=2`, `MaxStreamsPerConn=10` → all 200, both conns used.
- `TestIntegration_ClientPool_GoAwayMidFlight_Replaces` — server sends GOAWAY → next request lands on freshly dialed conn.
- `TestIntegration_ClientPool_IdleEviction_ReDials` — short `IdleTimeout`, idle past → next request triggers new dial.
- `TestIntegration_ClientPool_DoStream_AcrossConns` — multiple `DoStream` calls in flight spread across conns.

### Race + bench

- All pool tests under `-race`.
- `BenchmarkPool_Acquire_Hot` — visibility, not gated.
- `BenchmarkPool_AcquireRelease_Concurrent_NWorkers` — measure actor channel cost; informs future mutex variant decision.

### RFC trace (additions to `docs/RFC_COVERAGE.md`)

- `TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams` — pool respects peer-advertised `SETTINGS_MAX_CONCURRENT_STREAMS`.
- `TestConformance_RFC7540_Sec6_8_PoolDrainsOnGoAway` — pool stops sending new streams to a conn after peer GOAWAY.

## Acceptance criteria

1. All unit tests above pass under `make test-race`.
2. All integration tests above pass under `make test-race`.
3. `make lint` clean (golangci-lint v1.64).
4. New conformance rows added to `docs/RFC_COVERAGE.md`.
5. README "Phases" section updated with C.2 entry; quick-start gains
   a "Pool" section showing `Transport: client.TransportPool` usage.
6. `Pool.Stats()` works concurrently and is race-free.
7. `Client` with `Transport: TransportSingleConn` is bit-identical to
   C.1 (existing C.1 tests pass unchanged).
8. No new exported symbols on `conn` beyond at most
   `PeerMaxConcurrentStreams() int` (only if not already exported).
9. `bench-gate` zero-alloc invariants on `frame` + `hpack` remain
   intact. Pool allocates per request; not gated.

## Open questions / deferred

- **Per-host pool registry / multi-host** — C.3 service discovery.
- **`NewPool` standalone constructor** — defer until a user needs a
  pool independent of `Client`.
- **Adaptive cap** — punt; manual config is fine for load-gen.
- **Connection warm-up** (`WarmUp int` on `PoolOptions` to pre-dial
  N at construction) — punt; add later if benches show cold-start
  cost matters.
- **Mutex variant** — keep as fallback if actor-channel benchmark
  shows contention; not in C.2.
- **Per-pool `MaxWaiters`** — currently waiters queue is unbounded
  (ctx / `AcquireTimeout` governs). Add only if a user reports OOM
  on backlog.
