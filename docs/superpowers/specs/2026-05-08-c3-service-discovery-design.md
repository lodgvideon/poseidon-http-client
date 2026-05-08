# C.3 Service Discovery — Design Spec

**Date:** 2026-05-08
**Phase:** C.3
**Status:** approved, awaiting plan

## Problem

`client.Client` targets a single `Addr string`. Real load generators
need to fan out across N backend instances behind a logical service
name, with discovery sourced from DNS, Consul, k8s endpoints, or a
static list. A user must currently spin up N separate `*Client`
instances and route requests manually — defeating pool-level metrics,
GOAWAY drain, and `MAX_CONCURRENT_STREAMS` accounting.

## Goal

Add a pluggable `Resolver` interface and per-address sub-pool topology
to `client.Client`:

1. `Resolver` returns the current address set; supports both pull
   (`Resolve`) and push (`Watch`).
2. Built-in `StaticResolver` and `DNSResolver`.
3. Pluggable `Selector` picks the next address for new dials;
   built-ins: `RoundRobin` (default), `Random`, `Hash`.
4. Per-address sub-pools. Each sub-pool is the existing `Pool`,
   scoped to one `Address`; aggregated under a single `managedPool`.
5. Configurable drain mode when an address is removed by the
   resolver: graceful (default), hard, lazy.
6. `ClientOptions.Addr` remains supported via an internal
   `StaticResolver` wrapper — fully backwards-compatible.

## Non-goals

- Health-check probes (active liveness over the resolved set).
  Pool-level GOAWAY/EOF eviction stays the only liveness signal.
- TLS SNI overrides per address. SNI keeps using the logical service
  name from `ClientOptions.Authority` (or `Addr`).
- Locality / zone-aware routing as a built-in Selector. Users plug
  their own via `Selector` interface and `Address.Attributes`.
- Per-address retry budgets. Retry layer (PR #19) is unchanged.
- gRPC `xds`-style hierarchical resolution. Single-tier resolver only.

## API

### Files

- `client/resolver.go` — new file: `Address`, `Resolver`,
  `ErrWatchUnsupported`, `StaticResolver`, `DNSResolver`,
  `DNSOptions`.
- `client/selector.go` — new file: `Selector`, `PickContext`,
  `RoundRobin`, `Random`, `Hash`.
- `client/managed_pool.go` — new file: `managedPool`,
  per-address `subPool` map, `Watch` consumer goroutine, drain
  state machine.
- `client/pool_transport.go` — modified: `poolTransport` always
  drives `managedPool` (which contains exactly one sub-pool when
  the resolver yields a single address). No separate single-Pool
  fast path: the cost of a 1-element map lookup is negligible vs
  the simplicity gain of a single code path.
- `client/client.go` — modified: `NewClient` constructs Resolver
  from `Addr` if not supplied; passes `Resolver`/`Selector`/
  `DrainMode` into transport. `PoolStats` aggregates across
  sub-pools.
- `client/errors.go` — additions: `ErrWatchUnsupported`,
  `ErrNoAddresses`.

### Public surface

```go
// Address is one resolved backend endpoint.
type Address struct {
    // Host is the dial target — IP literal or DNS name. The pool
    // never re-resolves Host; the Resolver owns that.
    Host string
    Port int
    // Attributes carries optional metadata for user Selectors
    // (zone, weight, etc.). Built-in selectors ignore it.
    Attributes map[string]string
}

// String returns "host:port".
func (a Address) String() string

// Resolver discovers backend addresses for a logical service. Must
// be goroutine-safe.
type Resolver interface {
    // Resolve returns the current address set. Implementations may
    // cache and serve from a TTL-backed cache. Returning a non-nil
    // err with len(addrs) == 0 is an error; non-nil err with
    // len(addrs) > 0 is treated as a soft warning (use cached set).
    Resolve(ctx context.Context) ([]Address, error)

    // Watch streams address-set updates. The first message MUST be
    // the current full set. Subsequent messages MUST also be the
    // full set (not deltas) to keep consumers simple. The channel
    // is closed when ctx is cancelled or on terminal error.
    // Implementations without push support return
    // ErrWatchUnsupported; managedPool falls back to a TTL ticker
    // around Resolve.
    Watch(ctx context.Context) (<-chan []Address, error)
}

// StaticResolver returns a fixed address set. Watch returns a
// closed channel after sending the single initial set, signaling
// no further updates.
func StaticResolver(addrs ...Address) Resolver

// DNSResolver resolves Host via *net.Resolver A/AAAA lookups.
// Watch is implemented as a ticker around Resolve at TTL cadence.
func DNSResolver(host string, port int, opts DNSOptions) Resolver

// DNSOptions configures DNSResolver.
type DNSOptions struct {
    // TTL governs both the cache lifetime returned by Resolve and
    // the Watch ticker period. Zero → 30s default.
    TTL time.Duration
    // Resolver is the underlying net.Resolver; nil → net.DefaultResolver.
    Resolver *net.Resolver
    // PreferIPv4 filters out AAAA results when both families resolve.
    // Default false (return both).
    PreferIPv4 bool
}

// Selector picks one address from a set for the next dial.
// Implementations must be goroutine-safe.
type Selector interface {
    Pick(set []Address, pc PickContext) (Address, error)
}

// PickContext carries optional hints to the selector. All fields
// are optional; zero-value PickContext is valid.
type PickContext struct {
    // Request is the in-flight request, if Pick is invoked from
    // Acquire path (vs background dial). May be nil.
    Request *Request
}

// RoundRobin returns a stateful Selector that rotates through the
// set in order. Wraps an atomic counter; safe for high-rate dials.
func RoundRobin() Selector

// Random returns a Selector that picks uniformly at random.
// Optional rng (defaults to time-seeded *rand.Rand with mutex).
func Random(rng *rand.Rand) Selector

// Hash returns a deterministic Selector that picks by hash(keyFn(pc)).
// keyFn must not return "" — empty key is treated as ErrNoAddresses
// (caller passed insufficient hint).
func Hash(keyFn func(PickContext) string) Selector

// DrainMode governs sub-pool lifecycle when an address is removed
// by a Watch update.
type DrainMode int

const (
    // DrainGraceful refuses new streams on the removed sub-pool;
    // existing in-flight streams complete; sub-pool closes when
    // active==0.
    DrainGraceful DrainMode = iota
    // DrainHard closes every conn in the removed sub-pool immediately;
    // in-flight streams surface as RST_STREAM(CANCEL).
    DrainHard
    // DrainLazy leaves the sub-pool intact (no new dials); idle
    // eviction (HealthCheckPeriod tick) is the only closer.
    DrainLazy
)

// Sentinels:
var ErrWatchUnsupported = errors.New("client: resolver does not support Watch")
var ErrNoAddresses     = errors.New("client: resolver returned no addresses")
```

### ClientOptions changes

```go
type ClientOptions struct {
    // Addr remains supported. When non-empty AND Resolver == nil,
    // NewClient wraps it in StaticResolver(parseHostPort(Addr)).
    // Mutually exclusive with Resolver: error if both set.
    Addr string

    // Resolver is the address source. Overrides Addr.
    Resolver Resolver

    // Selector picks the address for new dials. nil → RoundRobin().
    Selector Selector

    // DrainMode governs removed-address sub-pool lifecycle.
    // Zero value = DrainGraceful.
    DrainMode DrainMode

    // ... existing fields unchanged ...
}
```

## Data flow

### Dial path (request → conn)

```
poolTransport.Acquire(ctx, req)
  └─► managedPool.Acquire(ctx, req)
        ├─ snapshot current address set (excluding draining)
        ├─ if set empty → ErrNoAddresses
        ├─ addr := Selector.Pick(set, PickContext{Request: req})
        ├─ subPool := managedPool.subPools[addr]   (lazy-create)
        ├─ resp, err := subPool.Acquire(ctx, req)
        ├─ on dial-only error (DialError, ErrDialBackoff):
        │     remove addr from this call's candidate set,
        │     ask Selector again, retry — bounded by len(set).
        │     ctx-cancel breaks the loop immediately.
        └─ all other errors surface verbatim (caller's retry layer
           is the right place for request-level retry)
```

**Every Acquire goes through Selector.** No "first sub-pool with
slack" shortcut: that would defeat fair distribution across
backends, which is the whole point of sub-pools. Within the chosen
sub-pool, the existing least-loaded conn-selection logic stands.

The dial-error retry is address-level resilience (one backend
out of N is dead) — distinct from the request-level retry layer
(`Retryer`) which handles RFC 7540 §8.1.4. They compose: managedPool
tries to find an alive address; if all fail, it returns the last
DialError; Retryer (if used) sees the DialError and may retry the
whole call.

### Watch path (address updates)

```
managedPool.run() goroutine:
  for set := range resolverWatchCh:
    diff old vs new:
      added = new \ old   → no-op (sub-pools dial lazily on demand)
      removed = old \ new → for each: subPool.beginDrain(mode)
    swap addrSet atomically (snapshot read by Acquire)
```

`beginDrain` per `DrainMode`:
- `DrainGraceful`: subPool.refuseNew = true; existing Acquire calls
  drain naturally; `release()` decrement hits 0 → subPool.Close.
- `DrainHard`: subPool.Close immediately; in-flight Acquire callers
  receive `ErrPoolClosed` per current contract; in-flight streams
  surface as conn-EOF / RST.
- `DrainLazy`: subPool.refuseNew = true; sub-pool retained until the
  next idle eviction tick removes its conns naturally.

### Pull (TTL) fallback when Watch unsupported

When `Resolver.Watch` returns `ErrWatchUnsupported`, `managedPool.run`
starts a ticker at `DNSOptions.TTL` (or a default 30s) that calls
`Resolve` and synthesizes Watch-style update messages. Failure of
`Resolve` mid-life: log + retain previous set (graceful degradation).

## Stats aggregation

`Client.PoolStats()` returns the sum across all sub-pools. New
fields:

```go
type Stats struct {
    ActiveConns     int  // sum
    InFlightStreams int  // sum
    Waiters         int  // sum across sub-pools
    InFlightDials   int  // sum
    // C.3 additions:
    Addresses        int  // current resolved set size
    DrainingSubpools int  // count of sub-pools in drain state
}
```

`PoolStats()` walks the sub-pool map under read lock; each
sub-pool's `Stats()` is invoked individually (existing actor-based
contract preserved).

## Failure modes

| Condition | Behavior |
|---|---|
| Resolver returns 0 addresses, no cache | `Acquire` → `ErrNoAddresses` |
| Resolver returns 0 addresses, has cached set | retain cache, log warning, continue |
| All sub-pools refuse (drain or saturated) | `Acquire` → `ErrAcquireTimeout` (existing contract) |
| Selector returns error (e.g. Hash + empty key) | `Acquire` → that error verbatim |
| Watch closes unexpectedly | `managedPool.run` re-invokes `Resolver.Watch`; if that returns `ErrWatchUnsupported`, switches to ticker mode permanently |
| `ClientOptions.Addr` set AND `Resolver` set | `NewClient` → `ErrInvalidOptions` (new sentinel)|

## Concurrency

- `managedPool.addrSet` — `atomic.Value` holding `[]Address` snapshot.
  Read-mostly; `managedPool.run` is the only writer.
- `managedPool.subPools` — `map[Address]*subPool` guarded by `sync.RWMutex`.
  `Acquire` takes RLock; Watch handler takes Lock.
- Each sub-pool keeps its current actor-based concurrency unchanged.
- `RoundRobin` uses `atomic.Uint64` counter; `Random` wraps `*rand.Rand`
  with `sync.Mutex` (same pattern as retry layer's default backoff).
- `DNSResolver` cache: `atomic.Value` for cached set + `sync.Mutex`
  serializing concurrent `Resolve` to avoid thundering herd to DNS.

## Test plan

`client/resolver_test.go`:

| # | Test |
|---|---|
| 1 | `TestStaticResolver_Resolve` — fixed set returned verbatim |
| 2 | `TestStaticResolver_Watch_SingleEmission` — first send is full set, then closed |
| 3 | `TestDNSResolver_Resolve_HappyPath` — mock `*net.Resolver` returns 2 As; assert Address slice |
| 4 | `TestDNSResolver_Resolve_TTLCache` — second call within TTL hits cache, no second lookup |
| 5 | `TestDNSResolver_Resolve_StaleOnError` — lookup fails; cached set returned |
| 6 | `TestDNSResolver_Watch_TickerEmits` — ticker drives initial + one update on Resolve change |
| 7 | `TestDNSResolver_PreferIPv4_FiltersAAAA` — both families returned by mock; only A surfaces |

`client/selector_test.go`:

| # | Test |
|---|---|
| 8 | `TestRoundRobin_RotatesSet` — 3-element set, 6 picks, exact rotation |
| 9 | `TestRoundRobin_Concurrent_NoDuplicates` — 1000 picks across goroutines, count balance ±1 |
| 10 | `TestRandom_Distribution` — 10k picks, χ² within 5% of uniform |
| 11 | `TestHash_Deterministic` — same key → same Address across calls |
| 12 | `TestHash_EmptyKey_Errors` — keyFn returning "" → `ErrNoAddresses` |

`client/managed_pool_test.go`:

| # | Test |
|---|---|
| 13 | `TestManagedPool_StaticResolver_DialsAllAddresses` — 3 addrs, 3 concurrent reqs, sub-pool count == 3 |
| 14 | `TestManagedPool_RoundRobin_DistributesDials` — 4 addrs, N>>4 dials, each addr dialed at least N/4 - 1 times |
| 15 | `TestManagedPool_DrainGraceful_AddressRemoved_KeepsInFlight` — Watch update removes addr X mid-flight; in-flight stream completes; new dials avoid X |
| 16 | `TestManagedPool_DrainHard_AddressRemoved_ClosesConns` — Watch update + DrainHard; in-flight stream surfaces conn error |
| 17 | `TestManagedPool_DrainLazy_AddressRemoved_NoNewDials` — Watch update + DrainLazy; refuseNew set; idle tick closes |
| 18 | `TestManagedPool_AddressAdded_LazyPickup` — Watch update adds addr; subsequent dials include it via RoundRobin |
| 19 | `TestManagedPool_NoAddresses_ReturnsErrNoAddresses` — Resolver returns []; Acquire surfaces ErrNoAddresses |
| 20 | `TestManagedPool_WatchClosesUnexpectedly_FallsBackToPull` — Watch closes mid-life; managedPool re-Resolves at TTL |
| 21 | `TestManagedPool_StatsAggregation` — 3 sub-pools, sum equals Client.PoolStats |

`client/client_test.go` additions:

| # | Test |
|---|---|
| 22 | `TestNewClient_AddrAndResolver_BothSet_Errors` |
| 23 | `TestNewClient_AddrOnly_WrapsInStaticResolver` |
| 24 | `TestNewClient_NoAddrNoResolver_Errors` |

Coverage target: ≥80% for `resolver.go`, `selector.go`,
`managed_pool.go` (project policy).

## Backwards compatibility

| Surface | Effect |
|---|---|
| `ClientOptions.Addr` (string) | Unchanged. Wrapped in `StaticResolver` internally. Existing tests pass without modification. |
| `Client.PoolStats` | Same struct + 2 new fields (`Addresses`, `DrainingSubpools`). Existing fields unchanged. |
| `Pool` (the existing type) | Unchanged. Continues to be the per-address sub-pool. |
| `poolTransport` | Internal type; refactored to drive `managedPool`. No public surface impact. |

## Risks

- **Selector misuse for stickiness**: users may reach for `Hash`
  expecting per-stream stickiness across reconnects. Document
  explicitly: `Hash` only governs *new dial* address picks;
  stream-to-conn affinity is owned by the sub-pool's least-loaded
  selection.
- **Watch implementations leak goroutines**: managedPool MUST
  cancel the watch ctx on `Close`. Test 20 covers Watch closure;
  add explicit goroutine-leak test via `goleak` (or runtime
  goroutine count delta) in `TestManagedPool_Close_LeakClean`.
- **DNS resolver thundering herd**: simultaneous Resolve calls on
  cache-miss hammer net.Resolver. Mitigated by per-Resolver mutex
  serializing concurrent first-fetch; documented in
  DNSResolver doc.
- **Watch fallback semantics**: when an explicit-push Resolver
  returns `ErrWatchUnsupported`, falling back to ticker is a
  silent behavior change. Doc-comment on `Watch` flags this.

## Open questions

None at this time. All scope decisions captured.
