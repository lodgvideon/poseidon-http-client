# Receive-Path Allocation Investigation

**Status:** investigation / decision record — **no code change** (deliberate NO-GO).
**Trigger:** "remove the remaining hot paths and make zero-alloc TLS the hottest point."

## TL;DR

Profiling **killed the premise**: `crypto/tls` is ~1% of receive-path
allocations, not a hot spot. The single biggest remaining per-request
allocation is the per-stream event channel (~24%), but re-allocating it is a
**load-bearing synchronization mechanism**, not waste — removing it cheaply is
unsafe, and removing it safely costs more (a lock on the hottest goroutine, or a
race) than the ~1 alloc/request it saves. **Decision: leave the receive path
as-is.** It is already effectively zero-alloc on the parts we control.

## Data (alloc_space, `BenchmarkConn_Roundtrip_Concurrent`, loopback h2/TLS)

Filtered to our packages + `crypto/tls` (the raw profile also includes the
in-process `net/http2` test server, which is noise):

| Allocation | Share | Notes |
|---|---|---|
| `conn.newStream` → `make(chan StreamEvent, buf)` | **~24%** | per-stream event channel |
| `conn.newStream` → `make(chan struct{})` (resetSignal) | ~3.7% | per-stream reset signal |
| `connHandler.emitHeaderBlock` → `make([]HeaderField, n)` | ~3.7% | decoded response-header slice |
| `headerSlabPool.New` (`make([]byte,0,512)`) | ~15% | **benchmark artifact** — the raw-conn bench never returns slabs to the pool; the production client (`Response.Reset` / `sr.Close`) does, so the pool is reused in real use |
| `crypto/tls` (all) | **~1%** | record buffers are reused inside `tls.Conn`; `readFromUntil` is the largest at 0.48% |

Everything else on the receive path is already 0-alloc: DATA buffers pooled
(`dataBufPool`), header-slab bytes pooled (`headerSlabPool`), HPACK 0-alloc, and
the `Stream` struct itself pooled (`streamPool`). Only the Stream's two channels
re-allocate per lifetime.

## Why the event channel cannot be cheaply reused

`recycleStream` re-`make`s `s.events` on every reuse **on purpose**. The fresh
channel is a fence: it orphans any late `s.events <- event` from the single
reader goroutine (on the RST / push-overflow / terminal-push edges) that races
the caller returning the `*Stream` to the pool. The "no goroutine still
references the recycled stream" precondition is enforced by convention in the
`On*` handlers, not by the type system, and the reset/overflow edges have a
reachable window between registry-eviction (`markStreamDone`) and the `push`.

- **Bare drain-and-reuse:** UNSAFE — a stale push lands in the channel now owned
  by the next stream's lifetime → cross-stream event corruption.
- **Provably-safe reuse (atomic evict+reuse under a lock, `push` gated on a
  per-stream generation):** correct, but adds a mutex acquire to the *fast path
  of every event delivery* on the hottest goroutine. For a load generator that
  trades tail latency for GC headroom it does not need — the wrong direction.
- **Channel-object pool (`sync.Pool` of `chan StreamEvent`, drain on handout):**
  recovers the alloc but leaves a residual stale-write-vs-handout-drain window —
  i.e. an unreproducible cross-stream corruption bug under peak load, which for a
  load generator poisons the very measurements it exists to produce.

## Decision

**Do not optimize the receive-path allocations.** The channel re-make is a good
trade (one ~200-byte alloc per request for a lock-free, provably-orphaning
recycle protocol). `resetSignal` (3.7%) and the `[]HeaderField` slice (3.7%) are
too small to justify pooling churn, and `crypto/tls` (~1%) is not worth touching
(and is standard-library anyway).

## Revisit rule

Reopen this only if **`runtime.mallocgc` / GC shows up in a CPU profile at
target load** (not an alloc-count profile on a loopback micro-benchmark). At
that point the `[]HeaderField` slice pool is the first cheap, safe candidate;
channel reuse remains gated on making the recycle invariant code-enforced.
