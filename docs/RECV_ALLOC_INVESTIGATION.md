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

## Target-load CPU + GC profile (the deciding measurement)

The alloc-count numbers above are from a loopback micro-benchmark. To settle
whether allocations matter *at all*, we took a **CPU profile with `gctrace`**
under sustained concurrent load (`-benchtime=5s`, `RunParallel` over one TLS
conn). This is the measurement the revisit-rule actually gates on.

| CPU consumer | share | note |
|---|---|---|
| socket write **syscall** (`syscall.SyscallN` → `runtime.cgocall` → `WSASend`) | **~50%** | inherent — you must call the OS to put bytes on the wire |
| **TLS crypto** (`crypto/.../aes/gcm.Seal`) | **~1.4%** | AES-NI hardware-accelerated; `crypto/tls.Conn.Write`'s 48% cum is ~47% the syscall it calls, not encryption |
| **GC** (`mallocgc` + concurrent mark) | **~1–3%** | `gctrace` reports a GC CPU fraction of **0–1%** every cycle; heap stays 3–5 MB; the few % includes the in-process `net/http2` test server |
| our codec (HPACK `EncodeBlock` + frame `WriteHeaders`, flat) | **~1%** | already tiny; the large *cumulative* % is the TLS write / syscall it feeds |

**The client is syscall-bound.** Both the alloc-count profile and the
target-load CPU+GC profile agree: TLS is not a hot spot (≈1% of allocations,
≈1.4% of CPU), GC costs ≈1% of CPU, and our encode path is already lean. The
dominant cost is the socket syscall, which is inherent; the only lever to reduce
it is batching writes into fewer syscalls. **Note (correction):** the
`docs/WRITE_QUEUE_INVESTIGATION.md` NO-GO rejected the **async writer**
(decouple the syscall from `wmu`) on a *lock-contention* discriminator — it did
**not** measure cross-frame **batching**, which is a separate question.
Cross-frame group-commit has since been measured and is a GO at high
single-conn concurrency (k up to 1.73 frames/flush, +26% req/s); see
`docs/WRITE_BATCHING_INVESTIGATION.md`.

**"Zero-alloc TLS" is doubly moot:** TLS barely allocates (~1%) *and* its crypto
is a rounding error (~1.4%, AES-NI). There is nothing in our code to make
zero-alloc that would move throughput, tail latency, or GC.

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
