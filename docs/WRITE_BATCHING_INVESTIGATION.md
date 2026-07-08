# Write-Batching Investigation (group-commit under `wmu`)

**Status:** measured **GO**; shipped as an **opt-in** feature
(`ConnOptions.GroupCommit`, default off).
**Trigger:** review of [ozontech/framer](https://github.com/ozontech/framer),
which ships cross-request write batching as its headline syscall-reduction
technique. See also [WRITE_QUEUE_INVESTIGATION.md](WRITE_QUEUE_INVESTIGATION.md)
(the earlier async-writer NO-GO) and
[RECV_ALLOC_INVESTIGATION.md](RECV_ALLOC_INVESTIGATION.md) (syscall ≈ 50% CPU).

## 1. What framer does, and why writev does NOT transfer

framer funnels all frames through one writer goroutine that coalesces many
requests' frames into a single `net.Buffers.WriteTo` (writev). **That specific
mechanism gives poseidon zero benefit**, because poseidon writes through
`crypto/tls.Conn`:

- `net.Buffers.WriteTo` only vectorizes if the target implements the unexported
  `net.buffersWriter`. `crypto/tls.Conn` does not — it exposes only
  `Write([]byte)`. So `net.Buffers.WriteTo(tlsConn)` falls into the per-buffer
  fallback loop, calling `tls.Conn.Write` once **per buffer**, each encrypting
  ≥1 record (≤16 KiB) and doing ≥1 socket write. Verified against Go stdlib
  (`net/net.go` `WriteTo` fallback; `crypto/tls/conn.go` `writeRecordLocked`,
  no application-data buffering).
- The **only** syscall-reduction lever under userspace TLS is to concatenate
  co-queued frames into **one plaintext buffer** and call `tls.Conn.Write`
  once, cutting N small-frame socket writes to `ceil(bytes / 16 KiB)` records.

poseidon's v0.7.1 `bufio.Writer` already coalesces a single frame's
header+payload into one `tls.Conn.Write`, but **flushes per frame** — it never
batches frame N with frame N+1. Batching = stop flushing per frame; let several
co-queued frames share one flush.

## 2. Design: synchronous group-commit under the existing `wmu`

No async writer, no queue, no timer — one policy change under the existing lock
(`conn/conn.go`, gated by `ConnOptions.GroupCommit`):

- Count writers queued on `wmu` (`writeWaiters`, incremented right before
  `Lock`, decremented right after acquiring).
- In `writeHeadersWithPriority`, after buffering the frame, `commitFrame`
  **defers the flush** when `writeWaiters > 0` and the buffer is below
  `groupCommitFlushBytes` (8 KiB): a queued writer will append its frame and
  flush the convoy in one `tls.Conn.Write`.
- **Synchronous — per-frame error semantics preserved.** The deferring writer
  waits on `flushCond` for the convoy flush, then returns that flush's
  **sticky, conn-fatal** error (`flushErr`). No async-error contract change, so
  no public API change and no wire-byte-assertion test sweep. It waits at most
  one broadcast, then flushes itself, which — together with the byte threshold
  — bounds both the wait and the convoy size and guarantees regular flushes
  under sustained load. Because the caller holds `wmu` continuously from the
  `writeWaiters` check into `flushCond.Wait` (which atomically releases it), the
  waker's broadcast cannot be missed.
- **Adaptive / zero-cost when idle.** No waiter → flush immediately →
  byte-identical to v0.7.1. `flushDeferring` gates the broadcast so an
  uncontended write does no cond work; the only always-on cost is the
  `writeWaiters` atomics. Unlike framer's 1 ms timer, a lone request is **never**
  delayed (it even leaves earlier — it skips its own lock-acquire-then-flush
  turn). Invariants I1–I7 from the write-queue doc are untouched — `wmu` still
  serializes everything; HPACK/stream-ID ordering is unchanged.
- **Scope:** only the HEADERS terminal flush defers. The
  flush-before-flow-control-block in `writeData` stays unconditional (skipping
  it would deadlock), so batching covers the HEADERS (GET) path — the
  load-generator common case.

## 3. Measurement

Both benches use an in-process `net/http2` 204 server over one/several TLS `h2`
connections, transport wrapped in a write-counter (one count per
`tls.Conn.Write` — the honest syscall proxy). `k = framesSent / transportWrites`
= mean frames per flush; `k > 1` means batching happened. `GOMAXPROCS=2`,
AMD Ryzen 7 7700.

### Single connection (`conn/groupcommit_bench_test.go`)

| Workers | k (off→on) | ns/op (off→on) | req/s (off→on) |
|---|---|---|---|
| **1**  | 1.00 → **1.00** | ~62k → ~62k | no-op |
| **8**  | 1.00 → **1.27** | 16.4k → 15.0k | +9% |
| **64** | 1.00 → **1.73** | 13.2k → 10.5k | **+26%** |

`k` rises monotonically with per-conn concurrency (more co-queued writers →
bigger convoys). **W1 is a byte-identical no-op.** Race-clean under `-race`.

### Pool sweep (`client/groupcommit_pool_bench_test.go`, W=64)

| Pool config | k (on) | req/s (off→on) |
|---|---|---|
| 1 conn, 100 streams/conn   | 1.68 | 61k → 79k (**+30%**) |
| 4 conns, 100 streams/conn  | 1.63 | 61k → 79k (**+30%**) |
| 8 conns, 100 streams/conn  | 1.70 | 60k → 81k (**+35%**) |
| 64 conns, **2** streams/conn | **0.99** | 31k → 34k (no-op) |

The win **survives the pool** at realistic settings: poseidon's lazy-grow,
least-loaded pool with a high per-conn stream cap packs streams densely onto few
connections, so per-conn concurrency — and therefore `k` — stays high as the
conn cap rises. It only dilutes to a no-op (`k≈1`) when the pool is forced to
spread thin (low `MaxStreamsPerConn`), and even then it is not a regression.

## 4. Verdict and honest caveats

**GO — shipped opt-in.** Passes the pre-registered kill criteria (`k > 1.3` and
per-core req/s rises at saturation) at every realistic pooled config, and is a
strict no-op below it.

Caveats that bound the claim:

1. **Loopback confound.** `req/s` is measured against an in-process server that
   shares the 2 cores, so it is a *lower* bound — the win rose req/s **despite**
   the shared-core tax. `k` is server-independent and clean. A CPU-isolated sink
   would likely show a larger req/s gain.
2. **Needs per-conn concurrency.** Batching only coalesces co-queued frames, so
   it helps workloads that drive many streams per connection (§3 shows
   poseidon's dense-multiplexing pool preserves this; forced thin spread
   no-ops). Off by default for exactly this reason — callers opt in when their
   workload fits.
3. **HEADERS-only.** The DATA/upload path is untouched; batching covers GETs.
4. **Latency fidelity.** A convoyed frame is briefly held for a co-queued peer.
   Because a lone request is never delayed (adaptive) the effect is bounded, but
   latency-fidelity-critical measurements should leave `GroupCommit` off.

## 5. Tests

- `conn/groupcommit_test.go`:
  - `TestGroupCommit_ConcurrentGETs_NoDeadlock` — liveness + correctness under
    concurrent open-loop GETs (all 204s, completes in-deadline).
  - `TestGroupCommit_WriteError_SurfacesNoHang` — a fault-injecting transport
    trips a write failure mid-flight; asserts no writer hangs on a failed convoy
    flush and the error surfaces (the synchronous error-path guard).
- Both pass under `-race`. The full suite is unchanged with `GroupCommit` off
  (byte-identical path).

## 6. Files

- `conn/options.go` — `ConnOptions.GroupCommit` (opt-in, documented).
- `conn/conn.go` — `groupCommit`/`writeWaiters`/`flushSeq`/`flushErr`/
  `flushCond`/`flushDeferring` + `commitFrame`; `Close` broadcasts `flushCond`.
- `conn/groupcommit_test.go` — correctness/liveness tests.
- `conn/groupcommit_bench_test.go`, `client/groupcommit_pool_bench_test.go` —
  the measurement harnesses.
