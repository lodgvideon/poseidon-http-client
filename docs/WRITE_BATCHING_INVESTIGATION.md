# Write-Batching Investigation (group-commit under `wmu`)

**Status:** experiment — measured **GO** at high single-conn concurrency.
Prototype landed behind an unexported, off-by-default flag; **not productized.**
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

## 2. Prototype: group-commit under the existing `wmu`

No async writer, no queue, no timer — one policy change under the existing lock
(`conn/conn.go`):

- Count writers queued on `wmu` (`writeWaiters`, incremented right before
  `Lock`, decremented right after acquiring).
- In `writeHeadersWithPriority`, after buffering the frame, **skip the flush**
  when `groupCommit && writeWaiters > 0` (`maybeGroupFlush`): a queued writer
  will append its frame and flush the convoy in one `tls.Conn.Write`. The frame
  always reaches the wire when the waiter burst drains (the last holder flushes)
  or when `bufio` auto-flushes at `writeBufferSize`.
- Invariants I1–I7 from the write-queue doc are **untouched** — `wmu` still
  serializes everything; stream-ID/HPACK order is unchanged.
- **Adaptive:** no waiter → flush immediately → byte-identical to v0.7.1. Worst
  case off the co-queuing path is one atomic load per write. Unlike framer's
  1 ms timer, a lone request is **never** delayed (it even leaves earlier — it
  skips its own lock-acquire-then-flush turn).

Scope of the prototype: only the HEADERS terminal flush is deferred. The
flush-before-flow-control-block in `writeData` stays **unconditional** (skipping
it would deadlock). So the prototype batches the HEADERS (GET) path only — the
load-generator common case.

## 3. Measurement

`conn/groupcommit_bench_test.go`: one TLS `h2` connection to an in-process
`net/http2` 204 server, transport wrapped in a `flushCountConn` that counts
`tls.Conn.Write` calls (the honest syscall proxy). `W` open-loop worker
goroutines issue empty GETs. `k = framesSent / transportWrites` = mean
frames per flush; `k > 1` means batching happened. `GOMAXPROCS=2`,
`-benchtime=30000x`, `-count=3`, AMD Ryzen 7 7700.

| Workers | k (off→on) | flush/req (on) | ns/op (off→on) | req/s (off→on) |
|---|---|---|---|---|
| **1**  | 1.00 → **1.00** | 1.00 | 62.6k → 61.3k | 16.0k → 16.3k |
| **8**  | 1.00 → **1.27** | 0.79 | 16.4k → 15.0k | 61.0k → 66.6k (+9%) |
| **64** | 1.00 → **1.73** | 0.58 | 13.2k → 10.5k | 76.1k → 95.7k (**+26%**) |

- `k` rises monotonically with per-conn concurrency (1.00 → 1.27 → 1.73) — more
  co-queued writers → bigger convoys → fewer flushes.
- **W1 is a byte-identical no-op** (k=1.00, same ns/op) — zero cost, zero
  latency penalty when there is no contention.
- Race-clean: `-race` on the ON path shows no data race (k=1.85 under `-race`).

## 4. Verdict and honest caveats

**GO at high single-conn concurrency** — passes the pre-registered kill criteria
(`k > 1.3` **and** per-core req/s rises at saturation) at W64 (k=1.73, +26%),
and is a strict no-op below it.

Caveats that bound the claim:

1. **Loopback confound.** `req/s` is measured against an in-process server that
   shares the 2 cores, so it is a *lower* bound — the write-path win rose req/s
   **despite** the shared-core tax. `k` is server-independent and clean. A
   clean number needs a CPU-isolated sink (separate process/machine); expect a
   larger req/s gain there.
2. **Pool dilution (the real limiter).** This is a *single-connection* result.
   A pool spreads load across N conns → lower per-conn concurrency → `k` toward
   1. Group-commit helps workloads that drive many streams per connection
   (e.g. exercising server multiplexing); a thin pool-spread dilutes it. Not
   swept here — the productization gate must sweep pool size × per-conn
   concurrency and report where `k > 1.3` actually materializes on the shipped
   pooled config.
3. **HEADERS-only.** The DATA/upload path is untouched (its flush-before-block
   must stay unconditional). Group-commit as built batches GETs.
4. **Prototype error semantics are optimistic.** A skipper returns `nil` without
   waiting for the deferred flush, so a later flush error is not attributed to
   it. Productization needs the **synchronous group-commit** (skipper blocks
   until its convoy's flush completes and receives that error) to preserve
   per-frame write-error semantics — a small cost that may trim the ceiling.

## 5. Productization requirements (if pursued)

1. Synchronous group-commit (per-frame error propagation to skippers), so no
   public async-error contract change and no wire-byte-assertion test sweep.
2. Extend the waiter count to **all** `wmu` acquirers, and evaluate deferring
   the DATA terminal flush (keeping the pre-block flush unconditional).
3. An **opt-in** knob (default off / adaptive), documented as a
   throughput-vs-latency-fidelity choice for load-gen callers.
4. The pool × per-conn-concurrency sweep from §4.2 on the shipped config, with
   the same `k > 1.3` + req/s-at-saturation kill criteria; revert if it does not
   reproduce off loopback.

## 6. Files

- `conn/conn.go` — `groupCommit`/`writeWaiters` fields + `maybeGroupFlush`
  (unexported, off by default).
- `conn/groupcommit_bench_test.go` — the measurement harness.
