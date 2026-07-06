# Write-Queue Investigation (decouple socket write from `wmu`)

**Status:** investigation / design only — NOT implemented, NOT committed.
**Trigger:** profiling of `BenchmarkConn_Roundtrip_Concurrent` (16 streams, one conn).

## 1. Problem (from data)

Mutex profile: `wmu` is **96% of all lock-contention delay**, **100% of it inside
`writeHeadersWithPriority`**. CPU profile: `runtime.cgocall` (the socket
syscall) = **42%** of CPU; our codec barely registers. Both `writeHeadersWithPriority`
and `writeData` hold `wmu` **across the socket `Write`** (see `writeHeaderBlock`
→ `fr.WriteHeaders`, and `writeData` → `fr.WriteData`). So every stream's write
serializes behind one in-flight syscall.

**The lock hold is dominated by the syscall, not by our code.** If the syscall
leaves the critical section, the per-write `wmu`-equivalent hold drops from
microseconds to tens of nanoseconds (HPACK encode ~46 ns + an enqueue).

**Caveat (the go/no-go):** at 16 streams over loopback TLS, throughput still
scaled **4.6×** vs sequential (12.3 µs vs 57 µs/op) — i.e. **RTT, not `wmu`,
binds at that concurrency**. The contention delay is real but currently *hidden*
by pipelining. The write-queue pays off only where `wmu` actually binds:
low-RTT links + high single-connection stream concurrency. See §6 measurement.

## 2. Proposed architecture

One dedicated **writer goroutine** owns the socket. Producers format a frame and
**enqueue** it; the writer drains a FIFO and writes. The syscall leaves the
per-request critical path.

```
SendHeaders ─┐                       ┌─ writer goroutine (sole socket owner)
SendData    ─┼─ format + enqueue ──▶ │  for f := range q { transport.Write(f) }
control     ─┘   (FIFO, bounded)     └─ on error: mark conn failed, broadcast
```

Two ordering regimes, because the two frame classes differ:

- **HEADERS (HPACK-stateful):** `EncodeBlock` mutates the shared dynamic table,
  and the decoder replays in wire order, so **encode-order must equal
  wire-order**. Hold a short `encMu` across **`{EncodeBlock; enqueue}`** so
  encode-order == enqueue-order == (FIFO) write-order. The whole block
  (HEADERS + any CONTINUATION) is **one queue item** to preserve §6.10
  contiguity.
- **DATA / WINDOW_UPDATE / RST / PING / SETTINGS / GOAWAY (stateless framing):**
  no shared encoder state; only frame-atomic wire ordering matters. A
  thread-safe enqueue suffices (no `encMu`).

The producer's hold shrinks from "syscall" to "encode + enqueue". `wmu` as a
syscall-spanning lock is **removed**; it is replaced by `encMu` (HEADERS only,
~tens of ns) plus a lock-free/bounded queue.

## 3. Correctness invariants (must all hold)

| # | Invariant (RFC 7540/7541) | How the design preserves it |
|---|---|---|
| I1 | HPACK encode-order == wire-order | `{EncodeBlock; enqueue}` atomic under `encMu`; FIFO queue |
| I2 | HEADERS+CONTINUATION contiguous, no interleave (§6.10) | whole block enqueued as one item; writer emits it contiguously |
| I3 | Frames never torn on the wire | each queue item is a fully-formatted frame buffer; writer writes whole items |
| I4 | DATA respects send flow control (§6.9) | producer calls `acquireSendCredits` **before** enqueue (unchanged); credit debited at enqueue time |
| I5 | Monotonic stream IDs == wire order (§5.1.1) | stream-ID assignment must stay atomic with HEADERS enqueue — fold ID-assign into the `encMu` section (it is currently inside `wmu`) |
| I6 | Connection close sends GOAWAY, then stops | `Close` enqueues GOAWAY, signals drain-then-exit, joins writer |
| I7 | Write errors are observable | **CONTRACT CHANGE — see §4** |

`★ I5 is the subtle one:` stream IDs are assigned lazily on first HEADERS write
(B.2.1 deferred allocation) *under `wmu`* today, precisely so ID order == wire
order across concurrent `NewStream` callers. The write-queue must assign the ID
inside the same `encMu`-guarded section that enqueues the HEADERS, or two
concurrent openers could enqueue out of ID order → `PROTOCOL_ERROR` from the peer.

## 4. Contract change (the real cost)

Today `SendHeaders` / `SendData` return the **socket write error synchronously**
for *that* frame. With an async writer, enqueue returns `nil` (or `ErrConnClosed`
if the conn is already failed); a write error surfaces **later** — the writer
marks the conn failed, broadcasts, and subsequent enqueues + `Recv` return the
error. Per-frame error attribution is lost.

For HTTP/2 this is defensible (a transport write error is **connection-fatal** —
you tear down the whole conn regardless of which frame tripped it). But it **is**
a public behavioral change and must be documented; callers relying on "Send
returned the write error" semantics would need to consult conn state instead.

## 5. Test-migration impact (the other real cost)

Many conn unit tests assert **synchronous** write output: wire `c.fr` to a
`bytes.Buffer` (or `net.Pipe`) and assert produced bytes **immediately** after a
write call (CLAUDE.md "wire-byte assertions on single Conn method"). With an
async writer the bytes appear after a goroutine hop, so every such test needs a
**flush/sync barrier**. The `net.Pipe` "unbuffered + synchronous → deadlock if
peer writes >1 frame" hazard also shifts (decoupling may *fix* some, break
others). Expect a broad test sweep — see §6 census.

## 6. Decision criteria & next steps (no code yet)

This change is **high-risk, high-churn, conditional-benefit** — exactly the kind
Karpathy says not to build on speculation. Gate it on evidence:

1. **Go/no-go measurement — DONE, result: NO-GO (RTT-bound).** Concurrency sweep
   of `BenchmarkConn_Roundtrip_Concurrent` (`-cpu=1,2,4,8,16`, loopback TLS):

   | GOMAXPROCS | 1 | 2 | 4 | 8 | 16 |
   |---|---|---|---|---|---|
   | ns/op | 27240 | 24994 | 22253 | 16429 | 13961 |

   ns/op **decreases monotonically** — it does not flatten. If `wmu` bound
   throughput, added goroutines would queue on the lock and ns/op would plateau.
   It keeps dropping → **RTT-bound**; concurrency hides the latency and the
   `wmu` hold (encode + syscall) is short relative to the per-request Recv-wait.
   The mutex profile's 19 s cumulative contention is real but **not** the
   throughput ceiling at realistic concurrency. A still-cheaper in-process
   transport variant could find the eventual binding point, but on any workload
   with real network RTT the write path is not the limiter.
2. **Test census.** Count conn tests that assume synchronous writes → migration size.
3. **Prototype on a branch only if (1) is positive** — measure real ns/op + alloc
   delta against this baseline before proposing a merge.

**Lower-risk partial alternative:** cross-frame write coalescing (batch several
queued frames into one `writev`/`Write` under the existing lock). Captures part
of the syscall-amortization win without the async-error contract change, but
still needs a queue, so it converges toward this design anyway.

**Current recommendation:** the connection pool already gives N independent
`wmu`s (N conns). Unless the measurement shows `wmu` binding in a regime the pool
can't cover, scaling out connections is the cheaper lever than this refactor.
