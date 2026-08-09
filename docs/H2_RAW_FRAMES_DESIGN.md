# HTTP/2 raw-frames mode — research + design

Status: **design draft + R0 measured**. Branch `claude/h2-raw-frames-mode-ff3b90`.
R0 ran and **refuted two of the three proposed phases** — read §7a before §7.

Goal, restated: *not* to write a load generator, but to make poseidon's HTTP/2
stack a viable foundation for one of the [ozontech/framer](https://github.com/ozontech/framer)
class — a generator that owns its own request wire bytes, batches frames across
streams into one write, and never materializes a response body.

---

## 1. What framer actually is (source-level)

framer is a gRPC load generator with a from-scratch HTTP/2 client. Its README
claims the top spot against pandora / ghz / k6 / h2load / gatling and attributes
it to four things: own h2+gRPC client, zero-allocation, minimized syscalls,
minimized copying. (The benchmark chart is a PNG; the numbers are not
machine-readable, so treat the ranking as unverified.)

The architecture in one sentence:

> **The request object owns its own frames; the connection owns only the
> connection.**

### 1.1 The send path

`loader/types/req.go` is the whole idea:

```go
type Req interface {
    SetUp(maxFramePayloadLen, maxHeaderListSize int,
          streamID uint32, fieldWriter HPackFieldWriter) ([]Frame, error)
    FullMethodName() string
    Tag() string
    Size() int
    Release()
}

type Frame struct {
    Chunks           [3][]byte   // {9-byte frame header, [gRPC prefix], payload}
    FlowControlPrice uint32
}
```

`datasource.RequestAdapter.SetUp` (in `datasource/request.go`) writes the
HPACK block into a reusable `bytes.Buffer`, slices it into
HEADERS+CONTINUATION frames, then builds the DATA frames — every 9-byte frame
header comes from a per-adapter `frameHeaders` bump arena, and the message body
is **referenced, never copied** (`b := pl[:bLen]`). The gRPC 5-byte length
prefix is a `[5]byte` field on the adapter.

`loader/sender/sender.go` then:

1. `s.streamID += 2` — monotonic client IDs, no map, no lock.
2. `streams.Limiter.WaitAllow()` — blocking MAX_CONCURRENT_STREAMS gate.
3. `streams.Pool.Acquire(streamID, tag)` — pooled stream struct.
4. `req.SetUp(...)` — build the frames.
5. per frame: `stream.FC().Wait(price)` + `fcConn.Wait(price)` — cond-var flow control.
6. `s.frameChan <- frame`.

A separate `batchedProcessor` goroutine drains `frameChan`, appends every chunk
into a `net.Buffers` (up to 2048 entries or a `SendBatchTimeout`), and a third
goroutine does `bufs.WriteTo(conn)` — **one `writev(2)` for many frames from
many streams**. Priority frames (PING ACK, WINDOW_UPDATE) jump the queue via a
separate channel and are written first.

Note what this buys and what it costs: it is a *plaintext* optimization.
`net.Buffers.WriteTo` only vectorizes when the writer is a `*net.TCPConn` /
`*net.UnixConn`; through `*tls.Conn` it degrades to one `Write` — and therefore
one TLS record — per buffer. framer's benchmarks are h2c.

### 1.2 The receive path

`loader/reciever/processor.go` is the mirror image of the same idea:

- A resumable framer yields `(header, payloadChunk, incomplete)` so a frame
  spanning two socket reads is processed in pieces, never buffered whole.
- `dataFrameProcessor.Process` **ignores the payload entirely**. It accumulates
  `header.Length()` and, past `65535/4`, emits a pre-built 13-byte
  WINDOW_UPDATE frame from a two-slot ping-pong buffer. The response body is
  never seen by anything.
- `headersFrameProcessor` streams fields straight out of the HPACK decoder into
  `stream.OnHeader(name, value)` — no `[]HeaderField`, no slab, no retention.
- END_STREAM / RST_STREAM / timeout all funnel into `stream.End()`, which
  reports and returns the stream to the pool.

`loader/streams/pool` recycles stream structs; `loader/timeout_queue.go` is a
slice queue that works only because every request shares one timeout, so
insertion order *is* deadline order — no heap, no per-request timer.

### 1.3 What framer gives up

- **No response body.** Ever.
- **No runtime SETTINGS.** `settingsProcessor` returns
  `errors.New("update settings in runtime not supported")` on a non-ACK SETTINGS.
- No PUSH_PROMISE, no PRIORITY, no ORIGIN/ALTSVC, no CONTINUATION-continuity
  enforcement, no content-length validation, no 1xx handling.
- HPACK is still re-encoded per request (it keeps the dynamic table), so it has
  **no** precoded-block win — only precoded *buffers*.

It is a measurement instrument, not a client. That distinction is what makes
"support writing one" a different task from "become one".

---

## 2. Gap analysis: what poseidon already has

| framer needs | poseidon today | gap |
|---|---|---|
| preface + SETTINGS exchange | `conn.NewClientConn` | — |
| monotonic stream IDs | `Conn.nextID`, assigned at first HEADERS | — |
| MAX_CONCURRENT_STREAMS gate | `NewStream` → `ErrTooManyStreams` | non-blocking only; framer blocks |
| conn + per-stream send flow control | `acquireSendCredits` (private) | not reachable from a caller-built frame |
| conn + per-stream recv flow control, batched WINDOW_UPDATE | `recvWindowRefundThreshold` = 32 KiB | — |
| GOAWAY drain, PING echo, RST | `conn/handler.go` | — |
| pooled stream structs | `Conn.streamPool` | — |
| build frames from caller-owned bytes | ❌ only `SendHeaders([]HeaderField)` / `SendData([]byte)` | **gap 1** |
| N frames from N streams in one transport write | ❌ per-frame `flushWrite`; `GroupCommit` batches HEADERS only, under contention | **gap 2** |
| response headers as a callback | ❌ decoded to `[]HeaderField` on a pooled slab, pushed to a channel | **gap 3** |
| response body discarded, counted for FC only | ❌ copied to a pooled slab, pushed to a channel | **gap 3** |
| completion without a goroutine per request | ❌ `Stream.Recv(ctx)` is a blocking channel read | **gap 4** |
| one timeout for all requests, no timer each | ❌ per-request `context` | outside conn — generator's job |
| adversarial / malformed frames | ❌ writers validate (stream 0, oversize, …) | **gap 5** (separate feature) |

Two of the five are structural (`1`, `3`); `2` and `4` follow from them; `5` is
an unrelated capability that keeps getting conflated with this one.

Everything else — the ~5,100 lines of `conn/` that implement the RFC — is
already there and is exactly what a generator author does not want to rewrite.

---

## 3. Where the wins actually are

Being precise here matters, because two of the four "obvious" wins are not real.

**Real: batching frames into one transport write.** Today `writeData` flushes
per DATA frame and `writeHeadersWithPriority` per HEADERS. One TLS record and
one `write(2)` per frame. Coalescing K frames into one write is a win under TLS
*and* plaintext — under TLS by cutting record count, under plaintext
additionally by vectoring. This is the single largest available saving at high
concurrency, and it is orthogonal to raw frames: it needs a *batch submission
point*, which is what raw mode provides.

**Real: skipping the receive event channel and slab copies.** For a generator
that wants status + timings, every DATA frame today costs a copy into a pooled
slab plus a channel send plus a channel receive plus a slab return. Discarding
in the handler costs an integer add. Note [[recv-alloc-nogo]] says do not chase
receive *allocations* — this is not that; it is skipping *work*, on a path where
the payload is provably unwanted.

**Real, and better than framer: precoded static header blocks.** framer
re-encodes HPACK per request. If the caller accepts a **stateless** encoding
(every field `IndexWithout`/`IndexNever`, no dynamic-table size update) the
encoded block is a pure function of the field list, so it can be built once and
reused on every stream and every connection — only the 4-byte stream ID in the
frame header changes. Trade-off is explicit and must be documented: a gRPC
request header set is ~15 bytes with a warm dynamic table vs ~90–120 bytes
static-only. That is CPU traded for wire bytes; at 200k rps it is real
bandwidth. It must be opt-in, never a default.

**Not real: "raw frames avoid the HPACK encode."** Only if the block is
stateless (above). With the dynamic table live, HPACK is one ordered stream of
state per connection — a cached block is invalid the moment the table shifts,
and a caller-supplied block encoded by a *different* encoder corrupts the peer's
table. Any raw-header-block API must state this as a hard precondition.

**Not real: an incremental/resumable frame parser.** framer has one to avoid
ever holding a whole frame. `frame.Framer.ReadFrame` does `io.ReadFull` into a
pooled buffer bounded by `SETTINGS_MAX_FRAME_SIZE`. That is a bounded copy from
a `bufio.Reader`, not a bottleneck. Do not port this.

---

## 4. Architecture options

### Option A — frame port inside `conn` (additive)
`ConnOptions.RawFrames` + `Conn.WriteFrames` + a per-stream sink. Reuses the
whole engine. Cost: a mode branch on hot paths; sink callbacks run on the reader
goroutine.

### Option B — sibling package `h2gen` over `frame` + `hpack`
framer's architecture, literally, next to `conn`. Maximum ceiling, zero risk to
`conn`. Cost: re-implements handshake, SETTINGS + retroactive
`INITIAL_WINDOW_SIZE`, both flow-control directions, GOAWAY, CONTINUATION
continuity — i.e. it manufactures the exact
[[sibling-divergence-bugs]] shape this repo has been bitten by repeatedly
(hpack ring vs qpack; H2 had zero 1xx). Every future `conn` fix silently misses it.

### Option C — extract a shared core
Split `conn` into a core (handshake/settings/FC/goaway) plus two façades:
classic `Conn` and generator `RawConn`. Best end state. Cost: a refactor of the
most heavily tested code in the repo, delivering no user-visible feature on its
own.

### Option D — Option A, plus a hard boundary (**recommended**)
Add the **minimum** additive seams to `conn`, and put everything
load-generator-specific — scheduler, timeout wheel, request-file format,
reporter, frame batching policy — in a *user-space* reference generator under
`contrib/` or `examples/`. The reference generator is the proof the seams are
sufficient; if it needs a private field, the seam set is wrong.

Recommendation is D because the RFC engine stays single-sourced (the thing this
codebase is actually good at, and the thing a generator author most wants to not
own), while the parts that genuinely differ per generator stay outside the
library.

---

## 5. Proposed API (Option D)

Sketch, not final. Roughly three additions, one option, one interface.

### 5.1 Send: batched raw frames

```go
// FrameChunks is one frame as a vector of byte slices: chunk 0 is the 9-byte
// frame header, the rest is payload. Slices are borrowed for the duration of
// the call.
type FrameChunks struct {
    Chunks [][]byte
    // FlowCost is the flow-controlled byte count (DATA payload + padding);
    // zero for non-DATA frames.
    FlowCost uint32
    // Stream is the stream to charge FlowCost to; nil charges the connection
    // window only.
    Stream *Stream
}

// WriteFrames charges flow control for every chunk, then coalesces the whole
// batch into ONE transport write. Blocks until credit is available.
func (c *Conn) WriteFrames(ctx context.Context, batch []FrameChunks) error
```

Implementation: acquire per-stream + connection credit for every `FlowCost`
*before* taking `wmu` (never block on credit under the write lock — the
existing `writeData` deadlock rule), then one pass under `wmu`. Coalescing
strategy is transport-dependent:

- plaintext (`*net.TCPConn`) and total size above the write buffer →
  `net.Buffers(all).WriteTo(transport)` — real `writev`;
- otherwise → append into the buffered writer and flush once, so TLS emits a
  single record.

`writeBufferSize` (currently a 16 KiB `const`) has to become a `ConnOptions`
field for this to be tunable.

### 5.2 Send: stateless precoded header blocks

```go
// StaticHeaderBlock encodes fields with no dynamic-table side effects, so the
// result is a pure function of fields and may be cached and reused across
// streams and connections. Larger on the wire than a dynamic-table encoding.
func StaticHeaderBlock(dst []byte, fields []HeaderField) []byte

// FrameHeader is a mutable 9-byte view over a caller-owned buffer, so a cached
// request can be re-stamped with a new stream ID instead of rebuilt.
type RawFrameHeader []byte
func (h RawFrameHeader) SetStreamID(uint32)
func (h RawFrameHeader) SetLength(int)
func (h RawFrameHeader) SetFlags(frame.Flags)
```

(`frame` already has `WriteFrameHeader`; this is the mutable-view sibling —
framer's `frameheader.FrameHeader`.)

Hard precondition, documented and enforced by construction: blocks handed to
`WriteFrames` MUST be stateless. A `ConnOptions.ValidateRawHeaderBlocks` dev
switch can decode-check them.

### 5.3 Receive: sink mode

```go
// StreamSink receives a stream's events inline on the connection reader
// goroutine, instead of via Stream.Recv.
//
// CONTRACT: every method MUST return promptly and MUST NOT block on the wire.
// A sink that blocks stalls the whole connection — all streams. Hand off to
// your own send loop; do not call WriteFrames from a sink.
type StreamSink interface {
    OnHeaderField(name, value []byte)   // borrowed; copy to retain
    OnHeadersEnd(endStream bool)
    OnData(n int, payload []byte)       // payload nil when DiscardBody is set
    OnEnd(reason EndReason, code ErrCode)
}

func (c *Conn) NewStreamSink(sink StreamSink) (*Stream, error)
```

and

```go
type ConnOptions struct {
    // ...
    // DiscardResponseBody accounts DATA for flow control and drops the payload
    // without copying it into a slab or delivering it. Sink mode only.
    DiscardResponseBody bool
}
```

Sink mode makes `Stream.events` unnecessary; allocate the channel lazily so a
sink stream costs no channel at all.

### 5.4 What deliberately stays outside

Scheduler, RPS shaping, request-file formats, reporter/phout, the timeout wheel,
batch-size and batch-timeout policy, gRPC message framing for the generator.
All of it goes in the reference generator, not in `conn`.

---

## 6. Repo-specific hazards

1. **`recycleStream` reset.** A sink pointer is a new per-stream field; it MUST
   be cleared in `recycleStream` or it leaks into the next request on a reused
   connection. Needs a REUSE test, not just a fresh-conn test.
2. **Issue #370** (stale `*Stream` after re-allocation) is exactly the shape a
   generator's own stream store produces. Mitigation: the sink is the caller's
   own object; never hand a generator a `*Stream` it is expected to key a map
   on. Consider an opaque cookie instead.
3. **Sink reentrancy.** A sink calling `WriteFrames` takes `wmu` on the reader
   goroutine and can block on the socket. Documented as forbidden; consider a
   debug-mode detector.
4. **GroupCommit interaction.** `GroupCommit` is opt-in and, per closed issue
   #360, extending it to DATA made p50 +81%. `WriteFrames` is a *different*
   mechanism (explicit caller-side batching, no waiting) and must not be routed
   through `writeBatcher`. Do not re-litigate #360.
5. **`writeBDPPing` silent flush** already violates the group-commit invariant;
   adding another flush site makes it worse. Audit flush sites when `WriteFrames`
   lands.
6. **Bench gate is absolute.** `frame` and `hpack` benches are gated at
   0 B/op, 0 allocs/op. Anything new on those paths must hold.
7. **`net.Buffers` on Windows** uses `WSASend`; the fast path must be
   feature-detected, not `GOOS`-gated, and must fall back cleanly.

---

## 7. Phasing, with measurement gates

Every phase gated on a number, because unmeasured coalescing wins have already
turned out to be illusions in this repo ([[quic-perf-plan]]: ACK+STREAM
coalescing looked like a win only against a reader-less bench).

**R0 — measure first (no code).** Build a load-generator-shaped bench: h2c,
N concurrent streams, small unary request, response discarded. Attribute
per-request cost across: HPACK encode, transport writes, event channel + slab
copies, goroutine scheduling. This produces the go/no-go for R2 and R3 and the
baseline every later claim is measured against.

**R1 — `Conn.WriteFrames` + configurable write buffer.** Gate: writes per
request under batch size K, and p50/p99 latency not regressed at K=1.

**R2 — sink mode + `DiscardResponseBody`.** Gate: R0 says the channel+slab path
is a meaningful share of per-request cost. If it is under ~5%, stop here and say so.

**R3 — `StaticHeaderBlock` + `RawFrameHeader`.** Gate: measured encode saving vs
measured extra wire bytes, both reported.

**R4 — reference generator** in `examples/` or `contrib/`, framer-shaped:
request source → precoded frames → batched submit → sink reporting. This is the
acceptance test for the whole seam set.

**R5 (separate feature, optional) — non-conformant frame emission** for testing
your own servers against malformed input: bypass the writer-side validation
behind an explicit option. Legitimate (h2spec and h2load do exactly this) but it
is conformance tooling, not load generation, and should not be bundled into the
raw-frames option.

---

## 7a. R0 results — measured, and what they kill

Harness: [`conn/bench_loadgen_test.go`](../conn/bench_loadgen_test.go). Plaintext
h2c, one connection, N concurrent streams, 64-byte request, response drained and
discarded, zero-alloc `frame.Framer` peer. Run on Windows and, for the numbers
quoted here, Linux (WSL, `go1.25.0 linux/amd64`, Ryzen 7 7700, 16 threads).

```
go test ./conn/ -run=NONE -bench=BenchmarkLoadGen_ -benchmem -benchtime=20000x
```

### Macro (Linux)

| bench | writes/req | wirebytes/req | ns/op | allocs/op |
|---|---|---|---|---|
| `Unary_C1` | 2.002 | 89 | 93241 | 2 |
| `Unary_C16` | 2.002 | 89 | 40258 | 2 |
| `Unary_C64` | 2.002 | 89 | 37589 | 2 |
| `Unary_C256` | 2.002 | 89 | 33026 | 2 |
| `HeadersOnly_C64` | 1.000 | 16 | 22083 | 2 |
| `Resp1KB_C64` | 2.031 | 89 | 25791 | 2 |
| `Unary_C64` **+GroupCommit** | **1.307** | 89 | 25532 | 2 |
| `HeadersOnly_C64` **+GroupCommit** | **0.125** | 16 | 7684 | 2 |

### CPU attribution (Linux, `Unary_C64`, whole test binary incl. peer)

| bucket | cum% |
|---|---|
| syscalls (`internal/runtime/syscall.Syscall6`, flat) | **61.7%** |
| — of which socket writes (`bufio.Flush` → `syscall.write`) | 44.2% |
| scheduler (`runtime.schedule` via `mcall`/`park_m`) | 22.7% |
| **client receive path** (`OnHeaders`+`OnData`+`Recv`+`deliverEnd`+validation) | **4.0%** |
| **HPACK encode** (client *and* peer) | **1.4%** |
| — `hpack.staticIndex` 61-entry linear scan | 0.8% |

### Verdicts

**R1 (batch frames into fewer transport writes) — CONFIRMED, and it is the only
lever that matters.** 44% of CPU is socket writes. The existing `GroupCommit`
option, which batches *HEADERS only* and *only under wmu contention*, already
takes headers-only traffic from 1.000 to **0.125 writes/req** — an 8x reduction,
−65% ns/op. Unary only reaches 1.307 because every DATA frame still forces its
own flush (`writeData` flushes per frame by design, to avoid the credit
deadlock). That residual ~1.3 writes/req is precisely what an explicit,
caller-driven batch removes.

This does **not** overturn closed issue #360, which measured group-commit's
*waiting* mechanism hurting mixed DATA+HEADERS p99/p50. Different regime (h2c
throughput vs TLS latency) and, decisively, a different mechanism: R1 batches
frames the caller has *already* handed over, and waits for nothing.

**R2 (sink mode / `DiscardResponseBody`) — REFUTED at its own gate.** The gate
was "meaningful share of per-request cost, stop if under ~5%". The entire client
receive path is **4.0%**, and the DATA-specific part (`OnData` + `deliverEnd`)
is 1.4%. `Resp1KB_C64` is not measurably worse than `Unary_C64` with a 64-byte
response. The isolated per-DATA-frame event hop (pooled slab checkout + copy +
channel send + receive + slab return) is **37–52 ns**. Discarding response
bodies buys about 1%.

The one surviving argument for a sink is removing the goroutine per request, and
the scheduler bucket is 22.7% — but most of that is park/unpark around blocking
socket syscalls, which a sink does not remove either. **Do not build R2 on
performance grounds.** If it is ever built, it must be justified as an
*ergonomics* feature (event-driven completion without a goroutine per request),
and measured separately.

**R3 (static precoded header blocks) — REFUTED.** The premise was that skipping
the HPACK encode is worth the bigger blocks. Measured: a warm dynamic-table
encode of the 7-field gRPC request set is **7 bytes** and 444 ns; the stateless
equivalent is **84 bytes**. That is +77 bytes/request — a 12x header inflation —
to save 1.4% of CPU (and that 1.4% includes the peer's own encodes, so the
client's share is smaller still). At 200k rps it trades ~15 MB/s of bandwidth
for a rounding error. framer was right to keep the dynamic table.

`hpack.staticIndex` at 0.8% is the perfect-hash item already in the perf backlog.
It is real but it is not this project.

### Caveats on the above

- `ns/op` from this harness is not a latency claim — a real socket is in the
  loop. `writes/req` is deterministic and is the metric the verdicts rest on.
- The profile covers the whole test binary, peer included. That biases *against*
  R2 and R3 being refuted... no: it inflates their apparent cost, so the
  refutations are conservative.
- One machine, loopback, h2c. A real network changes the syscall/latency ratio
  but cannot make writes cheaper, so R1's direction is safe. A TLS run would add
  record framing on top of every write, which strengthens R1 further.
- Incidental finding, worth its own note: **`conn` hands pooled buffers to the
  caller via `StreamEvent.Slab` / `DataSlab` and never reclaims them.** A caller
  that does not return them pays a fresh 16 KiB allocation per DATA frame — the
  first version of this harness did exactly that and reported 17153 B/op instead
  of 2 allocs/op. Any generator written straight against `conn` will hit this.
  It is an argument for a sink API on *ergonomic* grounds, and it belongs in the
  package docs regardless.

### Revised plan

1. **R1 only.** Explicit caller-driven frame batching that never waits. Two
   sub-parts, in order of measured payoff:
   a. one-shot send — HEADERS+DATA+END_STREAM in one flush (2.002 → ~1.0
      writes/req before any cross-stream batching); this is backlog item 1 and
      needs no new API.
   b. `Conn.WriteFrames` for cross-stream batching (the 0.125 writes/req that
      GroupCommit demonstrates is reachable).
2. **R4 reference generator**, to prove the seam set. Unchanged.
3. R2 and R3 are **shelved with their numbers recorded here**. Do not re-propose
   without new evidence.

## 8. Explicit non-goals

- Do not become a load generator. No scheduler, no report formats, no CLI.
- Do not port framer's incremental frame parser (§3).
- Do not change any existing default. `Stream.Recv` stays the primary API.
- Do not re-open async-writer-goroutine or writev-through-TLS
  ([[write-batching-go]]) — `WriteFrames` is caller-driven batching, a
  different mechanism, and the TLS path stays "one buffered write, one record".
