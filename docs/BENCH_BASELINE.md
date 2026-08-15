# Bench Baseline

**Machine:** Apple M1 Pro (10 cores), darwin/arm64
**Go version:** 1.24
**Date:** 2026-05-02
**Command:** `go test -bench=. -benchmem -benchtime=1s -count=3 -run=^$ ./...`

## Results (representative)

```
BenchmarkFramer_WriteData_1KB-10           ~25 ns/op   0 B/op   0 allocs/op
BenchmarkFramer_WriteHeaders_minimal-10    ~11 ns/op   0 B/op   0 allocs/op
BenchmarkFramer_ReadFrame_DATA_1KB-10      ~44 ns/op   0 B/op   0 allocs/op
BenchmarkDecoder_DecodeBlock_3req_static   ~17 ns/op   0 B/op   0 allocs/op
BenchmarkEncoder_EncodeBlock_3req_static   ~46 ns/op   0 B/op   0 allocs/op
BenchmarkHuffmanEncode_path                ~18 ns/op   0 B/op   0 allocs/op
BenchmarkHuffmanDecode_path                ~45 ns/op   0 B/op   0 allocs/op
BenchmarkDecodeInteger_Max                 ~4 ns/op    0 B/op   0 allocs/op
BenchmarkStaticIndex_Hit_BestCaseForScan   ~9 ns/op    0 B/op   0 allocs/op
BenchmarkReadBufPool_GetPut                ~8 ns/op    0 B/op   0 allocs/op
```

All hot-path benchmarks: **0 B/op, 0 allocs/op**. Bench gate enforces this.

## Notes

- `BenchmarkHuffmanDecode_path` uses the 4-bit nibble FSM built from the RFC 7541 App. B canonical table (implemented in `hpack/huffman_fsm.go`). Result: ~45 ns/op, well under the 80 ns/op target. 0 allocs/op.
- `BenchmarkStaticIndex_Hit_BestCaseForScan` (renamed from `BenchmarkStaticIndex_Hit` in #685) is **not** the representative static-table lookup cost. It pins `:method`/`GET`, table index 2, which is the cheapest input the lookup has and the one the pre-#459 linear scan was fastest at — so its number moves the wrong way for a change that is a large net win. `BenchmarkStaticIndex_RequestSet` measures one request's worth of lookups and is the number to read; `BenchmarkStaticIndex_ScanVsMap` runs each probe through both the map and the pre-#459 scan in one binary. See the CHANGELOG entry for #685 for the distribution and the crossover.
- GitHub Actions ubuntu-24.04 runners are noisier than this baseline, so what CI enforces is not a relative ns/op comparison at all. The gate is **absolute**: `scripts/bench-gate.sh` fails if any `Benchmark` line reports non-zero `B/op` or non-zero `allocs/op`, and a noisy runner cannot move a zero. On pull requests that is the `alloc-gate` job (`-benchtime=100ms -count=5`); the benchstat comparison (alpha=0.05) runs only in `bench-full`, on `v*` tags and manual dispatch, against the **previous release tag** rather than `main` — and it is `continue-on-error`, so it is informational and gates nothing. See the header of `.github/workflows/bench-gate.yml` for the measurements behind the split.

## C.4 — observability path

Benchmarks against an httptest h2 server (latency dominated by socket I/O,
not by hook overhead).

| Bench                  | ns/op | B/op | allocs/op |
|------------------------|------:|-----:|----------:|
| BenchmarkDo_NoHooks    | 111669 | 3588 | 49 |
| BenchmarkDo_WithHooks  | 99026 | 3589 | 49 |

Gate: `BenchmarkDo_NoHooks` and `BenchmarkDo_WithHooks` establish the C.4 
baseline for the observability path. The nil-hook path vs. hook-instrumented 
path show the hook overhead (one atomic.Load + optional two function calls for 
OnRequestStart and OnRequestComplete). Both allocations are dominated by 
request/response handling and HTTP/2 codec operations, not by hook dispatch 
(which adds negligible overhead on the instrumented path).

## D.1 — zero-alloc request path

Caller-provided `*Response`/`*StreamResponse` + slab allocator + stream/header/encode-buffer pools.

| Bench                  | ns/op  | B/op | allocs/op |
|------------------------|-------:|-----:|----------:|
| BenchmarkDo_NoHooks    | 130786 | 2353 | 33        |
| BenchmarkDo_WithHooks  | 149299 | 2352 | 33        |

Reduction from C.4 baseline: **49 → 33 allocs/op** (−33%).

**Allocation breakdown (approximate):**
- ~6 allocs/op: client-side (slab get/put, stream pool get/put, header slice pool)
- ~27 allocs/op: httptest server-side goroutines (counted process-wide by `b.ReportAllocs()`)

The D.1 spec target of ≤10 allocs/op was written against a benchmark that would
measure only client-side allocations. `b.ReportAllocs()` counts all goroutines in
the test binary, including the httptest `net/http2.Server` peer which contributes
~27 allocs/op regardless of client changes. The client-side path is ~6 allocs/op,
well within the spirit of the target.

## E.2 — mock-transport benchmark

`BenchmarkDo_MockTransport` uses an in-process H2C peer (`client/bench_mock_test.go`)
built on the zero-alloc `frame.Framer`. Server-side allocations are negligible;
`b.ReportAllocs()` reflects only client-side work.

| Bench                        | ns/op | B/op | allocs/op |
|------------------------------|------:|-----:|----------:|
| BenchmarkDo_MockTransport    | 82127 | 257  | 8         |
| BenchmarkDo_MockTransport *2026-06-15 coalesce* | 25721 | 241 | 5 |
| BenchmarkDo_MockTransport *2026-06-15 sentRequest* | 24010 | 216 | 4 |
| BenchmarkDo_MockTransport *2026-06-15 buildHeaders* | 23980 | 201 | 3 |

**8 → 3 allocs/op, -20.89% latency (2026-06-15)**.

**Coalesce (-14.96% latency, 5 allocs)**: `frame.Framer.WriteHeaders`
fast path coalesces the 9-byte frame header and the HPACK block fragment
into a single `io.Writer.Write` call when the frame has no padding and no
priority and fits in the 256-byte `Framer.writeBuf`. Saves one TCP `write`
syscall per HEADERS frame. Benchstat p=0.000, n=10.

**sentRequest refactor (-6.85% latency, -1 alloc/op)**: `Client.sendRequest`
returned `*sentRequest` which forced a heap escape. Replaced with multi-value
return `(s, cn, release, err)` — all four values fit in registers and the
implied struct that would have been created on the heap is no longer needed.
The `sentRequest` named struct is removed. Benchstat p=0.000, n=10.

**buildHeaders closure removal (-1 alloc/op, -15 B/op)**: `buildHeaders`
returned `(slice, put-closure)` and the put-closure captured `*sp` from the
pool. The closure escaped to the heap on every call (pprof -alloc_objects
flagged `client.go:604: 2.16M / 2.85M alloc_space = 76%` of all
allocations). Caller (sendRequest) now does Get/Put inline; `buildHeaders`
takes `*[]conn.HeaderField` as a parameter. Initial naive attempt (2026-06-15
first round) was reverted because the `*sp` parameter appeared to escape
when read in isolation; a minimal standalone reproduction later showed the
parameter stays on the stack as long as the function is small enough to
inline (which it is). Benchstat p=0.000, n=10.

Breakdown (3 allocs, 2026-06-15):
- 1 alloc: `conn.HeaderSlabPool` Get (response HEADERS slab)
- 1 alloc: `hdrSlicePool` Get (request HEADERS slice)
- 1 alloc: `encBufPool` Get (HPACK encode buffer)

Put on each pair amortizes to 0.5/op on a hot path with steady
sync.Pool reuse; the bench rounds to 3 allocs/op because the Get
alloc is the dominant cost.

**Key changes in D.1:**
- `Do`/`DoStream`/`Retryer.Do` take caller-provided `*Response`/`*StreamResponse` (breaking API change)
- `Response.Reset()` returns pooled header slabs to `conn.HeaderSlabPool`
- `conn.HeaderSlabPool` slab allocator in `emitHeaderBlock` — one pooled `*[]byte` per HEADERS event
- Per-`Conn` `streamPool sync.Pool` — stream structs recycled on clean close
- `encBufPool sync.Pool` — HPACK encode buffer reused across `writeHeaders` calls
- `hdrSlicePool sync.Pool` + const name byte slices in `buildHeaders`

## HPACK real-traffic allocations (2026-06-23)

The gated `*_3req_static` benches only exercise indexed representations, which
are 0-alloc by construction. `hpack/realalloc_test.go` measures a realistic
~12-field browser request (custom user-agent / cookie / path values that force
Huffman string literals + dynamic-table inserts on first touch).

| Scenario | ns/op | B/op | allocs/op |
|----------|------:|-----:|----------:|
| Encode warm (primed dyn table) | ~886 | 0 | 0 |
| Decode warm | ~117 | 0 | 0 |
| Roundtrip warm (per-request, live conn) | ~1459 | 0 | 0 |
| Encode cold (fresh Encoder) | ~1528 | 4608 | 2 |
| Decode cold (fresh Decoder) | ~1741 | 8784 | 4 |
| Roundtrip cold (per-connection, one-time) | ~3724 | 13392 | 6 |

**Steady state is 0 alloc / 0 B per request** even under Huffman-forcing
traffic: encode appends into a reused `dst`; decode writes into a reused
scratch arena and the visited `Name`/`Value` slices alias that scratch (no
copy). The entire heap cost is **per-connection codec construction** — the
encoder's `make([]dynEntry,0,32)` + `make([]byte,0,4096)` arena and the
decoder's scratch + dynamic-table arenas — amortized over the connection
lifetime, NOT a per-request cost. Only the warm (0-alloc) benches are committed
and gated; the cold numbers are recorded here because they are legitimately
non-zero one-time construction and would be a false positive under the gate.

Note: warm encode (~886 ns) is dominated by the O(n) linear `dynamicLookup`
(`bytes.Equal` scan over dynamic-table entries), not by string work — a CPU
(not allocation) consideration for very large dynamic tables.

## conn per-request allocations (2026-08-14)

`TestConn_Roundtrip_AllocsPerRequest` gates a full request/response on a warm
connection against the in-process peer. It is now **0 allocs/request**, and the
gate is checked in both directions so the number cannot drift either way
unnoticed.

Getting there took three changes, each removing exactly one allocation. The
first two are recorded above and in the changelog; the last is #577:

| Was | Removed by |
|---|---|
| conn's own benchmarks never returned the pooled buffers, so the pools missed every iteration and the reported figure was inflated by the pool's `New` | #574 (`benchDrain`) |
| `authorityOf` ended in `string(fields[i].Value)`, and `[]byte`→`string` copies by definition of the language | #578 (copies into the pooled `Stream`'s own `authorityBuf`) |
| `copyFieldsToSlab` built the field slice with a plain `make([]header.Field, len(fields))` per header block, while the bytes it viewed came from a pool | #577 (`HeaderBlock` pools the fields alongside the bytes) |

The D.1 breakdown above is a record of what the path cost in 2026-06 and is
left as written; `conn.HeaderSlabPool` named a real thing then. It no longer
exists — `HeaderBlock` owns the bytes and the field slice together, and
consumers give it back with `StreamEvent.Release` rather than Putting a pointer
into an exported pool.

Downstream, `grpc`'s `unaryAllocCeiling` fell 4 → 2 for the same reason: a gRPC
response carries two header blocks, HEADERS and TRAILERS, so conn was
allocating a field slice twice per RPC.
