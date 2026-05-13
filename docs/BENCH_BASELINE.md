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
BenchmarkHuffmanDecode_path                ~1950 ns/op 0 B/op   0 allocs/op  (linear walk; FSM optimisation deferred)
BenchmarkDecodeInteger_Max                 ~4 ns/op    0 B/op   0 allocs/op
BenchmarkStaticIndex_Hit                   ~9 ns/op    0 B/op   0 allocs/op
BenchmarkReadBufPool_GetPut                ~8 ns/op    0 B/op   0 allocs/op
```

All hot-path benchmarks: **0 B/op, 0 allocs/op**. Bench gate enforces this.

## Notes

- `BenchmarkHuffmanDecode_path` uses bit-by-bit linear walk over the canonical 257-entry table. RFC 7541 App. B FSM (4-bit nibbles) is a known optimisation deferred to a follow-up; the absolute ns/op target from the spec (~80 ns) is not yet met for this bench. The 0-allocs gate, the relative-regression gate, and all RFC vectors pass — correctness is unaffected.
- GitHub Actions ubuntu-24.04 runners are noisier than this baseline; the bench-gate workflow uses **relative** comparison (benchstat alpha=0.05) against `main` to avoid false-positive flakes.

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
