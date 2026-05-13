# D.1 — Zero-Alloc Request Path

**Date:** 2026-05-13
**Status:** Approved
**Goal:** Reduce `Client.Do` hot-path allocations from 49 allocs/op to ≤ 10 allocs/op.

---

## Context

The library targets load generators that issue millions of requests per second.
Every heap allocation on the request path adds GC pressure and degrades
throughput. Phase D.1 attacks the largest allocation sources without changing
the observable correctness guarantees.

### Baseline (pre-D.1)

Measured with `BenchmarkDo_NoHooks` against an in-process httptest h2 server:

```
BenchmarkDo_NoHooks-4   49 allocs/op   3589 B/op   ~77 µs/op
```

Allocation ownership (client goroutine only; server goroutines excluded):

| Source | Allocs | Package |
|---|---|---|
| `buildHeaders` slice backing array | 1 | client |
| `buildHeaders` 4× pseudo-header name `[]byte` conversions | 4 | client |
| `buildHeaders` 4× pseudo-header value `[]byte` conversions | 4 | client |
| `conn.newStream` `*Stream` struct | 1 | conn |
| `conn.newStream` `chan StreamEvent` buffer | 1 | conn |
| `conn.emitHeaderBlock` `[]HeaderField` copy slice | 1 | conn |
| `conn.emitHeaderBlock` N× name copies + N× value copies (N≈5) | 10 | conn |
| `client.drainResponse` `*Response` heap escape | 1 | client |
| `client.parseStatus` regular headers slice | 1 | client |
| `frame.Framer` write payload buffers | ~5 | frame |

---

## API Changes (Breaking)

### `Client.Do`

```go
// Before
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error)

// After
func (c *Client) Do(ctx context.Context, req *Request, resp *Response) error
```

The caller allocates `Response` once and reuses across calls:

```go
var resp client.Response
for {
    if err := c.Do(ctx, req, &resp); err != nil {
        handle(err)
    }
    use(resp.Status, resp.Headers)
    resp.Reset()
}
```

`resp` is undefined on error; the caller must call `Reset()` before reuse
regardless of error status.

### `Client.DoStream`

```go
// Before
func (c *Client) DoStream(ctx context.Context, req *Request) (*StreamResponse, error)

// After
func (c *Client) DoStream(ctx context.Context, req *Request, sr *StreamResponse) error
```

The caller allocates `StreamResponse` once. DoStream zeroes private fields
(`stream`, `release`, `closeOnce`, `drained`) before populating them.
`sr.Close()` must still be called when done.

### `Retryer.Do`

```go
// Before
func (r *Retryer) Do(ctx context.Context, req *Request) (*Response, error)

// After
func (r *Retryer) Do(ctx context.Context, req *Request, resp *Response) error
```

Same semantics as `Client.Do`. The internal retry loop reuses `resp` across
attempts; `Reset()` is called between retries.

### `Response.Reset()`

New method. Clears all fields to zero/nil, retaining slice backing arrays:

```go
func (r *Response) Reset() {
    r.Status = 0
    r.Headers = r.Headers[:0]
    r.Body = r.Body[:0]
    r.Trailers = r.Trailers[:0]
    r.BytesReceived = 0
}
```

### `StreamResponse` internal reset

Unexported `reset()` method called by `DoStream` before filling fields:

```go
func (sr *StreamResponse) reset() {
    sr.Status = 0
    sr.Headers = sr.Headers[:0]
    sr.stream = nil
    sr.release = nil
    sr.closeOnce = sync.Once{}
    sr.drained = false
}
```

---

## Implementation: client package

### D.1.1 — Pseudo-header name constants

Replace `[]byte(":method")` etc. in `buildHeaders` with package-level vars:

```go
var (
    hdrMethod    = []byte(":method")
    hdrScheme    = []byte(":scheme")
    hdrAuthority = []byte(":authority")
    hdrPath      = []byte(":path")
)
```

The HPACK encoder reads these bytes but never mutates them, so sharing across
concurrent calls is safe. Saves 4 allocs/call.

### D.1.2 — `buildHeaders` slice pool

```go
var hdrSlicePool = sync.Pool{New: func() any { s := make([]hpack.HeaderField, 0, 8); return &s }}
```

`buildHeaders` acquires a `*[]hpack.HeaderField` from the pool, appends into
it, returns the slice and the pool handle. Caller (`do`/`doStream`) returns
the handle after `SendHeaders` (encode is synchronous). Saves 1 alloc/call.

### D.1.3 — Caller-provided `*Response` and `*StreamResponse`

`drainResponse` writes into the caller-provided `*Response` instead of
constructing a new one. `parseStatus` writes directly into `resp.Headers`
(using the existing backing array when present). Saves 2 allocs/call.

---

## Implementation: conn package

### D.1.4 — `*Stream` pool

Per-`*Conn` stream pool:

```go
type Conn struct {
    // ...
    streamPool sync.Pool // element type: *Stream
}
```

`newStream` tries `c.streamPool.Get()` first; if nil, falls back to struct
literal + `make(chan StreamEvent, buf)`. On stream close/reset, drain the
event channel to empty, zero all fields, then `c.streamPool.Put(s)`.

The channel capacity must be fixed (equals `opts.StreamEventBuffer`, default
8). If a caller requests a different buffer size than what's in the pool,
discard the pooled stream and allocate fresh. In practice load generators
use a fixed config, so pool hit rate will be near 100% after warmup.

Saves 2 allocs/call after warmup.

### D.1.5 — `emitHeaderBlock` slab allocator

Replace the per-field `append([]byte(nil), ...)` copies with a single slab
allocation:

```go
// connHandler gains a pooled slab for header bytes.
var headerSlabPool = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

func (h *connHandler) emitHeaderBlock(...) error {
    slab := headerSlabPool.Get().(*[]byte)
    *slab = (*slab)[:0]
    h.scratch = h.scratch[:0]
    _ = h.dec.DecodeBlock(hb, func(f hpack.HeaderField) error {
        h.scratch = append(h.scratch, f)
        return nil
    })
    // One slice for all header fields.
    copied := make([]hpack.HeaderField, len(h.scratch))
    for i, f := range h.scratch {
        nameOff := len(*slab)
        *slab = append(*slab, f.Name...)
        valOff := len(*slab)
        *slab = append(*slab, f.Value...)
        copied[i] = hpack.HeaderField{
            Name:      (*slab)[nameOff:valOff:valOff],
            Value:     (*slab)[valOff:len(*slab):len(*slab)],
            Sensitive: f.Sensitive,
        }
    }
    // slab is owned by the StreamEvent until the stream is closed;
    // return it then (see Stream.Close slab lifecycle below).
    s.push(StreamEvent{Headers: copied, slab: *slab, ...})
    // Do NOT return slab to pool here — it is live inside the event.
}
```

The `StreamEvent` gains an unexported `slab []byte` field. When the stream is
done (caller drains to EndStream or calls `Stream.Close`), the slab is
returned to `headerSlabPool`. The caller never sees this field.

`Stream` accumulates all slabs it receives (one per HEADERS/TRAILERS event)
in an unexported `[][]byte` field `slabs`. On stream teardown (`Stream.close`
internal method), all slabs are returned to `headerSlabPool`. This is safe
because the caller has either drained all events (EndStream) or abandoned the
stream (Close), meaning no references to the slab bytes remain.

This trades 2N+1 allocs (N≈5 → 11) for 1 alloc (`make([]hpack.HeaderField,
len)`) + 0 net slab allocs after warmup. Saves ~10 allocs/call.

---

## Implementation: frame package

### D.1.6 — Framer write-buffer pool

`Framer.WriteHeaders` currently builds a payload `[]byte` per call.
Add a package-level pool:

```go
var framePayloadPool = sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}
```

Acquire before encoding, release after `w.Write(payload)`. Saves ~2-3
allocs/call.

---

## Bench gate

```
BenchmarkDo_NoHooks    ≤ 10 allocs/op
BenchmarkDo_WithHooks  ≤ 10 allocs/op
```

Enforced by `make bench` via `bench-gate`. `docs/BENCH_BASELINE.md` updated
with D.1 section after implementation.

The value `[]byte` conversions in `buildHeaders` (4 allocs for method/scheme/
authority/path values) are NOT eliminated in D.1 — that would require
changing `hpack.HeaderField.Name`/`.Value` from `[]byte` to `string`, which
is a separate RFC-layer API change deferred to a future phase.

---

## Testing

- All existing tests updated to new `Do(ctx, req, &resp)` / `DoStream(ctx,
  req, &sr)` call sites.
- New `TestDo_ResponseReuse` — calls `Do` N times with the same `*Response`,
  asserts `resp.Headers` backing array is not reallocated across calls
  (capacity non-decreasing).
- New `TestDoStream_SRReuse` — same pattern for `*StreamResponse`.
- `BenchmarkDo_NoHooks` / `_WithHooks` bench-gated at ≤ 10 allocs/op.
- Race detector passes: `go test -race ./...`.

---

## Non-goals

- `hpack.HeaderField` string API — deferred.
- Zero-alloc HPACK dynamic table updates — out of scope.
- `DoStream.Recv` alloc reduction — out of scope for D.1.
