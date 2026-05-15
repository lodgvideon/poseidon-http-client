# D.2 — Request/Response Body Streaming Design

**Date:** 2026-05-15  
**Status:** Approved  
**Goal:** Complete the body I/O layer — `content-length` header emission, streaming response bodies via `io.ReadCloser`, and Retryer safety for non-replayable bodies.

---

## Background

The request body *send* path (`Request.Body []byte`, `Request.BodyReader io.Reader`, `writeRequestBody`, `Stream.SendData`) was implemented as part of a previous sprint and is fully tested. D.2 adds the three remaining pieces:

1. `Request.ContentLength int64` → `content-length` header in HEADERS frame
2. `Request.StreamBody bool` → `Do` returns after HEADERS received; body available via `Response.BodyReader io.ReadCloser`
3. `Retryer` guard: refuse to retry when `req.BodyReader != nil` (exhausted reader), return `ErrBodyNotReplayable`

Plus a housekeeping fix: BENCH_BASELINE.md still says `BenchmarkHuffmanDecode_path ~1950 ns/op (linear walk; FSM optimisation deferred)` — the FSM was implemented and the bench is now 45 ns/op.

---

## Architecture

```
Request.BodyReader io.Reader ──► writeBodyReader (pool buf) ──► Stream.SendData ──► DATA frames ──► server
Request.ContentLength ≥ 0   ──► content-length header in HEADERS frame

server ──► DATA events ──► responseBodyReader.Read() ──► Response.BodyReader io.ReadCloser
                                  ↑ wraps stream.Recv loop
                                  ↑ Close() → RST_STREAM(CANCEL) if incomplete + release conn
```

**Invariants:**

- `Do` blocks until response HEADERS received, then returns when `StreamBody=true`.
- `Response.BodyReader` is non-nil iff `req.StreamBody=true`; nil otherwise (current behaviour preserved).
- Caller owns `BodyReader`: must drain + `Close()` before calling `resp.Reset()` or the next `Do`.
- `Response.Reset()` calls `BodyReader.Close()` if non-nil — leak-safe even if caller forgets.
- `BodyReader.Close()` is idempotent; RST_STREAM(CANCEL) sent only when stream not fully drained.
- Pool release (`conn → pool`) deferred into `BodyReader.Close()` when `StreamBody=true`.
- `Request.Body == nil && req.BodyReader == nil && !req.StreamBody` → zero behaviour change.

---

## Section 1: `Request.ContentLength`

**File:** `client/request.go`

Add field to `Request`:

```go
// ContentLength is the body size in bytes.
// ≥ 0: a content-length header is emitted.
// -1 (default): no content-length header.
// Ignored when Body and BodyReader are both nil.
ContentLength int64
```

**File:** `client/client.go`, `buildHeaders`

After appending `req.Headers`, when `(req.Body != nil || req.BodyReader != nil) && req.ContentLength >= 0`:

```go
var hdrContentLength = []byte("content-length")

// in buildHeaders, after req.Headers:
if (len(req.Body) > 0 || req.BodyReader != nil) && req.ContentLength >= 0 {
    *sp = append(*sp, hpack.HeaderField{
        Name:  hdrContentLength,
        Value: []byte(strconv.FormatInt(req.ContentLength, 10)),
    })
}
```

`hdrContentLength` is a package-level `[]byte` const (like the existing `hdrMethod` etc.) to avoid per-call allocation.

`strconv.FormatInt` allocates a small string. This is on the header-build path (once per request), not the hot DATA path, so the alloc is acceptable. The string is appended into the pooled `hdrSlicePool` slice.

---

## Section 2: `uploadBufPool`

**File:** `client/client.go`

`writeBodyReader` currently does `buf := make([]byte, readChunkSize)` — one heap allocation per streaming upload. Replace with a pool:

```go
var uploadBufPool = sync.Pool{New: func() any {
    b := make([]byte, readChunkSize)
    return &b
}}

func writeBodyReader(ctx context.Context, s *conn.Stream, r io.Reader) error {
    bufp := uploadBufPool.Get().(*[]byte)
    defer uploadBufPool.Put(bufp)
    buf := *bufp
    for { /* existing logic using buf */ }
}
```

---

## Section 3: `Request.StreamBody` + `Response.BodyReader`

### `Request` addition (`client/request.go`)

```go
// StreamBody, when true, causes Do to return immediately after the
// response HEADERS frame arrives. The response body is available via
// Response.BodyReader. WantBody is ignored when StreamBody is true.
// Caller MUST call Response.BodyReader.Close() before Response.Reset().
StreamBody bool
```

### `Response` addition (`client/response.go`)

```go
// BodyReader is set by Do when Request.StreamBody is true.
// Caller reads the body then calls Close(). Trailers (if any) are
// written into Response.Trailers just before Close() returns io.EOF.
// Reset() calls Close() automatically if the caller forgets.
BodyReader io.ReadCloser
```

Update `Reset()`:

```go
func (r *Response) Reset() {
    if r.BodyReader != nil {
        _ = r.BodyReader.Close()
        r.BodyReader = nil
    }
    // existing slab + field cleanup ...
}
```

### `do()` branch (`client/client.go`)

`drainResponse` pumps all stream events; it cannot be reused for `StreamBody` (which must stop after initial HEADERS). Add a new helper:

```go
// receiveInitialHeaders pumps stream events until the first HEADERS
// frame arrives, populating resp.Status and resp.Headers.
// Returns with the stream open; caller is responsible for further Recv.
func receiveInitialHeaders(ctx context.Context, s *conn.Stream, resp *Response) error {
    for {
        ev, err := s.Recv(ctx)
        if err != nil {
            return err
        }
        if ev.Type != conn.EventHeaders {
            continue // skip DATA before initial HEADERS (shouldn't happen per RFC but be safe)
        }
        if ev.Slab != nil {
            resp.slabs = append(resp.slabs, ev.Slab)
        }
        n, perr := parseStatus(ev.Headers, &resp.Headers)
        if perr != nil {
            return perr
        }
        resp.Status = n
        return nil
    }
}
```

Then in `do()`:

```go
if req.StreamBody {
    if err := receiveInitialHeaders(ctx, s, resp); err != nil {
        return err
    }
    ctx2, cancel := context.WithCancel(ctx)
    resp.BodyReader = newResponseBodyReader(ctx2, cancel, s, releaseConn, resp)
    return nil  // conn released via BodyReader.Close()
}
// else: existing drainResponse path, unchanged
```

`releaseConn` is the closure that returns the `*conn.Conn` to the pool (already threaded through `do()` for `DoStream`).

---

## Section 4: `responseBodyReader` (`client/body.go`)

New file. No dependency on anything outside `client/` and `conn/`.

```go
package client

import (
    "context"
    "fmt"
    "io"
    "sync"

    "github.com/lodgvideon/poseidon-http-client/conn"
)

type responseBodyReader struct {
    ctx       context.Context
    cancel    context.CancelFunc
    stream    *conn.Stream
    release   func() error  // closes stream + returns conn to pool
    resp      *Response     // for writing Trailers
    buf       []byte        // leftover bytes from last DATA event
    closeOnce sync.Once
    done      bool
}

func newResponseBodyReader(
    ctx context.Context,
    cancel context.CancelFunc,
    s *conn.Stream,
    release func() error,
    resp *Response,
) *responseBodyReader {
    return &responseBodyReader{
        ctx:     ctx,
        cancel:  cancel,
        stream:  s,
        release: release,
        resp:    resp,
    }
}

func (r *responseBodyReader) Read(p []byte) (int, error) {
    if len(r.buf) > 0 {
        n := copy(p, r.buf)
        r.buf = r.buf[n:]
        return n, nil
    }
    if r.done {
        return 0, io.EOF
    }
    for {
        ev, err := r.stream.Recv(r.ctx)
        if err != nil {
            return 0, err
        }
        switch ev.Type {
        case conn.EventData:
            n := copy(p, ev.Data)
            if n < len(ev.Data) {
                r.buf = ev.Data[n:] // ev.Data is deep-copied by conn layer; safe to retain
            }
            if ev.EndStream {
                r.done = true
            }
            if ev.EndStream && n == len(ev.Data) {
                return n, io.EOF
            }
            return n, nil
        case conn.EventTrailers:
            if r.resp != nil && len(ev.Headers) > 0 {
                r.resp.Trailers = append(r.resp.Trailers[:0], ev.Headers...)
            }
            r.done = true
            return 0, io.EOF
        case conn.EventReset:
            r.done = true
            return 0, fmt.Errorf("client: stream reset by peer: code %d", ev.RSTCode)
        case conn.EventHeaders:
            continue // skip spurious post-initial HEADERS
        }
    }
}

func (r *responseBodyReader) Close() error {
    var err error
    r.closeOnce.Do(func() {
        r.cancel()
        err = r.release() // s.Close() sends RST_STREAM(CANCEL) when not drained
    })
    return err
}
```

**Note on `ev.Data` lifetime:** `conn.EventData.Data` is deep-copied by the conn layer (`OnData` does `dataCopy := append([]byte(nil), p...)`). Retaining `ev.Data[n:]` in `r.buf` is safe across `Recv` calls.

---

## Section 5: Retryer guard

**File:** `client/retry.go`

New sentinel:

```go
// ErrBodyNotReplayable is returned by Retryer.Do when req.BodyReader
// is non-nil and the request fails. Retrying would re-read an already
// partially or fully consumed reader.
var ErrBodyNotReplayable = errors.New("client: request body is not replayable")
```

In `Retryer.doLoop` (or `Retryer.Do`), before the retry loop:

```go
if req.BodyReader != nil {
    // Single attempt only; retrying a consumed reader is undefined.
    err := r.inner.Do(ctx, req, resp)
    if err != nil {
        return fmt.Errorf("%w: %w", ErrBodyNotReplayable, err)
    }
    return nil
}
```

`Request.Body []byte` IS replayable (slice is not consumed) — no guard needed there.

---

## Section 6: File changes

```
Modified:
  client/request.go        — Request.ContentLength int64, Request.StreamBody bool
  client/client.go         — buildHeaders: content-length header; writeBodyReader: uploadBufPool;
                             do(): StreamBody branch; hdrContentLength const
  client/response.go       — Response.BodyReader io.ReadCloser; Reset() safety close
  client/retry.go          — ErrBodyNotReplayable; single-attempt guard for BodyReader

Created:
  client/body.go           — responseBodyReader

Modified (tests + docs):
  client/integration_test.go    — response streaming tests (see §7)
  client/conformance_test.go    — §8.1 END_STREAM conformance for streaming response
  client/retry_test.go          — ErrBodyNotReplayable test
  docs/RFC_COVERAGE.md          — streaming response body row (§8.1)
  docs/BENCH_BASELINE.md        — fix stale Huffman note (1950→45 ns/op)
```

---

## Section 7: Tests

### Integration (`client/integration_test.go`)

```
TestDo_StreamBody_Small          — 64-byte response; drain via BodyReader; verify bytes
TestDo_StreamBody_Large          — 1 MiB response; io.Copy(io.Discard); no OOM
TestDo_StreamBody_Trailers       — response with trailers; resp.Trailers populated after Close
TestDo_StreamBody_CloseEarly     — Close() before fully drained; RST_STREAM(CANCEL) sent
TestDo_StreamBody_ResetByServer  — server sends RST_STREAM; Read returns error
TestDo_StreamBody_CtxCancel      — ctx cancel after Do returns; Read returns ctx.Err()
TestDo_StreamBody_ResetForgot    — resp.Reset() without Close(); no stream leak
TestDo_ContentLength_Sent        — POST with ContentLength=N; server receives content-length header
```

### Retryer (`client/retry_test.go`)

```
TestRetryer_BodyReader_NoRetry   — BodyReader != nil + failure → ErrBodyNotReplayable
TestRetryer_Body_Retries         — Body []byte + retriable failure → succeeds on retry
```

### Conformance (`client/conformance_test.go`)

```
TestConformance_RFC7540_Sec8_1_StreamBody_EndStream
    — END_STREAM on final DATA frame; no RST_STREAM if fully drained
```

---

## Non-goals

- Rewindable body / automatic retry of `BodyReader` requests (caller wraps in `bytes.NewReader` and resets manually)
- `io.Pipe` helpers (caller can already pass `*io.PipeReader` as `BodyReader`)
- Chunked transfer encoding (`Transfer-Encoding: chunked` is HTTP/1.1; HTTP/2 uses DATA frames)
- Server push
- `DoStream` changes (DoStream already streams; BodyReader is a simpler interface over the same mechanism)
