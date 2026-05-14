# D.1 Zero-Alloc Request Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `Client.Do` hot-path allocations from 49 allocs/op to ≤ 10 allocs/op by pooling streams, slabbing header bytes, pooling encode buffers, and switching to caller-provided response buffers.

**Architecture:** Breaking API change (`Do`/`DoStream`/`Retryer.Do` now take a caller-provided output struct), conn-layer slab allocator for HPACK-decoded header bytes (owned via `conn.StreamEvent.slab` → transferred to `Response.slabs` → returned to pool on `Reset()`), per-Conn stream pool, and HPACK encode-buffer pool.

**Tech Stack:** Go 1.24, `sync.Pool`, `hpack.Encoder.EncodeBlock` (already accepts `dst []byte`), no new dependencies.

---

## File Map

| File | Change |
|---|---|
| `client/response.go` | `Response.Reset()`, `Response.slabs`, `StreamResponse.reset()`, `StreamResponse.slabs`, `parseStatus` refactor |
| `client/client.go` | `Do`/`DoStream` signatures, `do`/`doStream`, `drainResponse`, `buildHeaders` pool + const bytes |
| `client/retry.go` | `retryDoer` interface, `Retryer.Do`/`DoStream`/`doLoop` signatures |
| `conn/stream.go` | `conn.StreamEvent.slab`, `Stream.slabs`, `Stream.returnSlabs()`, stream pool reset helper |
| `conn/handler.go` | `emitHeaderBlock` slab rewrite, package-level `headerSlabPool` |
| `conn/conn.go` | `Conn.streamPool sync.Pool`, pool-aware stream allocation |
| `hpack/encoder.go` | No change — `EncodeBlock(dst, fields)` already supports reuse |
| `conn/conn.go` | encode-buffer pool for `EncodeBlock` call site |
| `client/*_test.go` | Update all 27+ `Do`/`DoStream` call sites |
| `client/bench_metrics_test.go` | Update bench gate comment to ≤ 10 allocs/op |
| `docs/BENCH_BASELINE.md` | Append D.1 section with measured numbers |

---

## Task 1: Create branch

- [ ] **Create and push branch**

```bash
git fetch origin main
git checkout -b claude/d1-zero-alloc origin/main
git push -u origin claude/d1-zero-alloc
```

---

## Task 2: `Response.Reset()` + `StreamResponse.reset()` + `parseStatus` refactor

**Files:** `client/response.go`

The new contract: header bytes in `Response.Headers` and `StreamResponse.Headers` are valid until `resp.Reset()` / `sr.Close()` is called. (Previously "safe to retain after Do returns indefinitely" — updated in doc comment.)

- [ ] **Write failing tests**

Add to `client/client_test.go` (do not run yet — `Do` signature hasn't changed):

```go
func TestResponse_Reset_RetainsBacking(t *testing.T) {
    r := &Response{
        Status:    200,
        Headers:   []hpack.HeaderField{{Name: []byte("a"), Value: []byte("b")}},
        Body:      []byte("body"),
        Trailers:  []hpack.HeaderField{{Name: []byte("t"), Value: []byte("v")}},
        BytesReceived: 42,
    }
    prevHdrCap := cap(r.Headers)
    prevBodyCap := cap(r.Body)
    r.Reset()
    if r.Status != 0 { t.Errorf("Status: want 0, got %d", r.Status) }
    if len(r.Headers) != 0 { t.Errorf("Headers len: want 0, got %d", len(r.Headers)) }
    if cap(r.Headers) < prevHdrCap { t.Errorf("Headers cap regressed") }
    if len(r.Body) != 0 { t.Errorf("Body len: want 0, got %d", len(r.Body)) }
    if cap(r.Body) < prevBodyCap { t.Errorf("Body cap regressed") }
    if r.BytesReceived != 0 { t.Errorf("BytesReceived: want 0, got %d", r.BytesReceived) }
}
```

- [ ] **Run to verify it fails**

```bash
go test ./client/ -run TestResponse_Reset_RetainsBacking -v
```
Expected: FAIL with `undefined: Reset` or similar.

- [ ] **Implement `Response.Reset()`, add `slabs` field, update `StreamResponse`**

In `client/response.go`, modify the `Response` struct and add `Reset()`:

```go
// Response is the synchronous result of Client.Do.
// Headers, Body, and Trailers backing bytes are valid until
// Reset() is called; do not retain slices past that point.
type Response struct {
    Status        int
    Headers       []hpack.HeaderField
    Body          []byte
    Trailers      []hpack.HeaderField
    BytesReceived int64

    slabs [][]byte // owned backing slabs; returned to pool on Reset
}

// Reset clears all exported fields for reuse, retaining backing arrays.
// Any references to Headers[i].Name / .Value / Body / Trailers bytes
// must not be used after Reset returns.
func (r *Response) Reset() {
    for _, s := range r.slabs {
        s = s[:0]
        headerSlabPool.Put(&s)
    }
    r.slabs = r.slabs[:0]
    r.Status = 0
    r.Headers = r.Headers[:0]
    r.Body = r.Body[:0]
    r.Trailers = r.Trailers[:0]
    r.BytesReceived = 0
}
```

Add unexported `reset()` to `StreamResponse`:

```go
// slabs inside StreamResponse tracks slab backing for Headers bytes;
// returned to headerSlabPool on Close.
// (add `slabs [][]byte` field to StreamResponse struct)

func (sr *StreamResponse) reset() {
    sr.Status = 0
    sr.Headers = sr.Headers[:0]
    sr.stream = nil
    sr.release = nil
    sr.closeOnce = sync.Once{}
    sr.drained = false
    // slabs intentionally NOT cleared here — Close() handles them
}
```

Update `StreamResponse.Close()` to return slabs:

```go
func (sr *StreamResponse) Close() error {
    var closeErr error
    sr.closeOnce.Do(func() {
        for _, s := range sr.slabs {
            s = s[:0]
            headerSlabPool.Put(&s)
        }
        sr.slabs = sr.slabs[:0]
        closeErr = sr.stream.Close()
        if sr.release != nil {
            sr.release()
        }
    })
    return closeErr
}
```

Add `headerSlabPool` reference (defined in Task 6 — for now just declare it here as a forward reference; we'll define it in conn package. Actually `headerSlabPool` must be accessible from `client` package. Move the declaration to `conn` package and export it, OR keep it in `client` package and pass slabs via `conn.StreamEvent`.)

> **Architecture note**: `conn.StreamEvent.slab` carries the slab from the conn layer. The client layer moves it to `resp.slabs`. The pool (`headerSlabPool`) is defined in `conn/handler.go` but exported so `client/response.go` can return slabs. Export as `conn.HeaderSlabPool`.

- [ ] **Refactor `parseStatus` to append into caller-provided slice**

Replace the current `parseStatus` signature:

```go
// Before:
func parseStatus(in []hpack.HeaderField) (status int, regular []hpack.HeaderField, err error)

// After:
// parseStatus extracts the integer :status value and appends all
// non-pseudo headers into *dst. Returns ErrEmptyResponse if :status
// is absent.
func parseStatus(in []hpack.HeaderField, dst *[]hpack.HeaderField) (int, error) {
    for i := range in {
        if string(in[i].Name) == ":status" {
            n, perr := strconv.Atoi(string(in[i].Value))
            if perr != nil || n < 0 {
                return 0, fmt.Errorf("%w: %q", ErrInvalidStatus, in[i].Value)
            }
            for j := range in {
                if j != i {
                    *dst = append(*dst, in[j])
                }
            }
            return n, nil
        }
    }
    return 0, ErrEmptyResponse
}
```

This breaks the existing callers in `client.go`. We'll fix those in Task 3.

- [ ] **Run tests (expect failures from broken callers — that's fine)**

```bash
go build ./client/ 2>&1 | head -20
```

Expected: compile errors for `parseStatus` callers. OK — Task 3 fixes them.

- [ ] **Commit (compile-broken is OK for now — Task 3 finishes the change)**

```bash
git add client/response.go
git commit -m "feat(client): D.1 — Response.Reset, StreamResponse.reset, parseStatus dst"
```

---

## Task 3: Change `Do`/`DoStream`/`Retryer` signatures + fix all call sites

**Files:** `client/client.go`, `client/retry.go`, all `client/*_test.go`

This is the largest task. Work through it methodically.

- [ ] **Update `retryDoer` interface in `client/retry.go`**

```go
type retryDoer interface {
    Do(ctx context.Context, req *Request, resp *Response) error
    DoStream(ctx context.Context, req *Request, sr *StreamResponse) error
}
```

- [ ] **Update `Retryer.Do`, `Retryer.doLoop`, `Retryer.DoStream` in `client/retry.go`**

```go
func (r *Retryer) Do(ctx context.Context, req *Request, resp *Response) error {
    if req == nil || !r.canRetry(req) {
        return r.d.Do(ctx, req, resp)
    }
    return r.doLoop(ctx, req, resp)
}

func (r *Retryer) doLoop(ctx context.Context, req *Request, resp *Response) error {
    var err error
    for attempt := 0; attempt < r.opts.MaxAttempts; attempt++ {
        if attempt > 0 {
            backoff := r.opts.Backoff(attempt)
            r.fireRetry(req, attempt, err, backoff)
            if err = r.sleepBackoff(ctx, backoff); err != nil {
                return err
            }
            resp.Reset()
        }
        err = r.d.Do(ctx, req, resp)
        if err == nil {
            if !r.userIsRetryable(nil, resp) {
                return nil
            }
            continue
        }
        if !r.shouldRetryErr(err) {
            return err
        }
    }
    return err
}

func (r *Retryer) DoStream(ctx context.Context, req *Request, sr *StreamResponse) error {
    if req == nil || !r.canRetry(req) {
        return r.d.DoStream(ctx, req, sr)
    }
    var err error
    for attempt := 0; attempt < r.opts.MaxAttempts; attempt++ {
        if attempt > 0 {
            backoff := r.opts.Backoff(attempt)
            r.fireRetry(req, attempt, err, backoff)
            if err = r.sleepBackoff(ctx, backoff); err != nil {
                return err
            }
            sr.reset()
        }
        err = r.d.DoStream(ctx, req, sr)
        if err == nil {
            return nil
        }
        if !r.shouldRetryErr(err) {
            return err
        }
    }
    return err
}
```

Also update `userIsRetryable` to accept `*Response` instead of returning it:

```go
func (r *Retryer) userIsRetryable(err error, resp *Response) bool {
    if r.opts.IsRetryable == nil {
        return false
    }
    return r.opts.IsRetryable(err, resp)
}
```

Update `RetryOptions.IsRetryable` field type:

```go
// IsRetryable func(err error, resp *Response) bool
// (signature unchanged — already takes *Response)
```

- [ ] **Update `Client.Do`, `Client.do`, `Client.DoStream`, `Client.doStream`, `drainResponse` in `client/client.go`**

```go
func (c *Client) Do(ctx context.Context, req *Request, resp *Response) error {
    if err := validateRequest(req); err != nil {
        return err
    }
    start := time.Now()
    authority := req.Authority
    if authority == "" {
        authority = c.authority
    }
    if h := c.hooksPtr.Load(); h != nil && h.OnRequestStart != nil {
        h.OnRequestStart(RequestStartEvent{
            Method: req.Method, Path: req.Path, Authority: authority, Attempt: 0,
        })
    }
    c.metrics.Counters.RequestsStarted.Add(1)

    err := c.do(ctx, req, resp)

    latency := time.Since(start)
    c.metrics.Latency.Request.Observe(latency)
    var status int
    var bytesRecv int64
    if err == nil {
        status = resp.Status
        bytesRecv = resp.BytesReceived
    }
    if err == nil {
        c.metrics.Counters.RequestsSucceeded.Add(1)
    } else {
        c.metrics.Counters.RequestsErrored.Add(1)
    }
    if h := c.hooksPtr.Load(); h != nil && h.OnRequestComplete != nil {
        h.OnRequestComplete(RequestCompleteEvent{
            Method: req.Method, Path: req.Path, Authority: authority,
            Status: status, Err: err, Latency: latency,
            BytesSent: int64(len(req.Body)), BytesRecv: bytesRecv,
            Attempt: 0,
        })
    }
    return err
}

func (c *Client) do(ctx context.Context, req *Request, resp *Response) error {
    cn, release, err := c.tr.acquire(ctx)
    if err != nil {
        return err
    }
    defer release()

    s, err := cn.NewStream(ctx)
    if err != nil {
        return err
    }

    hdrs, putHdrs := buildHeaders(req, c.authority)
    endStream := len(req.Body) == 0 && req.BodyReader == nil
    if err := s.SendHeaders(ctx, hdrs, endStream); err != nil {
        putHdrs()
        _ = s.Close()
        return err
    }
    putHdrs()

    if !endStream {
        if err := writeRequestBody(ctx, s, req); err != nil {
            _ = s.Close()
            return err
        }
    }

    err = drainResponse(ctx, s, req, resp)
    if err != nil {
        _ = s.Close()
    }
    return err
}

func (c *Client) DoStream(ctx context.Context, req *Request, sr *StreamResponse) error {
    if err := validateRequest(req); err != nil {
        return err
    }
    sr.reset()
    start := time.Now()
    authority := req.Authority
    if authority == "" {
        authority = c.authority
    }
    if h := c.hooksPtr.Load(); h != nil && h.OnRequestStart != nil {
        h.OnRequestStart(RequestStartEvent{
            Method: req.Method, Path: req.Path, Authority: authority, Attempt: 0,
        })
    }
    c.metrics.Counters.RequestsStarted.Add(1)

    err := c.doStream(ctx, req, sr)

    latency := time.Since(start)
    c.metrics.Latency.Request.Observe(latency)
    var status int
    if err == nil {
        status = sr.Status
    }
    if err == nil {
        c.metrics.Counters.RequestsSucceeded.Add(1)
    } else {
        c.metrics.Counters.RequestsErrored.Add(1)
    }
    if h := c.hooksPtr.Load(); h != nil && h.OnRequestComplete != nil {
        h.OnRequestComplete(RequestCompleteEvent{
            Method: req.Method, Path: req.Path, Authority: authority,
            Status: status, Err: err, Latency: latency,
            BytesSent: int64(len(req.Body)),
            Attempt:   0,
        })
    }
    return err
}

func (c *Client) doStream(ctx context.Context, req *Request, sr *StreamResponse) error {
    cn, release, err := c.tr.acquire(ctx)
    if err != nil {
        return err
    }

    s, err := cn.NewStream(ctx)
    if err != nil {
        release()
        return err
    }

    hdrs, putHdrs := buildHeaders(req, c.authority)
    endStream := len(req.Body) == 0 && req.BodyReader == nil
    if err := s.SendHeaders(ctx, hdrs, endStream); err != nil {
        putHdrs()
        _ = s.Close()
        release()
        return err
    }
    putHdrs()

    if !endStream {
        if err := writeRequestBody(ctx, s, req); err != nil {
            _ = s.Close()
            release()
            return err
        }
    }

    ev, err := s.Recv(ctx)
    if err != nil {
        _ = s.Close()
        release()
        return err
    }
    if ev.Type != conn.EventHeaders {
        _ = s.Close()
        release()
        return fmt.Errorf("client: expected initial HEADERS, got %s", ev.Type)
    }
    n, perr := parseStatus(ev.Headers, &sr.Headers)
    if perr != nil {
        _ = s.Close()
        release()
        return perr
    }
    sr.Status = n
    if ev.Slab != nil {
        sr.slabs = append(sr.slabs, ev.Slab)
    }
    sr.stream = s
    sr.release = release
    if ev.EndStream {
        sr.drained = true
    }
    return nil
}
```

Update `drainResponse` to fill `*Response` in place:

```go
func drainResponse(ctx context.Context, s *conn.Stream, req *Request, resp *Response) error {
    gotHeaders := false
    for {
        ev, err := s.Recv(ctx)
        if err != nil {
            return err
        }
        switch ev.Type {
        case conn.EventHeaders:
            if gotHeaders {
                if ev.EndStream {
                    return nil
                }
                continue
            }
            n, perr := parseStatus(ev.Headers, &resp.Headers)
            if perr != nil {
                return perr
            }
            resp.Status = n
            if ev.Slab != nil {
                resp.slabs = append(resp.slabs, ev.Slab)
            }
            gotHeaders = true
            if ev.EndStream {
                return nil
            }
        case conn.EventData:
            resp.BytesReceived += int64(len(ev.Data))
            if req.WantBody && len(ev.Data) > 0 {
                resp.Body = append(resp.Body, ev.Data...)
            }
            if ev.EndStream {
                return nil
            }
        case conn.EventTrailers:
            if req.WantTrailers {
                resp.Trailers = append(resp.Trailers, ev.Headers...)
                if ev.Slab != nil {
                    resp.slabs = append(resp.slabs, ev.Slab)
                }
            }
            if ev.EndStream {
                return nil
            }
        case conn.EventReset:
            return &StreamResetError{Code: ev.RSTCode}
        }
    }
}
```

Note: `conn.StreamEvent.Slab` — this field is added in Task 6. For now the code references it; it won't compile until Task 6 adds it to the conn package.

- [ ] **Verify it compiles (will fail until conn.StreamEvent.Slab exists)**

```bash
go build ./client/ 2>&1 | grep -c "error"
```

Note: conn-layer changes come in Task 6. This task focuses on the client layer.

- [ ] **Update all test call sites**

Run this mechanical transformation on all test files. For each `c.Do(ctx, req)` pattern:

```bash
# In client/*_test.go, change patterns:
# "res, err := c.Do(ctx" → declare res first, then assign err
# "if _, err := c.Do(ctx" → declare res first

# Use sed for the common patterns:
cd /home/user/poseidon-http-client

# Pattern 1: "resp, err := c.Do(ctx, req)" → split into 2 lines
sed -i 's/^\(\s*\)\(\w\+\), err := \(c\|r\)\.Do(ctx, \(.*\))$/\1var \2 Response\n\1err := \3.Do(ctx, \4, \&\2)/g' client/*_test.go

# Pattern 2: "if _, err := c.Do(ctx, req);" — need manual fix
grep -n "if _, err := .*\.Do(ctx" client/*_test.go
grep -n "if _, err := .*\.DoStream(ctx" client/*_test.go
```

After running sed, manually verify and fix any remaining patterns. Key patterns to look for:

```bash
grep -n "\.Do(ctx\|\.DoStream(ctx" client/*_test.go | head -30
```

For each remaining occurrence, the fix pattern is:
- `res, err := c.Do(ctx, req)` → `var res Response` (declare above) + `err := c.Do(ctx, req, &res)`
- `sr, err := c.DoStream(ctx, req)` → `var sr StreamResponse` + `err := c.DoStream(ctx, req, &sr)`
- `if _, err := c.Do(ctx, req); err != nil {` → `var _res Response` + `if err := c.Do(ctx, req, &_res); err != nil {`

Also update `client/bench_metrics_test.go`:

```go
// BenchmarkDo_NoHooks
var resp Response
b.ResetTimer()
b.ReportAllocs()
for i := 0; i < b.N; i++ {
    resp.Reset()
    if err := c.Do(ctx, req, &resp); err != nil {
        b.Fatal(err)
    }
}

// BenchmarkDo_WithHooks — same pattern
```

- [ ] **Verify build**

```bash
go build ./... 2>&1 | grep -v "^$" | head -20
```

Expected: only errors from `conn.StreamEvent.Slab` undefined (Task 6 fixes this) and `headerSlabPool` undefined (Task 6 fixes this).

- [ ] **Commit**

```bash
git add client/
git commit -m "feat(client): D.1 — Do/DoStream/Retryer take caller-provided resp/sr"
```

---

## Task 4: `buildHeaders` — const name bytes + slice pool

**Files:** `client/client.go`

- [ ] **Write failing benchmark (already exists — run to establish pre-pool baseline)**

```bash
go test -bench='BenchmarkDo_NoHooks' -benchmem -benchtime=3s -run='^$' ./client/ 2>&1
```

Record current allocs/op.

- [ ] **Add const name bytes and slice pool to `client/client.go`**

After the imports block, before `buildHeaders`:

```go
// Pseudo-header name bytes. The HPACK encoder reads these but never
// mutates them, so sharing across concurrent callers is safe.
var (
    hdrMethod    = []byte(":method")
    hdrScheme    = []byte(":scheme")
    hdrAuthority = []byte(":authority")
    hdrPath      = []byte(":path")
)

// hdrSlicePool recycles the []hpack.HeaderField backing array used by
// buildHeaders. EncodeBlock is synchronous, so the slice is safe to
// return immediately after SendHeaders returns.
var hdrSlicePool = sync.Pool{
    New: func() any {
        s := make([]hpack.HeaderField, 0, 8)
        return &s
    },
}
```

- [ ] **Update `buildHeaders` to use pool and const bytes**

```go
// buildHeaders assembles the on-wire HEADERS slice.
// Returns the slice and a put function; caller MUST call put() after
// SendHeaders returns.
func buildHeaders(req *Request, defaultAuthority string) ([]hpack.HeaderField, func()) {
    scheme := req.Scheme
    if scheme == "" {
        scheme = "https"
    }
    authority := req.Authority
    if authority == "" {
        authority = defaultAuthority
    }
    sp := hdrSlicePool.Get().(*[]hpack.HeaderField)
    *sp = (*sp)[:0]
    *sp = append(*sp,
        hpack.HeaderField{Name: hdrMethod, Value: []byte(req.Method)},
        hpack.HeaderField{Name: hdrScheme, Value: []byte(scheme)},
        hpack.HeaderField{Name: hdrAuthority, Value: []byte(authority)},
        hpack.HeaderField{Name: hdrPath, Value: []byte(req.Path)},
    )
    *sp = append(*sp, req.Headers...)
    return *sp, func() {
        *sp = (*sp)[:0]
        hdrSlicePool.Put(sp)
    }
}
```

Verify the callers in `do` and `doStream` already call `putHdrs()` after `SendHeaders` (added in Task 3).

- [ ] **Run tests**

```bash
go test -race -count=1 ./client/ -timeout=60s 2>&1 | tail -5
```

Expected: PASS (except for conn-layer compile errors deferred to Task 6).

- [ ] **Commit**

```bash
git add client/client.go
git commit -m "perf(client): D.1 — buildHeaders const names + slice pool"
```

---

## Task 5: `conn.Stream` pool — stream struct recycling

**Files:** `conn/stream.go`, `conn/conn.go`

This task pools `*Stream` objects (struct + channel) to avoid 2 allocs per request after warmup.

- [ ] **Add stream pool to `conn.Conn` and pool-aware allocation**

In `conn/conn.go`, add `streamPool sync.Pool` field to `Conn`:

```go
type Conn struct {
    // ... existing fields ...
    streamPool sync.Pool // pooled *Stream; cap(events) == opts.StreamEventBuffer
}
```

In `conn/stream.go`, add a `recycle` helper and update `newStream`:

```go
// recycleStream drains the event channel, zeroes all fields, and
// returns s to pool. Only call when the stream is fully done (both
// sides ended or RST sent).
func recycleStream(pool *sync.Pool, s *Stream) {
    // Drain any remaining events (should be 0 in normal operation).
    for len(s.events) > 0 {
        <-s.events
    }
    s.returnSlabs() // Task 6 adds this
    s.id = 0
    s.w = nil
    s.localEnded = false
    s.remoteEnded = false
    s.closed = false
    s.inflightDone = false
    s.headersReceived = false
    s.recvRefundPending = 0
    s.sendWindow = 0
    pool.Put(s)
}
```

Add pool-aware allocation to `Conn`:

```go
// allocStream returns a fresh or recycled *Stream. eventBuf must
// match opts.StreamEventBuffer for recycled streams to be reused.
func (c *Conn) allocStream(eventBuf int, recvWindow int32) *Stream {
    if v := c.streamPool.Get(); v != nil {
        s := v.(*Stream)
        if cap(s.events) == eventBuf {
            s.w = c
            s.recvWindow = recvWindow
            return s
        }
        // Wrong capacity — discard and fall through to allocate fresh.
    }
    return &Stream{
        w:          c,
        events:     make(chan StreamEvent, eventBuf),
        recvWindow: recvWindow,
    }
}
```

- [ ] **Update `Conn.NewStream` to use `allocStream`**

Find the `NewStream` method in `conn/conn.go`. It calls `newStream(...)`. Replace with `c.allocStream(...)`:

```go
// inside NewStream, the line that was:
//   s := newStream(0, c.opts.StreamEventBuffer, c, int32(recvInitial))
// becomes:
    s := c.allocStream(c.opts.StreamEventBuffer, int32(recvInitial))
```

- [ ] **Return streams to pool on Close**

In `conn/stream.go`, update `Stream.Close()` to recycle when both sides have ended:

```go
func (s *Stream) Close() error {
    s.mu.Lock()
    already := s.closed
    bothEnded := s.localEnded && s.remoteEnded
    s.closed = true
    s.mu.Unlock()
    if already || bothEnded {
        // Stream is fully done; try to recycle.
        if c, ok := s.w.(*Conn); ok {
            recycleStream(&c.streamPool, s)
        }
        return nil
    }
    return s.w.writeRSTStream(s, frame.ErrCodeCancel)
}
```

Also add recycling in the path where both sides ended normally (without explicit `Close()` call). The client layer calls `drainResponse` which returns after `EndStream`, then does NOT call `s.Close()` for the success path. Update `do()` in `client/client.go`:

```go
func (c *Client) do(ctx context.Context, req *Request, resp *Response) error {
    // ...
    err = drainResponse(ctx, s, req, resp)
    if err != nil {
        _ = s.Close()
    } else {
        _ = s.Close() // triggers recycle since both sides ended
    }
    return err
}
```

(i.e., always call `s.Close()` — when `bothEnded == true`, it just recycles without sending RST.)

- [ ] **Run tests with race detector**

```bash
go test -race -count=1 ./conn/ -timeout=90s 2>&1 | tail -5
```

Expected: PASS.

```bash
go test -race -count=1 ./client/ -timeout=60s 2>&1 | tail -5
```

Expected: PASS (pending Task 6 conn changes).

- [ ] **Commit**

```bash
git add conn/stream.go conn/conn.go
git commit -m "perf(conn): D.1 — per-Conn stream pool eliminates *Stream alloc"
```

---

## Task 6: `emitHeaderBlock` slab + `conn.StreamEvent.Slab` + `conn.HeaderSlabPool`

**Files:** `conn/handler.go`, `conn/stream.go`

This is the largest alloc-reduction: N+1 allocs per HEADERS event → 2 amortized.

**Slab ownership contract:**
- `emitHeaderBlock` allocates a slab from `HeaderSlabPool`, writes all Name+Value bytes into it
- The slab reference is stored in `StreamEvent.Slab`
- Client layer reads the event and moves the slab reference to `resp.slabs` (Task 3 already does this)
- `resp.Reset()` returns slabs to `HeaderSlabPool` (Task 2 already does this)
- Contract: do not access `resp.Headers[i].Name` / `.Value` bytes after calling `resp.Reset()`

- [ ] **Add `Slab` field to `conn.StreamEvent`**

In `conn/stream.go`, update `StreamEvent`:

```go
// StreamEvent is one observation about an in-flight stream.
type StreamEvent struct {
    Type      StreamEventType
    Headers   []hpack.HeaderField // EventHeaders / EventTrailers
    Data      []byte              // EventData
    EndStream bool
    RSTCode   frame.ErrCode       // EventReset

    // Slab is the backing byte buffer for all Headers[i].Name and
    // .Value slices. It is owned by the event; the client layer
    // transfers it to Response.slabs and returns it to
    // conn.HeaderSlabPool on Response.Reset().
    // nil when Headers is empty or when slab pooling is not in use.
    Slab []byte
}
```

- [ ] **Add `HeaderSlabPool` to `conn/handler.go`**

```go
// HeaderSlabPool recycles the byte backing for HPACK-decoded header
// fields. Acquire with Get().(*[]byte); reset len to 0; release with
// Put after the Response that holds the slab has been Reset().
var HeaderSlabPool = &sync.Pool{
    New: func() any {
        b := make([]byte, 0, 512)
        return &b
    },
}
```

- [ ] **Rewrite `emitHeaderBlock` in `conn/handler.go`**

```go
func (h *connHandler) emitHeaderBlock(s *Stream, hb []byte, endStream, isTrailer bool) error {
    h.scratch = h.scratch[:0]
    err := h.dec.DecodeBlock(hb, func(f hpack.HeaderField) error {
        h.scratch = append(h.scratch, f)
        return nil
    })
    if err != nil {
        return &ConnError{Code: frame.ErrCodeCompressionError, Reason: err.Error()}
    }
    evType := EventHeaders
    if isTrailer {
        evType = EventTrailers
    }
    if endStream {
        s.markRemoteEnd()
        h.streams.markStreamDone(s.id)
    }

    // Build one slab for all header bytes, one slice for all fields.
    slabPtr := HeaderSlabPool.Get().(*[]byte)
    *slabPtr = (*slabPtr)[:0]
    copied := make([]hpack.HeaderField, len(h.scratch))
    for i, f := range h.scratch {
        nameOff := len(*slabPtr)
        *slabPtr = append(*slabPtr, f.Name...)
        valOff := len(*slabPtr)
        *slabPtr = append(*slabPtr, f.Value...)
        endOff := len(*slabPtr)
        copied[i] = hpack.HeaderField{
            Name:      (*slabPtr)[nameOff:valOff:valOff],
            Value:     (*slabPtr)[valOff:endOff:endOff],
            Sensitive: f.Sensitive,
        }
    }
    s.push(StreamEvent{
        Type:      evType,
        Headers:   copied,
        Slab:      *slabPtr,
        EndStream: endStream,
    })
    return nil
}
```

- [ ] **Add `Stream.returnSlabs()` (needed by Task 5's `recycleStream`)**

In `conn/stream.go`, add a `slabs [][]byte` field to `Stream` and `returnSlabs()`:

```go
type Stream struct {
    // ... existing fields ...
    // slabs holds byte backing buffers for header events that have
    // not yet been consumed by the client layer. On stream recycling,
    // any unconsumed slabs are returned to HeaderSlabPool.
    slabs [][]byte
}

// returnSlabs returns any unconsumed header slabs to HeaderSlabPool.
// Called by recycleStream before putting the stream back in the pool.
func (s *Stream) returnSlabs() {
    for _, sl := range s.slabs {
        sl = sl[:0]
        HeaderSlabPool.Put(&sl)
    }
    s.slabs = s.slabs[:0]
}
```

Wait — actually the stream does NOT need `slabs [][]byte` because the slab is transferred via `StreamEvent.Slab` to the client layer. The stream itself never retains slabs past `push()`. The `recycleStream` helper calls `returnSlabs()` for slabs that were pushed but never consumed (e.g., stream was closed/abandoned without draining). So we DO need `Stream.slabs` to track which slabs were pushed.

Update `stream.push()` to also track slabs in the stream:

```go
func (s *Stream) push(e StreamEvent) {
    if e.Slab != nil {
        s.mu.Lock()
        s.slabs = append(s.slabs, e.Slab)
        s.mu.Unlock()
    }
    select {
    case s.events <- e:
        return
    default:
    }
    // ... existing overflow handling ...
}
```

And the client side must REMOVE the slab from `s.slabs` when it reads the event. This is getting complex.

**Simpler alternative**: Don't track slabs on the stream. When the stream is abandoned (Close without drain), the slabs leak until GC. This is acceptable for a load generator that always drains. The slab pool just won't reclaim as aggressively.

For the plan, use the simpler approach: `Stream.returnSlabs()` is a no-op stub, `Stream.slabs` is not needed. Slabs live in events; if events are drained they transfer to `resp.slabs`; if not (rare), they're GC'd normally.

Remove `Stream.slabs` and `returnSlabs()` tracking; have `recycleStream` simply drain the event channel (which also drops any unread events with their slabs — those will be GC'd).

```go
func recycleStream(pool *sync.Pool, s *Stream) {
    for len(s.events) > 0 {
        <-s.events // drops event; slab in event is GC'd if not transferred
    }
    // No slab tracking needed — slabs are owned by events.
    s.id = 0
    s.w = nil
    s.localEnded = false
    s.remoteEnded = false
    s.closed = false
    s.inflightDone = false
    s.headersReceived = false
    s.recvRefundPending = 0
    s.sendWindow = 0
    pool.Put(s)
}

func (s *Stream) returnSlabs() {} // no-op — slabs tracked via events
```

- [ ] **Verify `conn` package builds**

```bash
go build ./conn/ 2>&1
```

Expected: PASS.

- [ ] **Now `client` package should also build (all forward refs resolved)**

```bash
go build ./... 2>&1
```

Expected: PASS.

- [ ] **Run full test suite**

```bash
go test -race -count=1 ./... -timeout=90s 2>&1 | tail -10
```

Expected: all packages PASS.

- [ ] **Commit**

```bash
git add conn/handler.go conn/stream.go
git commit -m "perf(conn): D.1 — emitHeaderBlock slab pool + StreamEvent.Slab"
```

---

## Task 7: HPACK encode-buffer pool

**Files:** `conn/conn.go`

`EncodeBlock(nil, fields)` allocates a fresh `[]byte` per call. We pool `dst`.

- [ ] **Add encode buffer pool to `conn/conn.go`**

At package level in `conn/conn.go`:

```go
// encBufPool recycles the HPACK block-fragment buffer. The buffer is
// returned immediately after Framer.WriteHeaders — the call is
// synchronous under wmu.
var encBufPool = sync.Pool{
    New: func() any {
        b := make([]byte, 0, 256)
        return &b
    },
}
```

- [ ] **Update `writeHeaders` to use pool**

```go
func (c *Conn) writeHeaders(_ context.Context, s *Stream, fields []hpack.HeaderField, endStream bool) error {
    if c.closed.Load() {
        return ErrConnClosed
    }
    c.wmu.Lock()
    defer c.wmu.Unlock()
    if s.id == 0 {
        c.smu.Lock()
        s.id = c.nextID
        c.nextID += 2
        c.streams[s.id] = s
        c.smu.Unlock()
        c.psMu.RLock()
        initial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
        c.psMu.RUnlock()
        s.mu.Lock()
        s.sendWindow = int32(initial)
        s.mu.Unlock()
    }
    buf := encBufPool.Get().(*[]byte)
    *buf = (*buf)[:0]
    block := c.enc.EncodeBlock(*buf, fields)
    err := c.fr.WriteHeaders(frame.WriteHeadersParams{
        StreamID:      s.id,
        BlockFragment: block,
        EndHeaders:    true,
        EndStream:     endStream,
    })
    *buf = block[:0]
    encBufPool.Put(buf)
    if err != nil {
        return err
    }
    c.bumpFramesSent()
    return nil
}
```

Note: `fr.WriteHeaders` writes `block` synchronously to the wire under `wmu`. After it returns, `block` is no longer needed. We reset `*buf = block[:0]` (retaining capacity) before returning to pool.

- [ ] **Run full test suite**

```bash
go test -race -count=1 ./... -timeout=90s 2>&1 | tail -5
```

Expected: PASS.

- [ ] **Run lint**

```bash
golangci-lint run ./... 2>&1
```

Expected: 0 issues.

- [ ] **Commit**

```bash
git add conn/conn.go
git commit -m "perf(conn): D.1 — pool HPACK encode buffer in writeHeaders"
```

---

## Task 8: Bench gate + reuse tests + BENCH_BASELINE.md

**Files:** `client/client_test.go`, `client/bench_metrics_test.go`, `docs/BENCH_BASELINE.md`

- [ ] **Add `TestDo_ResponseReuse` to `client/client_test.go`**

```go
func TestDo_ResponseReuse(t *testing.T) {
    srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        w.Header().Set("x-test", "value")
        w.WriteHeader(200)
    }))
    srv.EnableHTTP2 = true
    srv.StartTLS()
    defer srv.Close()

    c, err := NewClient(ClientOptions{
        Addr: srv.Listener.Addr().String(),
        ConnOpts: conn.ConnOptions{
            Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
        },
    })
    if err != nil {
        t.Fatalf("NewClient: %v", err)
    }
    defer c.Close()

    var resp Response
    const N = 5
    var prevHdrCap int
    for i := 0; i < N; i++ {
        resp.Reset()
        if err := c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil {
            t.Fatalf("Do[%d]: %v", i, err)
        }
        if resp.Status != 200 {
            t.Fatalf("Do[%d]: status %d", i, resp.Status)
        }
        if i > 0 && cap(resp.Headers) < prevHdrCap {
            t.Errorf("Headers backing array reallocated at iteration %d (cap went %d→%d)",
                i, prevHdrCap, cap(resp.Headers))
        }
        prevHdrCap = cap(resp.Headers)
    }
}
```

- [ ] **Add `TestDoStream_SRReuse` to `client/client_test.go`**

```go
func TestDoStream_SRReuse(t *testing.T) {
    srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(200)
    }))
    srv.EnableHTTP2 = true
    srv.StartTLS()
    defer srv.Close()

    c, err := NewClient(ClientOptions{
        Addr: srv.Listener.Addr().String(),
        ConnOpts: conn.ConnOptions{
            Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
        },
    })
    if err != nil {
        t.Fatalf("NewClient: %v", err)
    }
    defer c.Close()

    var sr StreamResponse
    for i := 0; i < 5; i++ {
        if err := c.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}, &sr); err != nil {
            t.Fatalf("DoStream[%d]: %v", i, err)
        }
        if sr.Status != 200 {
            t.Fatalf("DoStream[%d]: status %d", i, sr.Status)
        }
        if err := sr.Close(); err != nil {
            t.Fatalf("Close[%d]: %v", i, err)
        }
    }
}
```

- [ ] **Run new tests**

```bash
go test -race -count=1 ./client/ -run 'TestDo_ResponseReuse|TestDoStream_SRReuse' -v 2>&1
```

Expected: PASS.

- [ ] **Run bench and record numbers**

```bash
go test -bench='BenchmarkDo_' -benchmem -benchtime=5s -run='^$' ./client/ 2>&1 | tee /tmp/d1_bench.txt
cat /tmp/d1_bench.txt
```

Record actual ns/op, B/op, allocs/op. **Target: ≤ 10 allocs/op.**

If allocs/op > 10, profile to find remaining sources:

```bash
go test -bench='BenchmarkDo_NoHooks' -benchmem -memprofile=/tmp/d1_mem.prof -benchtime=5s -run='^$' ./client/
go tool pprof -alloc_objects -top /tmp/d1_mem.prof 2>&1 | head -20
```

Identify any remaining client-owned allocs and fix them before proceeding.

- [ ] **Update bench gate comment in `client/bench_metrics_test.go`**

Update the comment above both benchmarks to document the new gate:

```go
// BenchmarkDo_NoHooks measures Do overhead with no hooks set.
// Gate: ≤ 10 allocs/op (D.1 target). Run: make bench.
func BenchmarkDo_NoHooks(b *testing.B) {

// BenchmarkDo_WithHooks measures Do overhead with all request hooks set.
// Gate: ≤ 10 allocs/op (D.1 target). Run: make bench.
func BenchmarkDo_WithHooks(b *testing.B) {
```

- [ ] **Append D.1 section to `docs/BENCH_BASELINE.md`**

Append the following (substituting `FILL` with actual measured numbers):

```markdown
## D.1 — zero-alloc request path

Benchmarks after stream pool, emitHeaderBlock slab, encode-buffer pool,
and caller-provided Response.

| Bench                  | ns/op | B/op | allocs/op |
|------------------------|------:|-----:|----------:|
| BenchmarkDo_NoHooks    | FILL  | FILL | FILL      |
| BenchmarkDo_WithHooks  | FILL  | FILL | FILL      |

Gate: both benches ≤ 10 allocs/op. Measured against httptest h2 server;
latency dominated by socket I/O. Header bytes valid until `resp.Reset()`.
```

- [ ] **Run full suite one final time**

```bash
go test -race -count=1 ./... -timeout=90s 2>&1 | tail -10
golangci-lint run ./... 2>&1
```

Expected: all packages PASS, 0 lint issues.

- [ ] **Commit**

```bash
git add client/client_test.go client/bench_metrics_test.go docs/BENCH_BASELINE.md
git commit -m "test(client): D.1 — reuse tests + bench gate ≤10 allocs/op + baseline"
```

---

## Task 9: Push + open draft PR

- [ ] **Push branch**

```bash
git push -u origin claude/d1-zero-alloc
```

- [ ] **Open draft PR via GitHub MCP**

Title: `perf(client): D.1 — zero-alloc request path (≤10 allocs/op)`

Body should include:
- Baseline: 49 allocs/op
- Achieved: (measured number) allocs/op
- Breaking changes: `Do`/`DoStream`/`Retryer.Do` signatures; header bytes valid until `Reset()`
- Closes #N (if issue exists)
