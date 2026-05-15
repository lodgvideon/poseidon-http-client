# D.2 — Request/Response Body Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete body I/O: `content-length` header emission, pooled upload buffer, and streaming response bodies via `Response.BodyReader io.ReadCloser`.

**Architecture:** `Request.ContentLength` emits `content-length` in the HEADERS frame; `writeBodyReader` switches to a pooled 16 KiB buffer; `do()` gains a `StreamBody` branch that receives the initial HEADERS then hands the open stream to a `responseBodyReader` (new `client/body.go`) which implements `io.ReadCloser` over `conn.Stream.Recv`; `Response.BodyReader` field + `Reset()` safety-close complete the receive side.

**Tech Stack:** Go 1.24, `client/` package only (plus `conn.Stream.Recv` which already exists).

---

## Background: what already exists (do NOT re-implement)

- `Request.Body []byte` and `Request.BodyReader io.Reader` — request send fields ✓
- `writeRequestBody` / `writeBodyReader` / `Stream.SendData` — request send path ✓
- `Retryer.canRetry` already returns false when `req.BodyReader != nil` — no change needed ✓
- `doStream` shows the pattern for receiving initial HEADERS + deferring `release()` into caller ✓

---

## File Structure

```
Modified:
  client/request.go        — add ContentLength int64, StreamBody bool
  client/client.go         — add hdrContentLength const, update buildHeaders,
                             switch writeBodyReader to uploadBufPool,
                             refactor do() for StreamBody path
  client/response.go       — add Response.BodyReader io.ReadCloser,
                             update Reset() for safety-close

Created:
  client/body.go           — responseBodyReader struct, Read, Close

Modified (tests + docs):
  client/integration_test.go  — content-length + StreamBody integration tests
  client/conformance_test.go  — §8.1 END_STREAM conformance
  docs/RFC_COVERAGE.md        — §8.1 streaming response row
  docs/BENCH_BASELINE.md      — fix stale Huffman note (1950→45 ns/op)
```

---

## Task 1: `ContentLength` header + `uploadBufPool`

**Files:**
- Modify: `client/request.go` (add field)
- Modify: `client/client.go` (const + buildHeaders + writeBodyReader pool)
- Test: `client/integration_test.go`

**Context:** `buildHeaders` (line ~393 in `client/client.go`) assembles the HEADERS slice from a pooled `hdrSlicePool`. It currently has const byte slices `hdrMethod`, `hdrScheme`, `hdrAuthority`, `hdrPath` for pseudo-headers. We add `hdrContentLength` the same way. `writeBodyReader` (line ~435) allocates `buf := make([]byte, readChunkSize)` every call — replace with a pool.

- [ ] **Step 1: Write the failing test**

Add to `client/integration_test.go`:

```go
func TestIntegration_Client_POST_ContentLength_Header(t *testing.T) {
	var gotCL string
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCL = r.Header.Get("Content-Length")
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	body := strings.NewReader("hello")
	err := c.Do(ctx, &client.Request{
		Method:        "POST",
		Path:          "/",
		BodyReader:    body,
		ContentLength: 5,
	}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotCL != "5" {
		t.Fatalf("content-length = %q, want %q", gotCL, "5")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./client/ -run TestIntegration_Client_POST_ContentLength_Header -v -count=1
```

Expected: FAIL — `content-length = "", want "5"` (field not implemented yet).

- [ ] **Step 3: Add `ContentLength int64` to `Request`**

In `client/request.go`, after the existing `BodyReader io.Reader` field (line ~30):

```go
// ContentLength is the body byte count. When > 0 and BodyReader is
// non-nil, a content-length header is emitted in the HEADERS frame.
// Zero or negative: no content-length header. For Body []byte the
// length is derived automatically; this field is ignored in that case.
ContentLength int64
```

- [ ] **Step 4: Add `hdrContentLength` const and update `buildHeaders`**

In `client/client.go`, in the `var` block alongside `hdrMethod` etc. (line ~374):

```go
hdrContentLength = []byte("content-length")
```

At the end of `buildHeaders` (after `*sp = append(*sp, req.Headers...)`), before the `return`:

```go
if req.BodyReader != nil && req.ContentLength > 0 {
    *sp = append(*sp, hpack.HeaderField{
        Name:  hdrContentLength,
        Value: []byte(strconv.FormatInt(req.ContentLength, 10)),
    })
}
```

Add `"strconv"` to the `client/client.go` import block if not already present.

- [ ] **Step 5: Replace `writeBodyReader` allocation with `uploadBufPool`**

In `client/client.go`, add the pool (alongside `hdrSlicePool`):

```go
// uploadBufPool recycles the per-call read buffer used by writeBodyReader.
var uploadBufPool = sync.Pool{New: func() any {
    b := make([]byte, readChunkSize)
    return &b
}}
```

Replace the first line of `writeBodyReader` (the `buf := make(...)` line, currently line ~436):

```go
func writeBodyReader(ctx context.Context, s *conn.Stream, r io.Reader) error {
	bufp := uploadBufPool.Get().(*[]byte)
	defer uploadBufPool.Put(bufp)
	buf := *bufp
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			final := rerr == io.EOF
			if werr := s.SendData(ctx, buf[:n], final); werr != nil {
				return werr
			}
			if final {
				return nil
			}
		}
		if rerr == io.EOF {
			return s.SendData(ctx, nil, true)
		}
		if rerr != nil {
			return fmt.Errorf("client: read request body: %w", rerr)
		}
	}
}
```

- [ ] **Step 6: Run the test to verify it passes**

```bash
go test ./client/ -run TestIntegration_Client_POST_ContentLength_Header -v -count=1
```

Expected: PASS.

- [ ] **Step 7: Run full test suite**

```bash
go test -race -count=1 ./...
```

Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add client/request.go client/client.go client/integration_test.go
git commit -m "feat(client): D.2 ContentLength header + uploadBufPool"
```

---

## Task 2: `responseBodyReader` (`client/body.go`)

**Files:**
- Create: `client/body.go`

**Context:** `responseBodyReader` wraps a `*conn.Stream` and implements `io.ReadCloser`. `Read` pumps `stream.Recv(ctx)` until it gets `EventData` bytes; `buf []byte` holds unconsumed tail when `copy` fills `p` before the DATA payload is exhausted. `Close` calls `stream.Close()` (which sends RST_STREAM(CANCEL) when the stream is not fully drained) and then `release()` (returns conn to pool). This is created in this task so Task 3 can reference the type. Integration tests come in Task 3.

`conn.StreamEvent` fields used here:
- `ev.Type` — one of `conn.EventData`, `conn.EventHeaders`, `conn.EventTrailers`, `conn.EventReset`
- `ev.Data []byte` — payload (deep-copied by conn layer, safe to retain)
- `ev.Headers []hpack.HeaderField` — trailer fields for EventTrailers
- `ev.RSTCode frame.ErrCode` — reset code for EventReset
- `ev.EndStream bool` — true on final event

`StreamResetError` is already defined in the `client` package (used by `drainResponse`).

- [ ] **Step 1: Create `client/body.go`**

```go
package client

import (
	"context"
	"io"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// responseBodyReader streams response DATA frames as an io.ReadCloser.
// Constructed by do() when Request.StreamBody is true; ownership
// transfers to Response.BodyReader.
type responseBodyReader struct {
	ctx       context.Context
	stream    *conn.Stream
	release   func()    // returns conn to pool; called exactly once in Close
	resp      *Response // written with trailers when EventTrailers arrives
	buf       []byte    // unconsumed tail of last DATA event
	closeOnce sync.Once
	done      bool
}

// Read implements io.Reader. Blocks on stream.Recv until DATA arrives,
// fills p, and saves any surplus in r.buf for the next call. Returns
// io.EOF when END_STREAM or EventTrailers is observed.
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
				r.buf = ev.Data[n:] // ev.Data is deep-copied by conn layer
			}
			if ev.EndStream {
				r.done = true
				if n == len(ev.Data) {
					return n, io.EOF
				}
			}
			return n, nil
		case conn.EventTrailers:
			if r.resp != nil {
				r.resp.Trailers = append(r.resp.Trailers[:0], ev.Headers...)
			}
			r.done = true
			return 0, io.EOF
		case conn.EventReset:
			r.done = true
			return 0, &StreamResetError{Code: ev.RSTCode}
		case conn.EventHeaders:
			continue // spurious mid-stream HEADERS; skip
		}
	}
}

// Close releases the stream and returns the conn to the pool. Sends
// RST_STREAM(CANCEL) when the body has not been fully drained.
// Idempotent.
func (r *responseBodyReader) Close() error {
	var err error
	r.closeOnce.Do(func() {
		err = r.stream.Close()
		r.release()
	})
	return err
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./client/
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add client/body.go
git commit -m "feat(client): D.2 responseBodyReader (client/body.go)"
```

---

## Task 3: `Request.StreamBody` + `Response.BodyReader` + `do()` refactor

**Files:**
- Modify: `client/request.go` (add StreamBody bool)
- Modify: `client/response.go` (add BodyReader field, update Reset)
- Modify: `client/client.go` (refactor do())
- Test: `client/integration_test.go`

**Context:** `do()` currently uses `defer release()` (line ~217). For the StreamBody path, `release()` must NOT fire when `do()` returns — it must fire inside `BodyReader.Close()`. Remove the defer and replace with explicit `release()` on every error path and on the non-StreamBody success path. The StreamBody branch mirrors `doStream` (lines 295-351): receive one HEADERS event, populate resp, store stream + release in BodyReader.

The complete replacement for `do()` is shown in Step 5 below.

- [ ] **Step 1: Write the failing integration tests**

Add to `client/integration_test.go`:

```go
func TestIntegration_Client_StreamBody_Small(t *testing.T) {
	want := []byte("hello stream")
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(want)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if res.BodyReader == nil {
		t.Fatal("BodyReader is nil")
	}
	got, err := io.ReadAll(res.BodyReader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestIntegration_Client_StreamBody_Large(t *testing.T) {
	want := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(want)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	n, err := io.Copy(io.Discard, res.BodyReader)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n != int64(len(want)) {
		t.Fatalf("read %d bytes, want %d", n, len(want))
	}
}

func TestIntegration_Client_StreamBody_CloseEarly(t *testing.T) {
	// Server writes a large body; client closes early. No panic/leak.
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(bytes.Repeat([]byte("z"), 64*1024))
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	// Read one byte only, then close early.
	buf := make([]byte, 1)
	if _, err := res.BodyReader.Read(buf); err != nil && err != io.EOF {
		t.Fatalf("Read: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestIntegration_Client_StreamBody_ResetForgot(t *testing.T) {
	// Caller forgets to Close; Reset() must not leak.
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("abc"))
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	res.Reset() // must call BodyReader.Close() internally; no panic/goroutine leak
}
```

- [ ] **Step 2: Run the failing tests to confirm they fail**

```bash
go test ./client/ -run 'TestIntegration_Client_StreamBody' -v -count=1
```

Expected: FAIL — `StreamBody` field does not exist yet (compile error).

- [ ] **Step 3: Add `StreamBody bool` to `Request`**

In `client/request.go`, after the `WantTrailers bool` field:

```go
// StreamBody, when true, causes Do to return after the response HEADERS
// frame arrives. The body is available via Response.BodyReader.
// Caller MUST call Response.BodyReader.Close() (or Response.Reset())
// before the next Do call. WantBody is ignored when StreamBody is true.
StreamBody bool
```

- [ ] **Step 4: Add `BodyReader io.ReadCloser` to `Response` and update `Reset`**

In `client/response.go`, add the field to `Response` (after `BytesReceived`):

```go
// BodyReader is non-nil when the request had StreamBody=true.
// Caller reads body bytes then calls Close(). Trailers (if any) are
// written into Response.Trailers just before Close returns io.EOF.
// Reset() calls Close() automatically when BodyReader is non-nil.
BodyReader io.ReadCloser
```

Add `"io"` to the import block in `client/response.go`.

Replace `Reset()`:

```go
// Reset clears all exported fields for reuse, retaining backing arrays.
// Any references to Headers[i].Name / .Value / Body / Trailers bytes
// must not be used after Reset returns. If BodyReader is non-nil, Reset
// calls Close() before clearing it — safe even if the caller forgets.
func (r *Response) Reset() {
	if r.BodyReader != nil {
		_ = r.BodyReader.Close()
		r.BodyReader = nil
	}
	for _, sp := range r.slabs {
		*sp = (*sp)[:0]
		conn.HeaderSlabPool.Put(sp)
	}
	r.slabs = r.slabs[:0]
	r.Status = 0
	r.Headers = r.Headers[:0]
	r.Body = r.Body[:0]
	r.Trailers = r.Trailers[:0]
	r.BytesReceived = 0
}
```

- [ ] **Step 5: Refactor `do()` to handle StreamBody**

Replace the entire `do()` function in `client/client.go` (currently lines ~211-243). Remove `defer release()` and add explicit `release()` calls. Add the StreamBody branch:

```go
// do is the inner request transport, without hook/metric wrapping.
func (c *Client) do(ctx context.Context, req *Request, resp *Response) error {
	cn, release, err := c.tr.acquire(ctx)
	if err != nil {
		return err
	}
	// No defer release() — managed explicitly so StreamBody can defer
	// it into BodyReader.Close().

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

	if req.StreamBody {
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
		n, perr := parseStatus(ev.Headers, &resp.Headers)
		if perr != nil {
			_ = s.Close()
			release()
			return perr
		}
		resp.Status = n
		if ev.Slab != nil {
			resp.slabs = append(resp.slabs, ev.Slab)
		}
		resp.BodyReader = &responseBodyReader{
			ctx:     ctx,
			stream:  s,
			release: release,
			resp:    resp,
		}
		return nil // release deferred to resp.BodyReader.Close()
	}

	err = drainResponse(ctx, s, req, resp)
	_ = s.Close()
	release()
	return err
}
```

- [ ] **Step 6: Run the StreamBody tests**

```bash
go test ./client/ -run 'TestIntegration_Client_StreamBody' -v -count=1 -race
```

Expected: all StreamBody tests PASS.

- [ ] **Step 7: Run full test suite**

```bash
go test -race -count=1 ./...
```

Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add client/request.go client/response.go client/client.go client/integration_test.go
git commit -m "feat(client): D.2 StreamBody + Response.BodyReader + do() refactor"
```

---

## Task 4: Conformance test + RFC_COVERAGE.md + BENCH_BASELINE.md fix

**Files:**
- Modify: `client/conformance_test.go`
- Modify: `docs/RFC_COVERAGE.md`
- Modify: `docs/BENCH_BASELINE.md`

**Context:** RFC 7540 §8.1 governs request/response message exchange. The key conformance property for streaming response bodies: the final DATA frame MUST carry END_STREAM. RFC_COVERAGE.md must have a row for this. BENCH_BASELINE.md has a stale note claiming `BenchmarkHuffmanDecode_path ~1950 ns/op (linear walk; FSM optimisation deferred)` — the FSM was implemented and the bench now runs at ~45 ns/op.

- [ ] **Step 1: Add conformance test**

In `client/conformance_test.go`, add:

```go
// TestConformance_RFC7540_Sec8_1_StreamBody_EndStream verifies that
// the final DATA frame in a streaming response carries END_STREAM=1,
// satisfying RFC 7540 §8.1 half-close semantics.
func TestConformance_RFC7540_Sec8_1_StreamBody_EndStream(t *testing.T) {
	payload := []byte("conformance body")
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(payload)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	got, err := io.ReadAll(res.BodyReader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("body = %q, want %q", got, payload)
	}
	// Close() returned without error — stream ended cleanly (END_STREAM
	// received, no RST_STREAM sent). This confirms §8.1 half-close.
}
```

If `conformance_test.go` is in package `client_test`, ensure `newTLSH2Server` and `clientFor` are accessible (they're defined in `integration_test.go` in the same package). Also ensure `bytes` and `io` are imported.

- [ ] **Step 2: Run the conformance test**

```bash
go test ./client/ -run TestConformance_RFC7540_Sec8_1_StreamBody_EndStream -v -count=1
```

Expected: PASS.

- [ ] **Step 3: Update `docs/RFC_COVERAGE.md`**

In the RFC 7540 table, add a row for §8.1 after existing §8.x rows (or in numeric order):

```markdown
| §8.1    | Conformance | TestConformance_RFC7540_Sec8_1_StreamBody_EndStream |
| §8.1    | Integration | TestIntegration_Client_StreamBody_Small, TestIntegration_Client_StreamBody_Large, TestIntegration_Client_StreamBody_CloseEarly |
```

- [ ] **Step 4: Fix stale Huffman note in `docs/BENCH_BASELINE.md`**

Find the line:

```
BenchmarkHuffmanDecode_path                ~1950 ns/op 0 B/op   0 allocs/op  (linear walk; FSM optimisation deferred)
```

Replace with:

```
BenchmarkHuffmanDecode_path                ~45 ns/op   0 B/op   0 allocs/op
```

Find and remove the Notes bullet that describes the linear walk / FSM deferral:

```
- `BenchmarkHuffmanDecode_path` uses bit-by-bit linear walk over the canonical 257-entry table. RFC 7541 App. B FSM (4-bit nibbles) is a known optimisation deferred to a follow-up; the absolute ns/op target from the spec (~80 ns) is not yet met for this bench. The 0-allocs gate, the relative-regression gate, and all RFC vectors pass — correctness is unaffected.
```

Replace with:

```
- `BenchmarkHuffmanDecode_path` uses the 4-bit nibble FSM built from the RFC 7541 App. B canonical table (implemented in `hpack/huffman_fsm.go`). Result: ~45 ns/op, well under the 80 ns/op target. 0 allocs/op.
```

- [ ] **Step 5: Run full test suite one more time**

```bash
go test -race -count=1 ./...
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add client/conformance_test.go docs/RFC_COVERAGE.md docs/BENCH_BASELINE.md
git commit -m "test(client): D.2 §8.1 conformance + RFC_COVERAGE + BENCH_BASELINE fix"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| `Request.ContentLength int64` | Task 1 |
| `hdrContentLength` const, emitted in `buildHeaders` | Task 1 |
| `uploadBufPool` in `writeBodyReader` | Task 1 |
| `client/body.go` — `responseBodyReader` Read + Close | Task 2 |
| `Request.StreamBody bool` | Task 3 |
| `Response.BodyReader io.ReadCloser` | Task 3 |
| `Response.Reset()` safety-close | Task 3 |
| `do()` refactor — remove defer, add StreamBody branch | Task 3 |
| Conformance test §8.1 | Task 4 |
| `RFC_COVERAGE.md` §8.1 rows | Task 4 |
| `BENCH_BASELINE.md` Huffman fix | Task 4 |
| Retryer guard for `BodyReader` | Already implemented in `canRetry` — no change needed |

**Placeholder scan:** No TBD, no TODO, no "add appropriate error handling" — all code is complete.

**Type consistency check:**
- `responseBodyReader.release func()` (void, matches `doStream`'s `sr.release func()` pattern)
- `responseBodyReader.resp *Response` (matches `do()` parameter type)
- `responseBodyReader.stream *conn.Stream` (matches `conn.Stream` type returned by `cn.NewStream`)
- `resp.BodyReader = &responseBodyReader{...}` — `responseBodyReader` implements `io.ReadCloser` via `Read([]byte)(int,error)` and `Close() error` ✓
- `conn.EventData`, `conn.EventHeaders`, `conn.EventTrailers`, `conn.EventReset` — matches constants used in `drainResponse` and `StreamResponse.Recv` ✓
- `ev.RSTCode` — matches `drainResponse` usage `&StreamResetError{Code: ev.RSTCode}` ✓
- `ev.Headers` for trailers — matches `StreamResponse.Recv` case `conn.EventTrailers: Trailers: ev.Headers` ✓
