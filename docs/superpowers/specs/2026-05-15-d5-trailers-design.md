# D.5 — HTTP Trailers Design

**Date:** 2026-05-15
**Status:** Approved
**Goal:** Add request trailer sending (`Request.Trailers` / `TrailerFunc`) and a
`StreamResponse.WaitTrailers` convenience method for response trailers.
Response trailer *receiving* already exists (`Response.Trailers`, `Request.WantTrailers`,
`body.go` routing); D.5 fills the two remaining gaps.

---

## Background

`Response.Trailers []hpack.HeaderField`, `Request.WantTrailers`, and
`StreamResponse.Recv` delivering `EventTrailers` are all implemented and
working. What is missing:

1. **Receiving side**: no convenience method to block until trailers arrive in
   a streaming response (`DoStream`). Callers must pump `Recv` manually.
2. **Sending side**: no way to send request trailers. HTTP/2 trailers are a
   HEADERS frame with `END_STREAM` sent after the body DATA frames (RFC 7540 §8.1.3).

---

## Architecture

```
Sending (client → server)
  Request.Trailers []hpack.HeaderField   ← static trailer fields
  Request.TrailerFunc func() []hpack.HeaderField  ← dynamic (wins if non-nil)

  do() / doStream():
    hasTrailers(req) = len(Trailers)>0 || TrailerFunc != nil
    endStream(initial HEADERS) = noBody && !hasTrailers
    writeRequestBody(..., endStream = !hasTrailers)
    writeRequestTrailers(ctx, s, req)  ← resolves + validates + SendHeaders(true)

Receiving (server → client)
  StreamResponse.trailers  ← cached when Recv sees EventTrailers
  StreamResponse.WaitTrailers(ctx) ← returns cached or pumps Recv until trailer/end
```

---

## Section 1: `StreamResponse.WaitTrailers`

**File:** `client/response.go`

New field on `StreamResponse`:
```go
trailers []hpack.HeaderField // cached when Recv delivers EventTrailers
```

Update `Recv` to cache when `EventTrailers` arrives:
```go
case conn.EventTrailers:
    out := StreamEvent{...}
    sr.trailers = out.Trailers  // cache for WaitTrailers
    ...
    return out, nil
```

New method:
```go
// WaitTrailers pumps Recv, discarding any remaining EventData events,
// until EventTrailers arrives or the stream ends. Returns the trailer
// fields and nil on success. Returns nil, nil when the server sent no
// trailers (stream ended without EventTrailers). Returns nil, ctx.Err()
// when the context is cancelled or times out.
//
// Safe to call after the caller has already drained all EventData via
// Recv. If called before body is fully drained, remaining EventData
// events are silently discarded — use this path only when body content
// is not needed.
//
// Caching: if Recv already delivered EventTrailers, WaitTrailers returns
// the cached result immediately without further network I/O.
func (sr *StreamResponse) WaitTrailers(ctx context.Context) ([]hpack.HeaderField, error) {
    if sr.trailers != nil {
        return sr.trailers, nil
    }
    for {
        ev, err := sr.Recv(ctx)
        if err == ErrStreamEnded {
            return nil, nil // stream ended, no trailers
        }
        if err != nil {
            return nil, err
        }
        switch ev.Type {
        case EventData:
            // Discard; documented behaviour.
            continue
        case EventTrailers:
            return ev.Trailers, nil // also cached in sr.trailers by Recv
        case EventReset:
            return nil, nil
        }
    }
}
```

Update `reset()` to clear cache:
```go
sr.trailers = nil
```

---

## Section 2: `Request.Trailers` / `TrailerFunc`

**File:** `client/request.go`

Add to `Request`:
```go
// Trailers are sent as a HEADERS+END_STREAM frame after the request
// body. Ignored when hasTrailers returns false. MUST NOT contain
// pseudo-headers (names starting with ':') — validated by validateRequest.
Trailers []hpack.HeaderField

// TrailerFunc, when non-nil, is called after the full body is sent.
// Its return value replaces Trailers; if it returns nil, Trailers is
// used as fallback. Must be idempotent: the retry layer may call Do
// (and therefore TrailerFunc) multiple times. Ignored for BodyReader
// requests, which are already non-retryable.
// MUST NOT return pseudo-headers — validated before sending.
TrailerFunc func() []hpack.HeaderField
```

Update `validateRequest`:
```go
// Validate Trailers for pseudo-headers (RFC 7540 §8.1.2 forbids them
// in trailers).
for i := range r.Trailers {
    if len(r.Trailers[i].Name) > 0 && r.Trailers[i].Name[0] == ':' {
        return fmt.Errorf("%w: pseudo-header %q in Trailers slice",
            ErrInvalidRequest, r.Trailers[i].Name)
    }
}
```

---

## Section 3: Wire logic

**File:** `client/client.go`

### Helpers

```go
// hasTrailers reports whether req will send a trailer HEADERS frame.
// Computed statically before body send; TrailerFunc result validated at send time.
func hasTrailers(req *Request) bool {
    return len(req.Trailers) > 0 || req.TrailerFunc != nil
}

// resolveTrailers returns the trailer fields to send.
// TrailerFunc wins; falls back to Trailers when TrailerFunc returns nil.
// Returns error if resolved fields contain pseudo-headers.
func resolveTrailers(req *Request) ([]hpack.HeaderField, error) {
    fields := req.Trailers
    if req.TrailerFunc != nil {
        if result := req.TrailerFunc(); result != nil {
            fields = result
        }
    }
    for i := range fields {
        if len(fields[i].Name) > 0 && fields[i].Name[0] == ':' {
            return nil, fmt.Errorf("%w: pseudo-header %q in trailer",
                ErrInvalidRequest, fields[i].Name)
        }
    }
    return fields, nil
}

// writeRequestTrailers resolves and sends the trailer HEADERS frame.
// TrailerFunc is called BEFORE acquiring wmu (inside SendHeaders) to
// avoid holding the write lock during user code.
func writeRequestTrailers(ctx context.Context, s *conn.Stream, req *Request) error {
    fields, err := resolveTrailers(req)
    if err != nil {
        return err
    }
    return s.SendHeaders(ctx, fields, true)
}
```

### `writeRequestBody` — add `endStream` parameter

```go
// writeRequestBody writes Body or BodyReader as DATA frames.
// endStream controls whether the final DATA frame sets END_STREAM.
// Pass false when trailer HEADERS will follow.
func writeRequestBody(ctx context.Context, s *conn.Stream, req *Request, endStream bool) error {
    if req.BodyReader != nil {
        return writeBodyReader(ctx, s, req.BodyReader, endStream)
    }
    if len(req.Body) == 0 {
        // No body content; send zero-byte DATA only when no trailers follow
        // (endStream=true). When endStream=false, skip DATA entirely —
        // the caller will send a trailer HEADERS frame next.
        if !endStream {
            return nil
        }
        return s.SendData(ctx, nil, true)
    }
    return s.SendData(ctx, req.Body, endStream)
}
```

```go
// writeBodyReader streams an io.Reader into DATA frames.
// endStream controls whether the final DATA frame sets END_STREAM.
func writeBodyReader(ctx context.Context, s *conn.Stream, r io.Reader, endStream bool) error {
    bufp := uploadBufPool.Get().(*[]byte)
    defer uploadBufPool.Put(bufp)
    buf := *bufp
    for {
        n, rerr := r.Read(buf)
        if n > 0 {
            final := rerr == io.EOF && endStream
            if werr := s.SendData(ctx, buf[:n], final); werr != nil {
                return werr
            }
            if rerr == io.EOF {
                return nil
            }
        }
        if rerr == io.EOF {
            if endStream {
                return s.SendData(ctx, nil, true)
            }
            return nil // trailers follow; caller sends END_STREAM via HEADERS
        }
        if rerr != nil {
            return fmt.Errorf("client: read request body: %w", rerr)
        }
    }
}
```

### `do()` changes

```go
hdrs, putHdrs := buildHeaders(req, c.authority, c.defaultScheme)
trailers := hasTrailers(req)
endStream := len(req.Body) == 0 && req.BodyReader == nil && !trailers
if err := s.SendHeaders(ctx, hdrs, endStream); err != nil { ... }
putHdrs()

if !endStream || trailers {
    // Body present, or trailers present with no body (HEADERS→HEADERS).
    if err := writeRequestBody(ctx, s, req, !trailers); err != nil { ... }
    if trailers {
        if err := writeRequestTrailers(ctx, s, req); err != nil { ... }
    }
}
```

### `doStream()` changes

Identical pattern as `do()` above (same three-step: body with endStream=false, trailer HEADERS).

---

## Section 4: No-body + trailers edge case

When `len(req.Body) == 0 && req.BodyReader == nil && hasTrailers(req)`:

1. Initial HEADERS: `endStream=false`
2. `writeRequestBody(ctx, s, req, false)` → no DATA frame sent (guard: `!endStream` skip)
3. `writeRequestTrailers(ctx, s, req)` → HEADERS+END_STREAM

Wire sequence: `HEADERS → HEADERS(END_STREAM)` — valid per RFC 7540 §8.1.3.

---

## Section 5: Retry semantics

- `TrailerFunc` called once per `do()` invocation. On retry, called again.
- **Requirement**: `TrailerFunc` must be idempotent (safe to call N times).
- `BodyReader + TrailerFunc`: `BodyReader != nil` already makes request
  non-retryable (existing check at `retry.go:149`). No change needed.
- `Body []byte + TrailerFunc`: retryable. Caller must ensure idempotency.

---

## Section 6: Tests

**File:** `client/trailer_test.go`

| Test | Path | What it pins |
|---|---|---|
| `TestDo_RequestTrailers_Static` | `do()` | `Trailers` field → HEADERS+END_STREAM after body; server reads them |
| `TestDo_RequestTrailers_Func` | `do()` | `TrailerFunc` result wins over `Trailers` |
| `TestDo_RequestTrailers_FuncNilFallback` | `do()` | `TrailerFunc` returns nil → `Trailers` used |
| `TestDo_RequestTrailers_NoBody` | `do()` | no body + trailers: HEADERS→HEADERS wire sequence |
| `TestDo_RequestTrailers_PseudoHeader` | `do()` | `Trailers` with `:name` → `ErrInvalidRequest` before send |
| `TestDo_RequestTrailers_FuncPseudoHeader` | `do()` | `TrailerFunc` returns `:name` → error after call |
| `TestDoStream_RequestTrailers` | `doStream()` | request trailers via streaming path; server receives |
| `TestDo_ResponseTrailers` | `do()` | `WantTrailers=true`; server sends trailers → `resp.Trailers` populated |
| `TestDoStream_WaitTrailers_AfterDrain` | `doStream()` | drain body via `Recv`, then `WaitTrailers` returns cached trailers |
| `TestDoStream_WaitTrailers_CachedFromRecv` | `doStream()` | `Recv` delivers `EventTrailers`; subsequent `WaitTrailers` returns cached |
| `TestDoStream_WaitTrailers_None` | `doStream()` | server sends no trailers → `WaitTrailers` returns nil, nil |
| `TestDoStream_WaitTrailers_Discard` | `doStream()` | `WaitTrailers` before body drain: EventData discarded, trailers returned |

---

## Section 7: File changes

```
Modified:
  client/request.go   — Trailers, TrailerFunc fields; validateRequest pseudo-header check
  client/client.go    — hasTrailers(), resolveTrailers(), writeRequestTrailers(),
                        writeRequestBody(endStream param), writeBodyReader(endStream param),
                        do() and doStream() endStream recalc + trailer send
  client/response.go  — trailers cache field on StreamResponse, WaitTrailers method,
                        Recv caches EventTrailers, reset() clears cache

New:
  client/trailer_test.go — 12 integration tests (listed above)
```

---

## FMEA mitigations included

| ID | Fix |
|---|---|
| F1 | `WaitTrailers` discards data when called early — documented explicitly; caching prevents double-process |
| F6 | `validateRequest` checks `Trailers` for pseudo-headers |
| F8 | `TrailerFunc` nil result → fallback to `Trailers` (in `resolveTrailers`) |
| F9 | `resolveTrailers` validates `TrailerFunc()` result for pseudo-headers |
| F11 | Both `do()` and `doStream()` updated; both have integration tests |
| F12 | No-body + trailers: explicit `writeRequestBody` guard skips DATA, sends trailer HEADERS |
| F14 | `TrailerFunc` called before `wmu` (inside `s.SendHeaders` ONLY after `resolveTrailers`) |
| F15 | `TrailerFunc` called once per attempt; documented idempotency requirement |

---

## Non-goals

- Response trailer type conversion to `map[string]string` — callers use `[]hpack.HeaderField` throughout
- PING rate limiting from keepalive — handled in D.4
- Request trailer sending from the pool keepalive — out of scope
- Server push trailer handling — server push rejected at protocol level
