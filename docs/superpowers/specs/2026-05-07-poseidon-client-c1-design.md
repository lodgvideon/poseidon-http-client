# Poseidon HTTP/2 Client — Phase C.1 Design

**Date:** 2026-05-07
**Phase:** C.1 — Public `Client` API (sync `Do` + streaming `DoStream`)
**Status:** Draft
**Predecessor:** `docs/superpowers/specs/2026-05-05-poseidon-conn-layer-b1-design.md` (Phase B)

## Goal

Expose a high-level, ergonomic HTTP/2 client API on top of the existing
`conn` package, so load generators can issue requests without touching
streams, frames, or HPACK directly. C.1 is the **first sub-slice of
Phase C**; subsequent slices add a connection pool (C.2), service
discovery (C.3), and stats/metrics (C.4). C.1 deliberately scopes to a
single connection per `Client`, but exposes a `transport` seam so C.2
can drop in a pool implementation without changing the public surface.

## Non-Goals

- Connection pooling across multiple endpoints (C.2).
- DNS / SRV / static-target resolution (C.3).
- Metrics / tracing / latency histograms (C.4).
- Full duplex (sending request body concurrently with reading response
  body) — request body is fully written before `DoStream` returns.
- Automatic request retries on application-level errors (5xx, RST). The
  caller orchestrates request retries; the client only redials a dead
  connection between requests.
- Path templating, path-parameter substitution, query-string builders,
  or automatic percent-encoding. `Request.Path` is the raw `:path` value
  emitted on the wire; the caller formats it.
- Zero-allocation hot path for `client` package itself. The codec
  (`frame`/`hpack`) holds the existing `0 B/op` gate; `client` allocates
  per request by design (request/response structs, response body
  buffer when opted in). Optimization deferred.

## Public API Surface

### Package layout

A new top-level package `client/`, peer to `conn/`, `frame/`, `hpack/`.

```
client/
  client.go            Client struct, NewClient, Do, DoStream, Close
  request.go           Request struct + builder helpers
  response.go          Response, StreamResponse, StreamEvent, EventType
  transport.go         transport iface (unexported)
  single_conn.go       singleConn implementation (C.1 default)
  errors.go            Typed errors and error sentinels
  doc.go               Package doc comment
  client_test.go       Unit tests against fake transport / pipeServer
  integration_test.go  Tests against real net/http2.Server
```

### Top-level types

```go
package client

type Client struct {
    transport transport
    addrAuthority string // default :authority derived from ClientOptions.Addr
}

type ClientOptions struct {
    Dialer      conn.Dialer       // required (e.g., *conn.TLSDialer)
    Addr        string            // required, "host:port"
    ConnOpts    conn.ConnOptions  // forwarded to conn.Dial
    DialBackoff time.Duration     // delay before redial after failure (default 0)
}

func NewClient(opts ClientOptions) (*Client, error)
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error)
func (c *Client) DoStream(ctx context.Context, req *Request) (*StreamResponse, error)
func (c *Client) Close() error
```

`NewClient` validates `opts` (Dialer non-nil, Addr non-empty) and stores
them; it does NOT dial. The first `Do`/`DoStream` triggers a lazy dial
through the transport.

### Request

```go
type Request struct {
    // Pseudo-headers (RFC 7540 §8.1.2.3) emitted first, in this order.
    Method    string  // required, e.g. "GET", "POST"
    Scheme    string  // "https" or "http"; "" defaults to "https"
    Authority string  // ":authority"; "" derives from Client.addrAuthority
    Path      string  // required; raw request-target including query string,
                      // e.g. "/api/v1/users/123?fields=id,name"

    // Regular headers. Caller owns slice (zero-copy until SendHeaders).
    // MUST NOT include pseudo-headers; validated up front.
    Headers []hpack.HeaderField

    // Request body. At most one of Body / BodyReader is honored;
    // BodyReader takes precedence if non-nil.
    Body       []byte    // small/known body; nil = empty
    BodyReader io.Reader // streamed in DATA frames

    // Response shaping
    WantBody     bool // if false, response DATA frames consumed but dropped
    WantTrailers bool // if false, trailers ignored
}
```

Validation (in order, on the front-end of `Do`/`DoStream`):

1. `req.Method` non-empty AND contains no Unicode whitespace
   (`unicode.IsSpace`) → else `ErrInvalidRequest`. Per RFC 7230 §3.2.6
   the HTTP method is a token: leading, trailing, and internal
   whitespace are all rejected.
2. `req.Path` non-empty AND contains no Unicode whitespace → else
   `ErrInvalidRequest`. Same rationale.
3. Each `req.Headers[i].Name` MUST NOT start with `':'` → else
   `ErrInvalidRequest`.

`Authority` defaults: if empty, fall back to a value derived from
`ClientOptions.Addr` (host portion + port unless 80/443).
`Scheme` defaults to `"https"` when the dialer is TLS-backed.

**Path is raw.** `Path` is the literal `:path` request-target (RFC 7540
§8.1.2.3). It includes any query string. No templating, no path-param
substitution, no automatic percent-encoding. Callers are responsible
for building the final path (e.g. `fmt.Sprintf("/users/%d", id)`) and
encoding it. This is a deliberate non-feature: path templating
allocates and is hot-loop-hostile in load generators. Callers that
need templating layer a helper above `client`.

Body chunk size for `BodyReader`: **16 KiB read window**, then handed
to `Stream.WriteData` which chunks again at peer `MAX_FRAME_SIZE` and
respects flow control via the existing B.2.3 path.

### Response (sync, from `Do`)

```go
type Response struct {
    Status        int                  // from :status pseudo-header
    Headers       []hpack.HeaderField  // regular response headers (no pseudo)
    Body          []byte               // nil unless Request.WantBody
    Trailers      []hpack.HeaderField  // nil unless Request.WantTrailers
    BytesReceived int64                // total DATA bytes (counts even when discarded)
}
```

`Headers` / `Trailers` / `Body` are deep-copied from internal scratch
buffers before `Do` returns; safe to retain. The cost is paid only for
fields the caller opted into.

`Status` is parsed as a base-10 integer from the `:status` value;
absent or unparseable → `ErrEmptyResponse`.

### Streaming response (from `DoStream`)

```go
type StreamResponse struct {
    Status  int                  // filled after first HEADERS frame
    Headers []hpack.HeaderField  // initial response headers
    // unexported: stream ref, release fn, drained flag
}

type EventType int

const (
    EventData EventType = iota + 1
    EventTrailers
    EventReset
)

type StreamEvent struct {
    Type      EventType
    Data      []byte               // EventData; aliases scratch — copy if retained
    Trailers  []hpack.HeaderField  // EventTrailers
    ResetCode frame.ErrCode        // EventReset
    EndStream bool                 // true on the last event of the stream
}

func (sr *StreamResponse) Recv(ctx context.Context) (StreamEvent, error)
func (sr *StreamResponse) Close() error // sends RST_STREAM(CANCEL) if open; idempotent
```

`DoStream` returns once the **initial HEADERS frame** has arrived
(status known). Subsequent DATA / trailers are pulled by the caller
via `Recv`. If the caller does not drain to `EndStream`, they MUST
call `Close()` — otherwise the stream slot leaks until the underlying
connection closes. Documented loud; no `runtime.SetFinalizer`.

`StreamEvent.Data` is an alias of internal scratch (same semantics as
`conn.StreamEvent.Data`). Retain only with a manual copy.

### Errors

```go
// errors.go
var (
    ErrInvalidRequest = errors.New("client: invalid request")
    ErrClosed         = errors.New("client: closed")
    ErrRedialBackoff  = errors.New("client: redial in backoff window")
    ErrEmptyResponse  = errors.New("client: response missing :status")
)

type StreamResetError struct {
    Code frame.ErrCode
}
func (e *StreamResetError) Error() string

type DialError struct {
    Addr string
    Err  error
}
func (e *DialError) Error() string
func (e *DialError) Unwrap() error
```

Pass-through (returned as-is, no wrapping):

- `ctx.Err()` (Canceled / DeadlineExceeded)
- `conn.ErrTooManyStreams`
- `conn.ErrGoAway`
- `conn.ErrConnClosed`
- `*conn.ConnError` (typed protocol error; `errors.As` works through)

Validation errors are wrapped:
`fmt.Errorf("%w: <detail>", ErrInvalidRequest)`.

Body-reader errors are wrapped:
`fmt.Errorf("client: read request body: %w", err)`, and the stream is
reset with `INTERNAL_ERROR` before returning.

## Internal architecture

Three layers, each replaceable independently.

```
┌────────────────────────────────────────────────────────────┐
│ Client                                                      │
│   Do(ctx, *Request) (*Response, error)                      │
│   DoStream(ctx, *Request) (*StreamResponse, error)          │
└──────────────────────────┬─────────────────────────────────┘
                           │ acquire(ctx) / release()
┌──────────────────────────▼─────────────────────────────────┐
│ transport (interface, unexported)                           │
│   acquire(ctx) (*conn.Conn, release func(), error)          │
│   close() error                                             │
└──────────────────────────┬─────────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────────┐
│ singleConn (C.1)         │  pool (C.2, future)              │
│ Lazy single conn,        │  Multi-conn, per-host fanout,    │
│ conn-only auto-redial,   │  inflight-aware checkout,        │
│ backoff window.          │  GOAWAY-driven eviction.         │
└────────────────────────────────────────────────────────────┘
```

The `transport` interface is the seam. C.1 ships exactly one impl
(`singleConn`); C.2 will add `pool` against the same interface, swap
it in via `ClientOptions`, and `Client` code remains unchanged.

### `Do` flow

1. Validate `req` (see Request validation rules above).
2. `tr.acquire(ctx)` → `*conn.Conn`, `release` fn. Error → return error.
3. `c.NewStream(ctx)` → `*conn.Stream`. On error
   (`ErrTooManyStreams` / `ErrGoAway` / ctx) → call `release`, return.
4. Build header slice via a `sync.Pool[[]hpack.HeaderField]` (cap 32):
   pseudo-headers first (`:method`, `:scheme`, `:authority`, `:path`)
   in that order, then `req.Headers`.
5. `endStream` flag = no body and no `BodyReader`.
6. `s.SendHeaders(ctx, hdrs, endStream)` → on err: `release`, return.
7. If body present:
   - `Body []byte` non-nil: `s.WriteData(ctx, req.Body, true)`.
   - `BodyReader` non-nil: read 16 KiB chunks; for each chunk call
     `s.WriteData(ctx, chunk, eofReached)`. On reader error: send
     `s.Reset(INTERNAL_ERROR)`, wrap and return.
8. Drain events via `s.Recv(ctx)`:
   - `EventHeaders` (first) → parse `:status`, copy regular headers
     into `resp.Headers`.
   - `EventData` → if `WantBody`: append to `resp.Body` (sized from
     `content-length` header when present, else grown via append).
     Always `BytesReceived += len(data)`.
   - `EventTrailers` → if `WantTrailers`: copy to `resp.Trailers`.
   - `EventReset` → return `&StreamResetError{Code: ev.ResetCode}`.
   - `ev.EndStream` → break loop.
9. Reset header slice + return to pool. Call `release`.
10. Return `*Response`.

Header slice pool reuse avoids `[]hpack.HeaderField` alloc per request
on the hot path; entries inside are caller-supplied byte slices, so
nothing is mutated.

### `DoStream` flow

Steps 1–7 of `Do` (validate → acquire → NewStream → SendHeaders → body
upload).

8. Read first event from `s.Recv(ctx)`. Must be `EventHeaders`.
   - Parse `:status` and headers into `*StreamResponse`.
9. Return `&StreamResponse{stream: s, release: release, status, headers}`.
10. Caller pumps `sr.Recv(ctx)`:
    - `conn.EventData` → `client.StreamEvent{Type: EventData, ...}`.
    - `conn.EventTrailers` → `client.StreamEvent{Type: EventTrailers, ...}`.
    - `conn.EventReset` → `client.StreamEvent{Type: EventReset, ...}`,
      then subsequent calls return `&StreamResetError{}`.
    - On `ev.EndStream`: mark `sr.drained = true`. Subsequent `Recv`
      returns `io.EOF`.
11. `sr.Close()`:
    - If not drained and not reset: `s.Reset(CANCEL)`.
    - Call `release`. Idempotent (sync.Once-guarded).

### `singleConn` (C.1 transport)

```go
type singleConn struct {
    dialer   conn.Dialer
    addr     string
    connOpts conn.ConnOptions
    backoff  time.Duration

    mu         sync.Mutex
    conn       *conn.Conn
    dialErr    error      // sticky last-attempt error
    lastDialAt time.Time
    closed     bool
}
```

`acquire(ctx)` algorithm:

1. Lock.
2. If `closed` → unlock, return `ErrClosed`.
3. If `conn != nil && conn.IsAlive()` → unlock, return conn, `release`
   no-op (single-conn, nothing to return). On C.2, `release` will
   decrement an inflight counter or push back to free list.
4. `conn` is nil or not alive (closed or GOAWAY received). Set
   `conn = nil`. If
   `time.Since(lastDialAt) < backoff && dialErr != nil` → unlock,
   return `fmt.Errorf("%w: %v", ErrRedialBackoff, dialErr)`.
5. Unlock for the dial (long operation). Call
   `conn.Dial(ctx, addr, connOpts)`.
6. Re-lock. If `closed` → close fresh conn, return `ErrClosed`. If
   another goroutine raced and stored a healthy conn → close ours,
   return theirs.
7. Else store fresh conn, update `lastDialAt`, clear `dialErr`. On
   dial failure: store `dialErr`, return wrapped `&DialError{Addr,
   Err}`.
8. Unlock; return.

`close()` — sets `closed = true`, closes current conn if non-nil.

A new helper on `*conn.Conn`:

```go
// IsAlive reports true if no Close has been initiated and no peer
// GOAWAY has been received. Cheap (atomic loads).
func (c *Conn) IsAlive() bool
```

This is the only addition to the `conn` package required by C.1.

## Concurrency model

- `Client` is goroutine-safe across `Do` / `DoStream` / `Close`.
- A single `Client` may have many in-flight `Do` calls; each acquires
  the same `*conn.Conn` (since C.1 has one), and the conn already
  serializes writes via `wmu` and provides per-stream isolation on
  `Recv`.
- Multiple in-flight `DoStream` calls share the conn; each
  `StreamResponse` owns its `*conn.Stream` until `Close()`.
- `singleConn.acquire` ensures a single in-flight dial across racing
  goroutines via the lock + double-check pattern (no `singleflight`
  dependency).
- `Close()` on `Client` blocks new acquires (`ErrClosed`) and closes
  the underlying conn. In-flight `Do` returns `conn.ErrConnClosed`.

## Testing strategy

### Unit (`client_test.go`)

Against fake transport returning a `*conn.Conn` driven by `net.Pipe`,
re-using the existing `pipeServer` helper from
`conn/conn_test.go`:

- `TestClient_Do_GET_NoBody_ReturnsStatus200`
- `TestClient_Do_POST_BodyBytes_WrittenAsDATA`
- `TestClient_Do_POST_BodyReader_ChunkedDATA`
- `TestClient_Do_WantBody_True_BuffersBody`
- `TestClient_Do_WantBody_False_DiscardsButCountsBytes`
- `TestClient_Do_WantTrailers_True_CapturesTrailers`
- `TestClient_Do_WantTrailers_False_TrailersIgnored`
- `TestClient_Do_StreamReset_ReturnsTypedError` — peer sends
  `RST_STREAM(REFUSED_STREAM)` mid-response, client returns
  `*StreamResetError`.
- `TestClient_Do_InvalidRequest_NoMethod`
- `TestClient_Do_InvalidRequest_NoPath`
- `TestClient_Do_InvalidRequest_PseudoHeaderInRegular`
- `TestClient_DoStream_RecvDataChunks_ThenTrailers`
- `TestClient_DoStream_CloseBeforeEnd_SendsRSTCancel`
- `TestClient_DoStream_DrainedToEnd_RecvReturnsEOF`
- `TestSingleConn_Acquire_LazyDial_OnFirstUse`
- `TestSingleConn_Acquire_ReturnsCachedConn_WhenAlive`
- `TestSingleConn_Acquire_GoAway_TriggersRedial` — first conn drained,
  second `acquire` dials new conn.
- `TestSingleConn_Acquire_ConcurrentDial_OnlyOneSucceeds` — N
  goroutines `acquire` simultaneously on cold client; exactly one
  dial happens.
- `TestSingleConn_Backoff_RefusesDialWithinWindow` — first dial fails,
  subsequent `acquire` within `DialBackoff` returns
  `ErrRedialBackoff` without redialing.
- `TestSingleConn_Close_BlocksNewAcquires`

### Integration (`integration_test.go`)

Against `httptest.NewUnstartedServer` with `EnableHTTP2 = true` and
`server.StartTLS()`:

- `TestIntegration_Client_GET_Status200`
- `TestIntegration_Client_POST_EchoBody` — server echoes; client uses
  `WantBody=true`, asserts equality.
- `TestIntegration_Client_ConcurrentRequests_OneClient` — 32
  goroutines call `Do` simultaneously, all observe 200.
- `TestIntegration_Client_DoStream_LargeResponse` — server streams
  1 MiB body, client reads via `Recv` until EOF.
- `TestIntegration_Client_GoAwayMidFlight_NextDoRedials` — server
  sends GOAWAY after first request; second `Do` succeeds against new
  connection.

### RFC trace

C.1 mostly orchestrates existing `conn`-layer behavior. Likely
additions to `docs/RFC_COVERAGE.md`:

- §8.1.2.1 — pseudo-header ordering. New conformance test
  `TestConformance_RFC7540_Sec8_1_2_1_PseudoHeadersFirst` asserts
  that on-wire HEADERS payload places pseudo-headers before regular
  ones (decode the produced HPACK block).

### No bench gate in C.1

The codec keeps its `0 B/op` gate (Phase A). The `client` package
allocates per request by design. A targeted bench (`BenchmarkDo_GET`)
may be added for visibility but is NOT gate-enforced. Optimization
deferred to a later sub-phase.

## Migration / compatibility

C.1 is purely additive. No changes to existing `frame`, `hpack`, or
`conn` public APIs except the new `(*conn.Conn).IsAlive() bool`
method. No imports change for existing consumers.

## Open questions / deferred decisions

- **Per-request timeout field on `Request`** — currently relies on
  `ctx`. Deferred; can add a non-breaking `Request.Timeout` later
  whose handler wraps `ctx`. Not in C.1.
- **Cookie / redirect support** — out of scope for load generators.
  Not planned.
- **HTTP/3** — out of scope for the entire project.
- **gRPC convenience layer** — distinct package on top of `client`,
  later phase.
- **Optional `Request.Pool` for response body buffer reuse** — punt
  to optimization pass.

## Acceptance criteria for C.1

1. All unit tests above pass under `make test-race`.
2. All integration tests above pass under `make test-race`.
3. `make lint` clean (golangci-lint v1.64).
4. New conformance test row added to `docs/RFC_COVERAGE.md`.
5. README.md "Quick start" rewritten to use the `client` package
   (existing snippet currently uses `conn` directly).
6. `(*conn.Conn).IsAlive` documented and tested.
7. No exported symbols added to `conn` beyond `IsAlive`.
