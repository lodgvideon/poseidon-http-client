# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.5.1] — 2026-06-21

### Fixed

- **Pool reply-channel poisoning race** — the global `replyPool` recycled a
  buffered reply channel even when the pool actor could still send a late
  reply (caller abandoned via `ctx`/`closedCh` after the actor took the
  request). The recycled channel was then handed to a different `Pool`,
  which read the stale reply — surfacing as a spurious `ErrPoolClosed` or a
  cross-pool conn (`stream reset by peer`) under concurrent load. The reply
  channel is now recycled only when the actor can no longer send on it.
  Regression test: `TestPool_ReplyChannelNotPoisonedUnderAbandonment`.

- **HTTP/1.1 request trailers no longer corrupt the connection** — sending a
  request with `Trailers`/`TrailerFunc` over an H1.1 transport (explicit
  `TransportH1SingleConn` or an ALPN-negotiated H1.1 connection) previously
  re-invoked `WriteRequest`, emitting a second request line onto the live
  connection. The H1.1 transport now rejects such requests up front with the
  new `ErrTrailersUnsupportedH1` and discards the connection.
  Test: `TestNewClient_H1SingleConn_TrailersRejected`.

### Changed

- All packages now at ≥ 90% statement coverage (spec acceptance bar).

## [v0.5.0] — 2026-06-21

### Added

- **HTTP/1.1 fallback** (`http1/` package) — zero-dependency HTTP/1.1 wire
  protocol from scratch (no `net/http`). Uses `net.Buffers` (writev) for
  scatter-gather writes. Both `Content-Length` and `Transfer-Encoding: chunked`
  for request/response bodies. Automatic 1xx skip. Keep-alive from `Connection:`
  header. `HEAD`/`204`/`304` no-body fast paths. Chunk-extension stripping.

- **`TransportH1SingleConn`** — explicit H1.1 single-connection transport.
  Serializes exchanges via in-flight mutex. Dial backoff, keep-alive reuse.

- **`TransportALPN`** — ALPN-aware transport. Dials once with `conn.FlexDialer`
  (offers `h2` + `http/1.1`) and permanently routes to H2 or H1.1 based on
  negotiated protocol.

- **`conn.FlexDialer`** — TLS dialer prepending `h2` and `http/1.1` to
  `NextProtos`; returns `ErrALPNFailed` if neither is negotiated.

- **`conn.NegotiatedProtocol`** — returns ALPN protocol string from `*tls.Conn`;
  `""` for plain-TCP connections (H2C).

- **`protoStream` interface** — protocol-agnostic abstraction over
  `*conn.Stream` (H2) and `*h1Exchange` (H1.1), enabling `Client.sendRequest`
  and `drainResponse` to drive either protocol uniformly.

- **`docs/USAGE.md`** — 21-section usage guide covering all client features.

## [v0.4.0] — 2026-06-20

### Added

- **CONTINUATION write path** (RFC 7540 §6.2/§6.10) — `writeHeadersWithPriority`
  splits HPACK blocks exceeding `SETTINGS_MAX_FRAME_SIZE` into one HEADERS
  frame (END_HEADERS=0) plus N CONTINUATION frames; padding and priority only
  on the HEADERS frame; the final CONTINUATION sets END_HEADERS=1. Zero
  additional allocations. Applies to both request and trailer HEADERS.
  3 unit conformance tests + 1 integration test (50-header ~50 KiB block).
  (`15a5425`, `e24be99`)

- **Retry layer** — `client.NewRetryer(c, RetryOptions)` wraps `*Client` with
  an automatic retry loop for idempotent requests. Built-in retry on
  `REFUSED_STREAM` (RFC §8.1.4), GOAWAY, and dial errors. `IsRetryable`
  callback for caller-defined 5xx/etc. policy. `Request.Idempotent *bool`
  overrides method-based classification. Truncated-exponential backoff with
  ±25% jitter, configurable `MaxAttempts`. `DoStream` retries only before
  the first HEADERS frame arrives. 16 tests. (`7fe1552`)

- **Docker integration test infrastructure** — OpenResty (nginx + Lua) and
  Undertow (Java) servers in Docker Compose; `gen-certs.sh` auto-generates
  TLS certs; `client/integration_test/matrix_test.go` cross-server test suite
  (healthz, root, status codes, echo POST, connection reuse, concurrency,
  chunked body, large body, delay, context cancel, headers, metrics). CI
  `docker-it` workflow runs the full matrix on every PR. (`7dcac0d`, `33af9c1`)

- **Request validation** — `validateRequest` rejects missing Method, empty
  Path, BodyReader + Body conflict, Trailers + method conflict early (returns
  `ErrInvalidRequest`), before any conn is touched. (`7fe1552`)

### Fixed

- **`FramesReceived` Stats race** — counter was incremented in the read loop
  after `ReadFrame` returned, after events were dispatched. Test reading
  `Stats()` right after `Recv` could observe `FramesReceived = 0`. Fix:
  `bumpFramesReceived()` added to `connOps` interface, called at the start
  of each `On*` handler so the counter is visible as soon as the frame is
  dispatched. (`b5488dd`)

- **Undertow `/status/:code` 500** — `PathHandler` strips the `/status`
  prefix; `getRelativePath()` returns `/301` (not `/status/301`).
  `substring("/status/".length())` on a 4-char string → `StringIndexOutOfBoundsException`.
  Fixed to `substring(1)`. (`b5488dd`)

- **nginx `/echo` returns empty body** — `echo_duplicate 1 $request_body`
  requires the echo module to buffer the body first; under OpenResty with
  HTTP/2 the variable stays empty. Replaced with a Lua block that calls
  `ngx.req.read_body()` before printing. (`e45e524`)

- **Silent consumer hang on stream event-channel overflow** — when the
  8-slot `Stream.events` channel filled (fast server, slow consumer), `push`
  silently dropped the event and sent `RST_STREAM(REFUSED_STREAM)`. Consumer
  blocked in `Recv` forever. Fixed by non-blocking send + RST + close channel.
  (`2389f55`)

- **Flow-control hang on large body** — `TestIntegration_Client_StreamBody_Large`
  raced: server filled the default 8-slot event buffer before the consumer
  goroutine started, triggering the overflow path above. Fixed by using
  `StreamEventBuffer: 128` for that test. (`ebbfa4a`)

- **55 lint issues** — golangci-lint v2.5 clean: removed dead code
  (`encIdentity`, `pooledZlibReader`, `drainResponse` in push_test), fixed
  unchecked `Close()` returns in proxy.go, added doc comments, removed
  redundant `int32()` cast, extracted `handleHeadersEvent`/`handleDataEvent`
  to reduce `drainResponse` cyclomatic complexity, added `gosec` to
  test-file exclusion (eliminates G104 false-positives). (`ebbfa4a`)

- **Dockerfile.undertow baked-in certs** — `COPY fixtures/certs /app/certs`
  ran at image build time (before `gen-certs.sh`), causing CI failures.
  Removed; docker-compose already mounts `./fixtures/certs:/app/certs:ro`.
  (`1960ca2`)

- **Security — P0/P1 pre-production audit** — closed all P0 and P1 findings
  from the pre-production security review. (`e705532`)

### Performance

- **WriteHeaders single syscall** — coalesced the previously split header
  write into one `Write` call, removing a system-call boundary. −15% latency
  on `BenchmarkClient_DoParallel`. (`f32a062`)

- **−2 allocs/op on request path** — removed buildHeaders closure escape
  (−1 alloc) and replaced `*sentRequest` return with multi-value return
  (−1 alloc). (`9618fa7`, `f0b769a`)

### Diff

26 commits since v0.3.0. 19 files changed in PRs #31 and #32.

---

## [v0.3.0] — 2026-06-15

### Added

- **Automatic response body decompression** (gzip/deflate) — `Request.DisableDecompression`
  opt-out, auto-injected `accept-encoding: gzip` (preserved when caller
  supplies one), `decompressFully` for batch path, `decompressingReader`
  for streaming path, `gzipReaderPool` for reader reuse.
  `Response.BytesReceived` = wire bytes; `Response.Body` = decompressed.
  10 tests. (`a3338da`)

- **Priority hints** (RFC 7540 §5.3) — `Request.Priority *frame.Priority`
  embeds a 5-byte priority block (E + StreamDep + Weight) in the first
  HEADERS frame. `Stream.SendHeadersWithPriority` carries the PRIORITY
  flag. Backward compatible: nil priority → original SendHeaders
  behavior. 4 frame tests + 3 client tests. (`6dd5148`)

- **Graceful shutdown** (RFC 7540 §6.8) — `Conn.Shutdown(gracefulTimeout)`
  sends GOAWAY(lastClientStreamID, NO_ERROR), marks the conn as
  draining (NewStream returns `ErrConnDraining`), then waits up to
  the timeout for in-flight streams to drain. `Client.Shutdown(timeout)`
  proxies the request down to the underlying *conn.Conn (single-conn
  transport). Pool transports close all conns in parallel.
  `markStreamDone` closes a wake-up channel when inflight hits zero.
  4 conn tests + 3 client tests. (`9a5c1f8`)

- **Client.Warmup(n)** — pre-dial up to n conns in the background to
  avoid TLS handshake + HTTP/2 setup on the first request. n is
  capped at MaxConnsPerHost (1 for single-conn). Pool transport
  fan-outs across the live set. Idempotent. 4 tests.
  (`24be6f8`)

- **Client-side rate limiting** (token-bucket) —
  `ClientOptions.RateLimitPerSecond` (float) gates Do/DoStream via
  an internal token bucket. Take respects ctx cancellation. 5 tests
  (4 unit + 1 integration). (`0fb9dd5`)

- **Per-request timeout** — `Request.Timeout time.Duration` derives
  a sub-context from the parent ctx with the given deadline. When
  the timeout fires the request fails with
  `context.DeadlineExceeded` and the in-flight stream is reset.
  Applies to both Do and DoStream. Zero = use parent ctx. 4 tests.
  (`0fb9dd5`)

- **ClientOptions.RateLimitBurst** — separate burst capacity
  decoupled from steady-state RPS. Zero (default) falls back to
  `RateLimitPerSecond` for backward compatibility. (`809533c`)

### Fixed

- **TestWarmup_Pool_CappedByMaxConns** asserted `ActiveConns <= 2`
  only, which is also satisfied by a no-op warmup. Test now wires
  `countingDialer` and asserts `dialCount >= 1` (warmup actually
  ran) plus `dialCount <= MaxConnsPerHost` (cap honored). (`462c179`)

- **`rateLimiter` doc comment claimed "lock-free on the hot path"**
  while `Take` actually takes `rl.mu.Lock()` on the first line.
  Replaced with "goroutine-safe (sync.Mutex + Cond)". No code change.
  (`462c179`)

- **Goroutine leak in `singleConn.warmup`** — the 30s background
  dial context was created and `defer cancel()`-ed inside the
  goroutine, with no external handle. `Warmup(1) → Close()` left
  the goroutine alive for the full timeout. Fix caches
  `warmupCancel` on `singleConn`; `close()` and `shutdown()` call
  it. Repeated `warmup()` calls reuse the in-flight context. (`462c179`)

- **`TestClient_RateLimit_BlocksExcess` and warmup tests** had
  magic-number deadlines (`400ms`, `2s`, `3s`) chosen "by feel".
  Replaced with derived values: `expectedMin = (need-burst)/rps - slack`
  and `maxConns * dialPerBudget + slack`. Tweak parameters, not
  wall-clock guesses. (`809533c`)

### Docs

- **Self-review action plan** — post-sprint audit found 3 fake-green
  / false-claim / leak defects in v0.3.0 sprint tests. 7 items
  tracked in `docs/SELF_REVIEW_2026-06-15.md`. (`878e8d4`)

- **Self-review close** — all 7 items resolved; validation table
  documents the gate (`make test-fast`, count=20 stress, lint
  baseline unchanged). (`a3b4938`)

## [v0.2.0] — 2026-06-14

### Added

- **HTTP/1.1 CONNECT proxy support** — `ProxyDialer` and `ProxyTLSDialer`
  for tunneling HTTP/2 traffic through an HTTP/1.1 CONNECT proxy.
  Includes proxy auth, custom headers, and TLS to proxy. (`56be170`)

- **Frame padding** (RFC 7540 §4.2) — `PaddingStrategy` struct with
  `ForHeaders()` and `ForData()` methods. `WriteDataPadded` /
  `WriteHeadersPadded` emit PADDED frames with random-length padding
  for traffic-fingerprint resistance. (`f5df543`)

- **Server Push — connection layer** (RFC 7540 §8.2) —
  `ConnOptions.EnablePush`, `EventPushPromise` stream event with
  `PushStreamID`, `OnPushPromise` handler registers pushed streams and
  decodes promised request headers. Peer PUSH_PROMISE rejected with
  PROTOCOL_ERROR when push is disabled. 2 conn-layer tests. (`76dc45d`)

- **Server Push — client callback API** (RFC 7540 §8.2) —
  `ClientOptions.PushHandler` = `func(ctx, promisedHeaders, *Response, error)`.
  Auto-sets `ConnOpts.EnablePush = true` when non-nil. `drainPushedStream`
  goroutine drains pushed response and invokes handler with ready
  `*Response`. `Conn.LookupStream()` public method. Nested pushes
  supported. 2 integration tests. (`cd9fcd0`)

- **ORIGIN frame + connection coalescing API** (RFC 8336) —
  `FrameOrigin` (type 0x0c) with TLV parsing, stream-0 enforcement,
  malformed-frame detection. `Conn.Origins()` and `Conn.CanCoalesce(origin)`
  public API. 5 frame tests + 5 conn tests. (`e65cb3a`)

- **ALTSVC frame** (RFC 7838) — `FrameAltSvc` (type 0x0a)
  with TLV parsing, `AltSvcEntry` struct (Origin + AltValue),
  `Framer.WriteAltSvc`, `Conn.AltSvcEntries()`. Server-wide and
  per-stream variants. Empty payload clears all alt-svc entries.
  6 tests (3 roundtrip + 3 negative). (`a65c5a7`)

- **Extended CONNECT protocol** (RFC 8441) —
  `SettingEnableConnectProtocol` (0x8) setting ID.
  `Conn.ConnectProtocolSupported()` checks peer advertisement.
  `Request.Protocol` field emits `:protocol` pseudo-header in
  `buildHeaders` for WebSocket/extended CONNECT semantics. 6 tests
  (3 conn + 3 client). (`d360e12`)

### Changed

- **Code review fixes** — 3 BLOCKER + 11 WARNING + 8 INFO findings
  resolved: `parseStatus` alloc elimination, `sendRequest` extraction,
  slab leak fix, hash alloc reduction, `Unwrap` consistency,
  direct frame/hpack import removal from client package.

### Fixed

- **nil-panic** on `StreamBody` with nil `Response` (`941d6b4`)
- **Slab leak** in client request path (`8a3d261`)

### Diff

44 files changed, +4,109 / −283 lines.

---

## [v0.1.0] — 2026-05-12

Initial release. Full HTTP/2 + HPACK codec from scratch, no
`net/http` or `golang.org/x/net/http2` dependencies.

### Features

- **Phase A** — Frame layer + HPACK codec (RFC 7540 + 7541)
- **Phase B.1–B.2.6** — Connection layer: TLS+ALPN dial, SETTINGS
  handshake, multi-stream, bidirectional flow control, dynamic
  SETTINGS, MAX_CONCURRENT_STREAMS gate, GOAWAY drain, PING ACK
- **Phase C.1** — Public client API: `Client`, `Request`, `Response`,
  `Do`/`DoStream`
- **Phase C.2** — Per-host connection pool with lazy-grow, idle
  eviction, GOAWAY handling
- **Phase C.3** — Service discovery: managed pool with resolver-based
  address fan-out, round-robin / random / consistent-hash selectors
- **Phase C.4** — Metrics & hooks: `Counters`, lock-free `Histogram`,
  lifecycle `Hooks` (OnRequestStart/Complete, OnRetry, OnDial,
  OnConnClose, OnResolverUpdate)
- **Phase D.1** — Zero-alloc request path (33 allocs/op, down from 49)
- **Phase D.2** — Request/response body streaming (`StreamBody`,
  `BodyReader`, `ContentLength`)
- **Phase D.3** — H2C (plaintext HTTP/2) via `PlaintextDialer`
- **Phase D.4** — PING / keepalive (`Conn.Ping(ctx)`,
  `KeepaliveInterval`)
- **Phase D.5** — HTTP request trailers (`Request.Trailers` /
  `Request.TrailerFunc`, `StreamResponse.WaitTrailers`)

39 E2E + stress tests. Bench-gate enforced: frame + hpack = 0 B/op,
0 allocs/op.
