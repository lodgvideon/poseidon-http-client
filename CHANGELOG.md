# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
