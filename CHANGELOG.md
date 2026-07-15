# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.0.0] — 2026-07-15

The first stable release: HTTP/1.1, HTTP/2, and HTTP/3 through one client API,
on a from-scratch, near-zero-dependency stack.

### Added

- **HTTP/3 as a first-class transport in `client.Client`.** The same `Do` /
  `DoStream` API drives HTTP/3 over QUIC: transports `TransportH3`,
  `TransportH3Pool`, `TransportH3Managed`, and the constructors `NewH3Client`,
  `NewH3PoolClient`, `NewManagedH3Client`. Concurrent in-flight requests over one
  QUIC connection, connection pooling, service discovery, streaming, retries,
  hooks, and metrics now work over HTTP/3, not only HTTP/2.
- **Dynamic QPACK (RFC 9204), both directions** — encode and decode with the
  dynamic table, encoder/decoder instruction streams, known-received-count, and
  blocked-streams handling (#193–#196, #202).
- **ChaCha20-Poly1305 packet protection (RFC 9001 §5.3 / §5.4.4)** — handshakes
  complete against ChaCha20-only servers; verified byte-for-byte against the
  RFC 9001 §A.5 test vector and on the wire (#204).
- **BBR congestion control (opt-in)** via
  `ClientOptions.H3ConnOptions = []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)}`;
  NewReno stays the default (#203, #206).
- **Batched UDP I/O on Linux** — GSO send, GRO receive, and bounded ACK deferral
  (#199–#201).
- A documentation site (Hugo → GitHub Pages) in English, 中文, Español, Русский,
  and 日本語, plus runnable `examples/http1`, `examples/http2`, `examples/http3`.

### Changed

- **Minimum Go version is now 1.25** — a supported release that receives security
  backports.
- New direct dependency `golang.org/x/crypto` (ChaCha20-Poly1305 only). Still no
  `quic-go`, `nghttp2`, `net/http`, or cgo.
- README rewritten; per-protocol guides and an accurate support matrix replace the
  earlier single-request/HTTP-2-only description.

### Security

- **CI security scanning** — `govulncheck` (Go vulnerability database, reachable
  symbols) and CodeQL, plus Dependabot for Go modules and GitHub Actions.
- **Suite-aware AEAD usage limits (RFC 9001 §6.6)** — ChaCha20-Poly1305 uses its
  mandated 2^36 integrity limit rather than AES-GCM's 2^52.
- **Bounded HTTP/1.1 response reads.** A peer that never sends a line terminator
  could make the client read until it ran out of memory. Status lines, header
  lines, and chunk-size lines are now read within the existing 16 KiB connection
  buffer; the header block is capped at 8 MiB (the same limit HTTP/2 advertises
  as `MaxHeaderListSize`), and interim 1xx responses and trailers are bounded.
  Over-limit responses fail with the new `http1.ErrResponseTooLarge`, mirroring
  `http3.ErrResponseTooLarge`. Header names are lowercased as ASCII per
  RFC 7230 §3.2.6: `strings.ToLower` re-encodes each invalid byte as U+FFFD,
  which both corrupted the name and let a peer retain three bytes per byte sent.
- **Server push no longer corrupts the request-stream gate.** Registering a
  pushed stream never took a `MAX_CONCURRENT_STREAMS` slot, but closing one
  released a slot — so every pushed stream that closed freed a slot belonging to
  a real request. Measured: three concurrent request streams against a peer gate
  of two, and `Shutdown` returning immediately with a request still open. The two
  limits are directional and independent (RFC 7540 §5.1.2 / §6.5.2), so they are
  now two counters, each releasing to the one that issued the slot. The push
  registry is bounded by the client's own advertised
  `SETTINGS_MAX_CONCURRENT_STREAMS` — which §6.5.2 already defines as the streams
  the peer may create — with over-cap promises refused per §6.6. A promised
  stream ID is now validated (even, non-zero, increasing; §5.1.1): previously a
  PUSH_PROMISE for ID 1 overwrote the client's own stream 1. Push is opt-in
  (`EnablePush`), off by default.
- **HTTP/2 understands 1xx informational responses.** A server sending
  `103 Early Hints` before its real response — Cloudflare, Fastly and Shopify all
  do — made the client report the 103 as the final answer: `Status` was 103, the
  real 200 header block arrived as *trailers*, and `Do` returned no error. On the
  `BodyStream` path the body came back empty, also with no error. The cause was
  RFC 7540 §8.1: trailers are a header block after a **final (non-informational)**
  status, and the code treated the first block as final whatever it held.
  Informational blocks now surface as `conn.EventInterimHeaders` and are capped at
  100 per stream (matching HTTP/1.1 and HTTP/3); `Client.Do` and `DoStream` report
  the final status, as they already did on the other two protocols. §8.1's other
  half is enforced too: a trailer block without END_STREAM is malformed
  (§8.1.2.6) and is now a stream error, which also bounds the trailer-block flood
  a peer could otherwise sustain.
- **QUIC streams release consumed bytes.** A receive stream's buffer was only
  ever appended to: reading advanced a cursor but never freed the bytes behind
  it, so a stream retained every byte it had ever received. Receive flow control
  did not bound this — its window is keyed on the read cursor, so it slides
  forward as the application consumes, bounding bytes *in flight* rather than
  bytes *retained*. `DoStream` therefore did not stream: an 8 MiB response
  retained 8 MiB after being fully consumed, 32x the 256 KiB window. Fixed by
  releasing the consumed prefix. Also ~3x faster on the receive path, since the
  growing buffer had been forcing `append` to recopy.
- **Bounded HPACK dynamic-table entry ring.** The HTTP/2 dynamic table grew its
  entry ring by one slot per header and never reused an evicted entry's slot, so
  a peer could drive the ring's size with its header *count* rather than the
  4096-octet table size that is supposed to bound it. Empty-name/empty-value
  literals cost three bytes on the wire and bought a slot each: 3 MB sent
  retained 16 MB, with no ceiling. The ring now grows only when every slot is
  live, matching QPACK. Compaction allocates a fresh ring rather than aliasing
  the old one — with a ring that can now wrap, the aliasing would have silently
  swapped one header's value for another's.
- Added `SECURITY.md` with a private disclosure process.

### Tested

- **Soak / endurance** — over one million requests on one managed HTTP/3
  connection with no goroutine or heap growth.
- **Fuzzed wire parsers** for QUIC, HTTP/3, QPACK (#198), and HTTP/1.1.
- HTTP/3 interop extended with a ChaCha20-only server and a 1 MiB BBR transfer,
  across Caddy, nginx, and aioquic.

### Removed

- Internal working documents (design specs, review notes, investigations) and
  stray build artifacts pruned from the repository.

## [v0.9.0] — 2026-07-11

### Added

- **HTTP/3 client — a from-scratch, zero-dependency QUIC + HTTP/3 stack.** A
  second protocol alongside the HTTP/2 client, implemented in pure Go from the
  RFCs with **no `quic-go`, no `net/http`, no `golang.org/x/net`** — the same
  zero-dependency, fine-grained-control philosophy as the HTTP/2 codec. Public
  entry point: `http3.Dial(ctx, addr, tlsConfig) → *Client`, then `Client.Do`.
  The layers, each conformance-tested (see
  [docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md); design in
  [docs/HTTP3_DESIGN.md](docs/HTTP3_DESIGN.md)):

  - **QUIC transport (RFC 9000)** — connection establishment, stream
    multiplexing, bidirectional flow control (`MAX_DATA` / `MAX_STREAM_DATA` /
    `MAX_STREAMS`), connection-ID issuance and rotation (`NEW_CONNECTION_ID` /
    `RETIRE_CONNECTION_ID`), ACK ranges, transport parameters, varint framing,
    `CONNECTION_CLOSE`, and Retry (`quic/`).
  - **Loss recovery + congestion control (RFC 9002)** — ACK-based loss
    detection, PTO / probe timeouts, RTT estimation, and a congestion controller
    with retransmission (`loss.go`, `cc.go`, `pto.go`, `rtt.go`).
  - **QUIC-TLS (RFC 9001)** — the TLS 1.3 handshake over CRYPTO streams, 1-RTT
    key derivation, key update (Key Phase 0→1), and AEAD packet + header
    protection (`crypto*.go`, `handshake.go`).
  - **QPACK (RFC 9204)** — a static-table encoder and decoder. The client
    advertises `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0` (static-only, no
    dynamic-table state to synchronise); a server that references the dynamic
    table anyway is rejected cleanly as `QPACK_DECOMPRESSION_FAILED` rather than
    mis-decoded (`qpack/`).
  - **HTTP/3 (RFC 9114)** — the control stream + `SETTINGS`, request/response
    mapping (`HEADERS` / `DATA` / trailers / 1xx interim responses), strict frame
    and message-order validation, typed HTTP/3 and QUIC error codes,
    `RESET_STREAM` / `STOP_SENDING` handling with retryable classification
    (`*StreamResetError.Retryable()`), and `GOAWAY` drain (`http3/`).

### Hardened

- **Receive-path resource bounds** (#162–166) — the decode path is bounded
  against an adversarial or buggy peer: bounded ACK / loss-tracking memory,
  capped stream and CRYPTO reassembly buffers, a `NEW_CONNECTION_ID`
  retire-flood bound, HTTP/3 response per-frame and cumulative size caps
  (`ErrResponseTooLarge`), and eviction of reset streams from the routing map.

### Tested

- **Conformance suites** for RFC 9000, 9001, 9002, 9114, and 9204, gate-tracked
  by `conformance-gate` (see [docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md)).
- **Whole-stack interop**, run in CI, against **three independent HTTP/3 server
  implementations** over real UDP — Caddy (`quic-go`, Go), nginx (its own C QUIC
  stack), and aioquic (pure Python) — plus a fault-injecting server (malformed
  frames, `RESET_STREAM`, `STOP_SENDING`, dynamic-QPACK) and a loss/reorder relay
  (`test/integration/http3`).

### Known limitations

- **ChaCha20-Poly1305 header protection is deferred** — AES-GCM cipher suites
  only (RFC 9001 §5.1). A ChaCha20-only server is a documented graceful failure.
- **QPACK is static-only** — no dynamic-table *encode*; the decoder honours a
  capacity of 0.
- **`Client.Do` is blocking and sequential** — the engine is single-goroutine
  and not safe for concurrent use, so the public API exposes no concurrent
  in-flight requests (QUIC multiplexing exists at the transport).
- A few rare RFC 9000 conformance items remain deferred (documented in `docs/`).

## [v0.8.0] — 2026-07-08

### Added

- **`ConnOptions.GroupCommit` — opt-in write batching (group-commit).** When a
  HEADERS writer finds another writer queued on the write lock, it defers its
  flush so the next holder batches both frames into a single `tls.Conn.Write` —
  fewer TLS record encrypts + socket writes under high per-connection stream
  concurrency. It is **synchronous** (the deferring writer waits for the convoy
  flush and returns its error, so per-frame write-error semantics are preserved
  — no async contract change), **adaptive** (a strict no-op with no contention;
  a lone request is never delayed), and **off by default**. Measured on one TLS
  h2 conn at 64 concurrent streams: 1.73 frames/flush, ≈−42% write syscalls,
  ≈+26% req/s; the win survives poseidon's dense-multiplexing pool (≈+30–35% at
  1–8 conns). Motivated by a review of ozontech/framer; see
  [docs/WRITE_BATCHING_INVESTIGATION.md](docs/WRITE_BATCHING_INVESTIGATION.md).
  Note: framer's `net.Buffers`/writev mechanism gives zero benefit through
  `crypto/tls.Conn`; plaintext frame coalescing is the only TLS-viable lever.

## [v0.7.1] — 2026-07-07

### Performance

- **Buffered connection writer** — the transport writer is now wrapped in a
  reused 16 KiB `bufio.Writer`, so a DATA frame's 9-byte header and its payload
  coalesce into a single flush = **1 write syscall per frame instead of 2**.
  A target-load CPU profile showed the client is syscall-bound (~50% CPU in the
  socket write); this halves the DATA-frame write syscalls (measured: a
  7-DATA-frame body dropped from ~16 write syscalls to 8) with no per-frame
  allocation. The buffer is flushed under `wmu` before every lock release and
  before every flow-control (`acquireSendCredits`) wait, so no buffered frame is
  ever stranded behind a WINDOW_UPDATE the peer hasn't been asked for.
  Regression guard: `TestWriteBuffer_MultiChunkUpload_NoDeadlock`.

## [v0.7.0] — 2026-06-23

### Changed

- **BREAKING:** `Request.WantBody bool` and `Request.StreamBody bool` are
  replaced by a single `Request.BodyMode` (`BodyMode` enum). The zero value
  `BodyDiscard` drops the body (same as the old `WantBody:false` /
  `StreamBody:false` default); `BodyBuffer` accumulates it into `Response.Body`
  (old `WantBody:true`); `BodyStream` returns `Response.BodyReader` for
  incremental reads (old `StreamBody:true`). This removes the invalid-but-
  representable `WantBody:true, StreamBody:true` state and the "StreamBody wins"
  precedence rule — matching the `IdempotencyMode` enum introduced in v0.6.0.

  Migration: `WantBody: true` → `BodyMode: client.BodyBuffer`;
  `StreamBody: true` → `BodyMode: client.BodyStream`; leave the field unset for
  the discard default. The `GET`/`POST`/`NewRequest` helpers set `BodyBuffer`
  for you, so code using those is unaffected.

### Docs

- Retired `docs/USAGE.md` (superseded by `docs/CLIENT_GUIDE.md`, the single
  canonical guide). It is now a redirect stub; README links to CLIENT_GUIDE.md.
  Keeping one guide avoids the drift that left USAGE.md with stale examples.

## [v0.6.0] — 2026-06-23

### Added

- **Opt-in ergonomic layer** (`client/sugar.go`): `client.H(name, value)`
  header helper (lowercases names per RFC 7540 §8.1.2) + a `client.HeaderField`
  alias so callers need not import `conn` for the common type; `NewRequest` /
  `GET` / `POST` constructors that default `WantBody=true` (the obvious path now
  captures the body instead of silently dropping it); `Request.WithHeaders`
  chaining; `Response.Header` / `HeaderString` (allocation-free reads);
  `Response.CopyBody` / `Response.Clone` / `StreamEvent.DataCopy` to safely
  retain bytes past the next `Recv`/`Reset`/`Close`; and
  `Client.Stream(ctx, req, fn)`, which pumps a streaming response and **always**
  `Close`s it so callers cannot leak the pooled stream slot.
- **Focused constructors** + functional options: `NewSingleConnClient`,
  `NewPoolClient`, `NewManagedClient` encode a valid transport + required-field
  combination in their signatures (invalid combinations are unrepresentable);
  options `WithHooks`, `WithPushHandler`, `WithDefaultScheme`, `WithRateLimit`,
  `WithMaxResponseBodySize`, `WithMaxDecompressedSize`, `WithDialBackoff`,
  `WithSelector`, `WithDrainMode`, `WithConnOptions`. `NewClient(ClientOptions{})`
  remains for full control.
- **`Client.Retryer(opts)`** — discoverable method form of `NewRetryer(c, opts)`.
- **Status-class metrics** — `Counters.Responses2xx` / `Counters.ResponsesNon2xx`
  split `RequestsSucceeded` by status class, so a load generator can measure
  real success rate rather than "a response arrived".
- **`EventNone`** — names the zero value of `EventType`.
- **Debug-gated leak detector** (`-tags poseidondebug`) — reports a
  `StreamResponse` or `Response.BodyReader` garbage-collected without `Close()`.
  Zero cost in normal builds; CI compile-checks the tag and `make test-debug`
  runs its finalizer tests.
- **`docs/CLIENT_GUIDE.md`** — full client usage guide with verified examples.
- **Examples** — 24 compile-tested godoc `Example` functions covering every
  feature (`client/example_*_test.go`, rendered on pkg.go.dev), plus a runnable
  `examples/loadgen` load generator (pooled client, rate limit, hooks, worker
  pool, metrics snapshot).

### Changed

- **BREAKING:** `Request.Idempotent *bool` → `Request.Idempotency`
  (`IdempotencyMode` enum). The zero value `IdempotencyAuto` classifies by HTTP
  method (same as the old `nil`); `ForceIdempotent` / `ForceNotIdempotent`
  override (same as the old `&true` / `&false`). Removes the
  addr-of-a-local-`*bool` dance.

### Performance

- **Zero-allocation receive hot path** — the per-DATA-frame copy buffer is now
  pooled (`conn.dataBufPool`); steady-state receive allocates nothing per frame.
  Ownership transfers to the client via `StreamEvent.DataSlab` and is returned
  on consume.
- **Buffered transport reader** — `NewClientConn` wraps the transport reader in
  a 16 KiB buffer, collapsing the per-frame 2× `ReadFull` into far fewer
  `read(2)` syscalls when frames arrive together (notably h2c). Strictly
  no-op-or-better; smaller win over TLS where the record layer already buffers.
- Realistic warm HPACK allocation benchmarks added; steady-state encode/decode
  is **0 alloc/op** even under Huffman-forcing custom-header traffic.

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
