# poseidon-http-client

[![Go Reference](https://pkg.go.dev/badge/github.com/lodgvideon/poseidon-http-client.svg)](https://pkg.go.dev/github.com/lodgvideon/poseidon-http-client)
[![CI](https://github.com/lodgvideon/poseidon-http-client/actions/workflows/ci.yml/badge.svg)](https://github.com/lodgvideon/poseidon-http-client/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/lodgvideon/poseidon-http-client?sort=semver)](https://github.com/lodgvideon/poseidon-http-client/releases)
[![Go 1.24](https://img.shields.io/badge/go-1.24-00ADD8)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**From-scratch HTTP/2 and HTTP/3 clients for Go, built for load generators.**
The HTTP/2 client implements RFC 7540 (HTTP/2) and RFC 7541 (HPACK) — plus an
HTTP/1.1 fallback — with no `net/http` and no `golang.org/x/net/http2`. The
HTTP/3 client (new in v0.9.0) implements its own QUIC transport — RFC 9000
(QUIC), 9002 (loss recovery + congestion control), 9001 (QUIC-TLS), 9204
(QPACK), and 9114 (HTTP/3) — with no `quic-go` and no
`golang.org/x/net/http3`, on top of the standard library's `crypto/tls`. You
get fine-grained control over connections, streams, and flow control, wrapped
in APIs that stay out of the hot path.

```go
// HTTP/2 — the mature, full-featured client:
c, _ := client.NewSingleConnClient("example.com:443",
    &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}})
defer c.Close()

var resp client.Response
_ = c.Do(context.Background(), client.GET("/"), &resp)
fmt.Println(resp.Status, len(resp.Body)) // 200 1256
```

```go
// HTTP/3 — new in v0.9.0, minimal but conformance-tested:
h3, _ := http3.Dial(ctx, "example.com:443", &tls.Config{ServerName: "example.com"})
defer h3.Close()

resp, body, _ := h3.Do(ctx, &http3.Request{
    Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"})
fmt.Println(resp.Status, len(body)) // 200 1256
```

## Why poseidon?

- **Zero-alloc codecs.** The `frame`, `hpack`, and `qpack` codecs are
  `0 B/op`, `0 allocs/op` — enforced by a bench gate in CI. On the HTTP/2
  side, the request/response path pools receive buffers, header slabs, and
  `Stream` structs, and reuses a caller-owned `Response`, so steady-state load
  allocates almost nothing.
- **Nothing hidden.** HTTP/2 framing, HPACK, flow control, the HTTP/1.1 wire
  protocol, QUIC packetization, loss recovery, congestion control, and QPACK
  are all implemented directly from the RFCs. TLS comes from the standard
  library (`crypto/tls`; the QUIC handshake uses Go 1.24's `tls.QUICConn`).
  The module's one dependency, `golang.org/x/net`, is never on the wire path —
  every protocol here is hand-written.
- **Conformance you can audit.** Every pinned RFC behavior — across RFC 7540,
  7541, 9000, 9001, 9002, 9114, and 9204 — maps to a gate-tracked test in
  [docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md). The HTTP/3 stack additionally
  runs a CI interop matrix against **three independent server
  implementations** — Caddy (quic-go), nginx (its own C QUIC stack), and
  aioquic (pure Python) — over real UDP, plus a fault-injecting server and a
  loss/reorder relay.
- **Load-generator ergonomics (HTTP/2).** Connection pooling, DNS-based
  service discovery, rate limiting, opt-in retries, per-request metrics, and
  lifecycle hooks — each with **zero overhead when unused** (nil hooks and
  disabled rate limits are branch-free on the fast path).
- **Syscall-lean.** A buffered reader and writer coalesce frame I/O so a DATA
  frame's header and payload become one `write` syscall, not two, and opt-in
  group-commit batches concurrent writers into a single TLS record. Profiling
  shows the HTTP/2 client is bound by the socket syscall itself — not by our
  code.

## Install

```bash
go get github.com/lodgvideon/poseidon-http-client@latest
```

Requires **Go 1.24+**. Public packages: `client` (high-level HTTP/2), `conn`
(HTTP/2 connections/streams), `frame` and `hpack` (standalone HTTP/2 codec),
`http3` (HTTP/3 client), `quic` (QUIC transport), `qpack` (standalone QPACK
codec).

## Two clients, one philosophy

Both clients are written from scratch and conformance-tested, but they are at
very different points in their lives. The HTTP/2 client has been through many
releases of load-generator hardening; the HTTP/3 client is a **newer,
minimal-but-conformant client: dial, do, close.** Pick accordingly.

|  | HTTP/2 (`client`) | HTTP/3 (`http3`) |
|---|---|---|
| **Maturity** | Mature — the full feature set below | New in v0.9.0 — minimal, conformance-focused |
| **Entry point** | `NewSingleConnClient` / `NewPoolClient` / `NewManagedClient` | `http3.Dial` |
| **Requests** | Unary `Do` + streaming `DoStream`, body streaming, trailers | Blocking `Do` — buffered body, trailers and 1xx surfaced on the `Response` |
| **Concurrency** | `Client` is goroutine-safe; streams multiplex per connection | One request at a time; a `Client` is **not** safe for concurrent use |
| **Pooling / discovery / retries / rate limit / hooks / metrics** | Yes | No |
| **Server push / extended CONNECT / priority** | Yes | No |
| **Header compression** | HPACK, zero-alloc | QPACK, zero-alloc, **static table only** |
| **Fallback / negotiation** | ALPN (`h2` / `http/1.1`), H2C | — |
| **Conformance** | RFC 7540 + 7541, gate-tracked | RFC 9000 + 9001 + 9002 + 9114 + 9204, gate-tracked + 3-server interop matrix |

## Quick start — HTTP/2

The easy path — a focused constructor plus a request helper:

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	c, err := client.NewSingleConnClient("example.com:443",
		&conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close() // REQUIRED — leaks the conn + reader goroutine otherwise.

	var resp client.Response
	if err := c.Do(context.Background(), client.GET("/"), &resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println("status:", resp.Status, "bytes:", resp.BytesReceived)
}
```

The steady-state load loop reuses one `Response` — call `Reset()` before every
`Do`, and treat the result's slices as valid only until the next `Reset()`:

```go
var resp client.Response // one per goroutine
for {
	resp.Reset() // MUST reset before every (re)use, including after an error.
	if err := c.Do(ctx, client.GET("/metrics"), &resp); err != nil {
		log.Fatal(err)
	}
	_ = resp.Status  // e.g. 200
	_ = resp.Headers // valid until next Reset()
	_ = resp.Body    // valid until next Reset()
}
```

### Pick a transport

One constructor per strategy — an invalid transport/field combination is
unrepresentable:

```go
// 1. Single connection, auto-redial (the default).
c, _ := client.NewSingleConnClient(addr, dialer)

// 2. Pool of up to N connections to one backend.
c, _ := client.NewPoolClient(addr, dialer,
	client.PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 100})

// 3. Managed multi-backend: a Resolver discovers addresses, a Selector picks one.
c, _ := client.NewManagedClient(resolver, dialer,
	client.WithSelector(client.RoundRobin()))
```

Tune anything with functional options — `WithRateLimit`, `WithHooks`,
`WithPushHandler`, `WithDefaultScheme` (H2C), `WithMaxResponseBodySize`,
`WithConnOptions` (keepalive, SETTINGS, push), and more. Drop to
`client.NewClient(client.ClientOptions{...})` for full control.

### Codec-only usage

The `frame`, `hpack`, and `qpack` packages have no networking dependency and
are importable on their own for anyone building their own HTTP/2 or HTTP/3
stack:

```go
import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)
```

## Quick start — HTTP/3

`http3.Dial` establishes a QUIC connection (handshake, transport parameters,
control streams); `Do` sends one request and returns the response head and the
buffered body:

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/lodgvideon/poseidon-http-client/http3"
)

func main() {
	ctx := context.Background()

	c, err := http3.Dial(ctx, "example.com:443", &tls.Config{ServerName: "example.com"})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close() // REQUIRED — same contract as the HTTP/2 client.

	resp, body, err := c.Do(ctx, &http3.Request{
		Method:    "GET",
		Scheme:    "https",
		Authority: "example.com",
		Path:      "/",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status, len(body))
}
```

The request and response types are deliberately small:

```go
type Request struct {
	Method    string
	Scheme    string
	Authority string // omitted from the field section when empty
	Path      string
	Headers   []hpack.HeaderField
	Body      []byte // optional, sent in a DATA frame after HEADERS
}

type Response struct {
	Status   int
	Headers  []hpack.HeaderField
	Interim  []*Response // 1xx informational responses
	Trailers []hpack.HeaderField
}
```

Errors are typed. A server-side `RESET_STREAM` surfaces as a
`StreamResetError` (`struct{ Code uint64 }`), whose `Retryable()` method
reports whether the request is known to be safe to resend —
`H3RequestRejected` means the server refused it before processing:

```go
var reset *http3.StreamResetError
if errors.As(err, &reset) && reset.Retryable() {
	// H3_REQUEST_REJECTED — the server never processed the request; resend it.
}
```

Connection- and message-level failures match the sentinels `ErrH3Control`,
`ErrH3Message`, `ErrH3Settings`, `ErrH3FrameTooLarge`, and
`ErrResponseTooLarge` (the response exceeded the client's buffering cap) via
`errors.Is`.

### HTTP/3 support matrix and non-goals

The HTTP/3 stack speaks the same `client.Do` API as HTTP/2: concurrent in-flight
requests, connection pooling, service discovery, retries, hooks/metrics, dynamic
QPACK, and opt-in BBR all work over HTTP/3 — not only HTTP/2.

**Supported**

| Capability | Detail |
|---|---|
| **Cipher suites** | All TLS 1.3 AEADs — AES-128-GCM, AES-256-GCM, **and ChaCha20-Poly1305** (RFC 9001 §5.3 / §5.4.4) |
| **Concurrency** | Concurrent in-flight requests over one QUIC connection (stream multiplexing); `Client.Do` / `DoStream` over `TransportH3` are safe for concurrent use |
| **Pooling & discovery** | `TransportH3Pool` (multi-conn per host) and `TransportH3Managed` (`Resolver` + `Selector`), mirroring the HTTP/2 transports |
| **QPACK** | Dynamic-table encoding **and** decoding, both directions (RFC 9204) |
| **Congestion control** | NewReno by default; opt-in BBR via `ClientOptions.H3ConnOptions = []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)}` |
| **Batched I/O (Linux)** | GSO send + GRO receive; bounded ACK deferral/coalescing |
| **Resilience & observability** | Retries, rate limiting, `Hooks`, metrics — shared with the HTTP/2 client through the unified `client.Do` |

**Non-goals (deliberately out of scope for 1.0)**

- **0-RTT / session resumption** — the client never sends early data; a server that offers a session ticket is simply not resumed, nothing fails.
- **Connection migration** — one 4-tuple for a connection's life; the client never initiates path migration and a peer cannot force it.
- **HTTP/3 server push** — not implemented; the client does not advertise push.
- An **unsupported cipher suite** fails cleanly at key install (`ErrCryptoSuite`) — never a hang, panic, or silent mis-decode.

Throughput note: BBR ships correct and opt-in, but its benefit over NewReno is
only measurable on a bottlenecked WAN path (a `netem` lab), not on loopback/CI —
so it is verified for correctness (unit + interop), not claimed as a speedup.

## Features

### HTTP/2 (`client`, `conn`, `frame`, `hpack`)

| Area | What you get |
|---|---|
| **Protocol** | HTTP/2 (RFC 7540) framing + HPACK (RFC 7541) from scratch; HTTP/1.1 fallback; ALPN auto-negotiation (`h2` / `http/1.1`); H2C plaintext prior-knowledge |
| **Requests** | Unary `Do` with a reusable `Response`; `content-length` upload; gzip/deflate response decompression with a decompression-bomb guard |
| **Streaming** | Event-driven `DoStream` + `StreamResponse`; `io.ReadCloser` body streaming (`BodyStream`); streaming request-body upload; request trailers |
| **Connections** | Single-conn with auto-redial; per-host pool (least-loaded stream pick, idle eviction, dial backoff); HTTP `CONNECT` proxy dialers (with Basic auth); full bidirectional flow control; dynamic SETTINGS; `MAX_CONCURRENT_STREAMS` gating; GOAWAY drain; PING keepalive |
| **Service discovery** | `Resolver` (`StaticResolver`, `DNSResolver` with TTL cache + Watch) + `Selector` (`RoundRobin`, `Random`, `Hash`); graceful / hard / lazy drain modes |
| **Resilience** | Opt-in `Retryer` wrapper: bounded retries of idempotent requests (`REFUSED_STREAM`, GOAWAY, dial errors) with truncated-exponential backoff + jitter; per-request idempotency override; token-bucket rate limiting; per-request timeouts |
| **Observability** | Lifecycle `Hooks` (request start/complete, retry, dial, conn close, resolver update); lock-free counters + log-bucket latency histograms; status-class metrics; `PoolStats()` |
| **Advanced protocol** | Server push (`PUSH_PROMISE`); request priority (§5.3); extended CONNECT (RFC 8441 — WebSockets over HTTP/2); large header blocks via `CONTINUATION` |
| **Performance** | Zero-alloc codec (bench-gated); pooled buffers/slabs/streams; caller-owned `Response` reuse; buffered writer coalescing DATA syscalls; opt-in group-commit write batching |

### HTTP/3 (`http3`, `quic`, `qpack`)

| Layer | What you get |
|---|---|
| **QUIC transport (RFC 9000)** | Connection establishment, stream multiplexing, bidirectional flow control (`MAX_DATA` / `MAX_STREAM_DATA` / `MAX_STREAMS`), connection-ID issuance and rotation, ACK ranges, transport parameters, varint framing, `CONNECTION_CLOSE`, Retry |
| **Loss recovery + congestion control (RFC 9002)** | ACK-based loss detection, PTO / probe timeouts, RTT estimation; **NewReno (default) or opt-in BBR** with retransmission; pacing; **GSO/GRO batched UDP I/O + ACK coalescing on Linux** — the client stays correct on lossy, reordering paths |
| **QUIC-TLS (RFC 9001)** | TLS 1.3 handshake over CRYPTO streams via stdlib `tls.QUICConn`, 1-RTT key derivation, key update, AEAD packet + header protection — **AES-128/256-GCM and ChaCha20-Poly1305**; suite-aware AEAD usage limits (§6.6) |
| **QPACK (RFC 9204)** | Static- **and dynamic-table** encoder and decoder, both directions; encoder/decoder instruction streams with known-received-count and blocked-streams handling; zero-alloc static path, bench-gated |
| **HTTP/3 (RFC 9114)** | Control stream + `SETTINGS`; request/response mapping (`HEADERS` / `DATA` / trailers / 1xx interim responses); strict frame and message-order validation; typed HTTP/3 + QUIC error codes; `RESET_STREAM` / `STOP_SENDING` handling with retryable classification; `GOAWAY` drain |
| **Robustness** | Receive path bounded against adversarial peers: capped ACK/loss-tracking memory, stream and CRYPTO reassembly caps, `NEW_CONNECTION_ID` flood bound, per-frame and cumulative response size caps (`ErrResponseTooLarge`) |
| **Testing** | Conformance suites for all five RFCs (gate-tracked); CI interop matrix vs. Caddy, nginx, and aioquic over real UDP; fault-injecting server; loss/reorder relay |

## Documentation

- **[docs/CLIENT_GUIDE.md](docs/CLIENT_GUIDE.md)** — the canonical HTTP/2
  client guide. Every HTTP/2 feature above with verified, copy-pasteable
  examples: construction and the five transports, unary and streaming
  requests, retry / idempotency / rate limiting, pooling and service
  discovery, observability, required-call contracts, and the error model.
- **[docs/HTTP3_DESIGN.md](docs/HTTP3_DESIGN.md)** — the HTTP/3 stack design:
  layering, the QUIC engine, and the conformance strategy.
- **[docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md)** — the authoritative
  test-to-RFC-section matrix for all seven RFCs, enforced by the
  `conformance-gate` CI job.
- **[Go Reference (pkg.go.dev)](https://pkg.go.dev/github.com/lodgvideon/poseidon-http-client/client)**
  — full API docs and 30+ runnable / compile-tested examples.
- **[examples/loadgen](examples/loadgen)** — a runnable load generator: pooled
  client, rate limiting, hooks, a worker pool, and a metrics snapshot.
- **[CHANGELOG.md](CHANGELOG.md)** — release history (v0.1.0 → **v0.9.0**).

### Where each HTTP/2 feature is documented

| I want to… | Guide section | Example |
|---|---|---|
| Pick a transport (single / pool / managed / ALPN) | [The five transport kinds](docs/CLIENT_GUIDE.md#the-five-transport-kinds) | `ExampleNewSingleConnClient`, `ExampleNewPoolClient`, `ExampleNewManagedClient`, `Example_alpnTransport` |
| Speak H2C (plaintext HTTP/2) | [H2C](docs/CLIENT_GUIDE.md#h2c-plaintext-prior-knowledge) | `Example_h2c` |
| Dial through a `CONNECT` proxy | [Dialers](docs/CLIENT_GUIDE.md#dialers-connoptsdialer) | `ExampleProxyTLSDialer` |
| Make a GET / POST | [Unary requests](docs/CLIENT_GUIDE.md#unary-requests-do) | `ExampleGET`, `ExamplePOST`, `Example_postJSON` |
| Reuse a `Response` under load | [The `Response` reuse contract](docs/CLIENT_GUIDE.md#the-response-struct-and-the-reset-reuse-contract) | `Example_reuseResponse` |
| Stream a response body | [Streaming responses](docs/CLIENT_GUIDE.md#streaming-responses--body-upload) | `Example_streamBodyReader`, `Example_streamingDownload` |
| Upload a request body | [Streaming the request body](docs/CLIENT_GUIDE.md#streaming-the-request-body-upload) | `Example_uploadBody` |
| Get the raw (compressed) body | [Disabling decompression](docs/CLIENT_GUIDE.md#7-disabling-response-decompression) | `ExampleRequest_disableDecompression` |
| Retry idempotent requests | [Retryer](docs/CLIENT_GUIDE.md#retryer) | `ExampleClient_Retryer`, `Example_retryOnRefusedStream`, `Example_idempotencyOverride` |
| Rate-limit requests | [Rate limiting](docs/CLIENT_GUIDE.md#rate-limiting) | `Example_rateLimit` |
| Time out a request | [Per-request timeout](docs/CLIENT_GUIDE.md#per-request-timeout) | `Example_requestTimeout` |
| Pool + warm up + drain connections | [Pooling](docs/CLIENT_GUIDE.md#connection-pooling--service-discovery), [Lifecycle](docs/CLIENT_GUIDE.md#2-lifecycle-close-shutdown-warmup), [Pool stats](docs/CLIENT_GUIDE.md#3-pool-statistics--stats-and-poolstats) | `ExampleNewPoolClient`, `Example_poolLifecycle` |
| Discover backends via DNS | [Resolvers](docs/CLIENT_GUIDE.md#5-resolvers--resolver-staticresolver-dnsresolver) | `ExampleDNSResolver`, `ExampleStaticResolver` |
| Balance across backends | [Selectors](docs/CLIENT_GUIDE.md#6-selectors--selector-roundrobin-random-hash-pickcontext) | `ExampleHash`, `ExampleRandom` |
| Collect metrics / hooks | [Hooks](docs/CLIENT_GUIDE.md#1-hooks), [Metrics](docs/CLIENT_GUIDE.md#2-metrics) | `ExampleHooks`, `ExampleClient_MetricsSnapshot`, `Example_successRate` |
| Handle server push | [Server push](docs/CLIENT_GUIDE.md#3-server-push-push_promise) | `Example_serverPush` |
| Set request priority | [Request priority](docs/CLIENT_GUIDE.md#4-request-priority-rfc-7540-53) | `Example_requestPriority` |
| Tunnel WebSockets (extended CONNECT) | [Extended CONNECT](docs/CLIENT_GUIDE.md#5-extended-connect-rfc-8441--websockets-over-http2) | `Example_extendedConnect` |
| Send / read trailers | [Request trailers](docs/CLIENT_GUIDE.md#6-request-trailers) | `Example_requestTrailers`, `ExampleStreamResponse_WaitTrailers` |
| Use the ergonomic helpers | [Convenience helpers](docs/CLIENT_GUIDE.md#convenience-helpers) | `ExampleH`, `ExampleResponse_Header`, `ExampleClient_Stream` |
| Match errors | [Error model](docs/CLIENT_GUIDE.md#8-error-model) | `Example_errorsIs` |

## Contracts

A few invariants worth internalizing before you build on the API — the full
HTTP/2 list lives in [the guide's Required-call contracts](docs/CLIENT_GUIDE.md#required-call-contracts):

- **Always `Close`** (or `Shutdown`) every client you construct — HTTP/2
  `client.Client` and HTTP/3 `http3.Client` alike. Otherwise you leak the
  connection and its goroutines.
- **`Response.Reset()` before every reuse** (HTTP/2). `Response.Headers` /
  `Body` and `StreamEvent` slices alias internal scratch buffers, valid only
  until the next `Reset()` / `Recv` / `Close`. Copy (`Response.CopyBody`) to
  retain.
- **An HTTP/2 `Client` is goroutine-safe;** a single `Response` /
  `StreamResponse` is owned by one goroutine at a time. An **HTTP/3 `Client`
  is not goroutine-safe** — one request at a time; dial one per worker. The
  `frame` / `hpack` / `qpack` codecs are not goroutine-safe either — each
  connection owns its own and serializes access.

## Development

Requirements: **Go 1.24**, `golangci-lint` **v2.5** (optional, for `make lint`).

```bash
make tidy       # go mod tidy
make lint       # golangci-lint v2.5
make test-race  # go test -race ./...  (default verification)
make bench      # benches with the 0 B/op · 0 allocs/op gate on the codecs
```

The HTTP/3 interop matrix runs in CI and locally via Docker Compose:

```bash
make h3-interop          # real servers (Caddy / nginx / aioquic) over UDP
make h3-interop-loss     # same suite through a datagram-loss relay
make h3-interop-reorder  # a datagram-reorder relay
make h3-interop-fault    # deliberately misbehaving server (negative paths)
```

Optional pre-commit hook:

```bash
git config core.hooksPath .githooks
```

See [docs/BENCH_BASELINE.md](docs/BENCH_BASELINE.md) for reference numbers and
[docs/COVERAGE.md](docs/COVERAGE.md) for the coverage policy. Architecture and
milestones: [conn/doc.go](conn/doc.go) (HTTP/2) and
[docs/HTTP3_DESIGN.md](docs/HTTP3_DESIGN.md) (HTTP/3).

## License

MIT — see [LICENSE](LICENSE).
