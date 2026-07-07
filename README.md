# poseidon-http-client

[![Go Reference](https://pkg.go.dev/badge/github.com/lodgvideon/poseidon-http-client.svg)](https://pkg.go.dev/github.com/lodgvideon/poseidon-http-client)
[![CI](https://github.com/lodgvideon/poseidon-http-client/actions/workflows/ci.yml/badge.svg)](https://github.com/lodgvideon/poseidon-http-client/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/lodgvideon/poseidon-http-client?sort=semver)](https://github.com/lodgvideon/poseidon-http-client/releases)
[![Go 1.24](https://img.shields.io/badge/go-1.24-00ADD8)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A **zero-allocation HTTP/2 client for Go, built for load generators.**
It implements RFC 7540 (HTTP/2) and RFC 7541 (HPACK) — plus an HTTP/1.1
fallback — **from scratch**, with no `net/http` and no
`golang.org/x/net/http2`. You get fine-grained control over connections,
streams, flow control, and pooling, wrapped in an ergonomic API that stays
out of the hot path.

```go
c, _ := client.NewSingleConnClient("example.com:443",
    &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}})
defer c.Close()

var resp client.Response
_ = c.Do(context.Background(), client.GET("/"), &resp)
fmt.Println(resp.Status, len(resp.Body)) // 200 1256
```

## Why poseidon?

- **Zero-alloc hot path.** The frame + HPACK codec is `0 B/op`, `0 allocs/op`
  — enforced by a bench gate in CI. The request/response path pools receive
  buffers, header slabs, and `Stream` structs, and reuses a caller-owned
  `Response`, so steady-state load allocates almost nothing.
- **Nothing hidden.** Framing, HPACK, flow control, and even the HTTP/1.1
  wire protocol are implemented directly. No surprise `net/http` behavior,
  no reflection, no interface soup between you and the socket.
- **Load-generator ergonomics.** Connection pooling, DNS-based service
  discovery, rate limiting, automatic retries, per-request metrics, and
  lifecycle hooks — each with **zero overhead when unused** (nil hooks and
  disabled rate limits are branch-free on the fast path).
- **Syscall-lean.** A buffered reader and writer coalesce frame I/O so a DATA
  frame's header and payload become one `write` syscall, not two. Profiling
  shows the client is bound by the socket syscall itself — not by our code.

## Install

```bash
go get github.com/lodgvideon/poseidon-http-client@latest
```

Requires **Go 1.24+**. Public packages: `client` (high-level), `conn`
(connections/streams), `frame` and `hpack` (standalone codec).

## Quick start

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
`WithConnOptions` (keepalive, buffers), and more. Drop to
`client.NewClient(client.ClientOptions{...})` for full control.

### Codec-only usage

The `frame` and `hpack` packages have no networking dependency and are
importable on their own for anyone building their own HTTP/2 stack:

```go
import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)
```

## Features

| Area | What you get |
|---|---|
| **Protocol** | HTTP/2 (RFC 7540) framing + HPACK (RFC 7541) from scratch; HTTP/1.1 fallback; ALPN auto-negotiation (`h2` / `http/1.1`); H2C plaintext prior-knowledge |
| **Requests** | Unary `Do` with a reusable `Response`; `content-length` upload; gzip response decompression with a decompression-bomb guard |
| **Streaming** | Event-driven `DoStream` + `StreamResponse`; `io.ReadCloser` body streaming (`BodyStream`); streaming request-body upload; request trailers |
| **Connections** | Single-conn with auto-redial; per-host pool (least-loaded stream pick, idle eviction, dial backoff); full bidirectional flow control; dynamic SETTINGS; `MAX_CONCURRENT_STREAMS` gating; GOAWAY drain; PING keepalive |
| **Service discovery** | `Resolver` (`StaticResolver`, `DNSResolver` with TTL cache + Watch) + `Selector` (`RoundRobin`, `Random`, `Hash`); graceful / hard / lazy drain modes |
| **Resilience** | Automatic retry of idempotent requests (`REFUSED_STREAM`, GOAWAY, dial errors) with truncated-exponential backoff + jitter; per-request idempotency override; token-bucket rate limiting; per-request timeouts |
| **Observability** | Lifecycle `Hooks` (request start/complete, retry, dial, conn close, resolver update); lock-free counters + log-bucket latency histograms; status-class metrics; `PoolStats()` |
| **Advanced protocol** | Server push (`PUSH_PROMISE`); request priority (§5.3); extended CONNECT (RFC 8441 — WebSockets over HTTP/2); large header blocks via `CONTINUATION` |
| **Performance** | Zero-alloc codec (bench-gated); pooled buffers/slabs/streams; caller-owned `Response` reuse; buffered writer coalescing DATA syscalls |

## Documentation

- **[docs/CLIENT_GUIDE.md](docs/CLIENT_GUIDE.md)** — the canonical client
  guide. Every feature above with verified, copy-pasteable examples:
  construction and the five transports, unary and streaming requests, retry /
  idempotency / rate limiting, pooling and service discovery, observability,
  required-call contracts, and the error model.
- **[Go Reference (pkg.go.dev)](https://pkg.go.dev/github.com/lodgvideon/poseidon-http-client/client)**
  — full API docs and 30+ runnable / compile-tested examples.
- **[examples/loadgen](examples/loadgen)** — a runnable load generator: pooled
  client, rate limiting, hooks, a worker pool, and a metrics snapshot.
- **[CHANGELOG.md](CHANGELOG.md)** — release history (currently **v0.7.1**;
  every phase A–F released).

### Where each feature is documented

| I want to… | Guide section | Runnable example |
|---|---|---|
| Make a GET / POST | [Unary requests](docs/CLIENT_GUIDE.md#unary-requests-do) | `ExampleGET`, `ExamplePOST`, `Example_postJSON` |
| Reuse a `Response` under load | [The `Response` reuse contract](docs/CLIENT_GUIDE.md#the-response-struct-and-the-reset-reuse-contract) | `Example_reuseResponse` |
| Stream a response body | [Streaming responses](docs/CLIENT_GUIDE.md#streaming-responses--body-upload) | `Example_streamBodyReader`, `Example_streamingDownload` |
| Upload a request body | [Streaming the request body](docs/CLIENT_GUIDE.md#streaming-the-request-body-upload) | `Example_uploadBody` |
| Retry idempotent requests | [Retryer](docs/CLIENT_GUIDE.md#retryer) | `ExampleClient_Retryer`, `Example_retryOnRefusedStream` |
| Rate-limit / time out requests | [Rate limiting](docs/CLIENT_GUIDE.md#rate-limiting) | `Example_rateLimit`, `Example_requestTimeout` |
| Pool connections | [Connection pooling](docs/CLIENT_GUIDE.md#connection-pooling--service-discovery) | `ExampleNewPoolClient` |
| Discover backends via DNS | [Managed pool + resolvers](docs/CLIENT_GUIDE.md#4-managed-pool--transportmanaged--drainmode) | `ExampleNewManagedClient`, `ExampleStaticResolver` |
| Collect metrics / hooks | [Observability](docs/CLIENT_GUIDE.md#observability--advanced-protocol) | `ExampleHooks`, `ExampleClient_MetricsSnapshot`, `Example_successRate` |
| Handle server push | [Server push](docs/CLIENT_GUIDE.md#3-server-push-push_promise) | `Example_serverPush` |
| Send trailers | [Request trailers](docs/CLIENT_GUIDE.md#6-request-trailers) | `Example_requestTrailers` |
| Match errors | [Error model](docs/CLIENT_GUIDE.md#8-error-model) | `Example_errorsIs` |

## Contracts

A few invariants worth internalizing before you build on the API — the full
list lives in [the guide's Required-call contracts](docs/CLIENT_GUIDE.md#required-call-contracts):

- **Always `Close`** (or `Shutdown`) every `Client` you construct — otherwise
  you leak connections, reader goroutines, and the pool actor goroutine.
- **`Response.Reset()` before every reuse.** `Response.Headers` / `Body` and
  `StreamEvent` slices alias internal scratch buffers, valid only until the
  next `Reset()` / `Recv` / `Close`. Copy (`Response.CopyBody`) to retain.
- **A `Client` is goroutine-safe;** a single `Response` / `StreamResponse` is
  owned by one goroutine at a time. The `frame`/`hpack` codecs are not
  goroutine-safe — `conn.Conn` owns one of each and serializes access.

## Development

Requirements: **Go 1.24**, `golangci-lint` **v2.5** (optional, for `make lint`).

```bash
make tidy       # go mod tidy
make lint       # golangci-lint v2.5
make test-race  # go test -race ./...  (default verification)
make bench      # benches with the 0 B/op · 0 allocs/op gate on frame + hpack
```

Optional pre-commit hook:

```bash
git config core.hooksPath .githooks
```

See [docs/BENCH_BASELINE.md](docs/BENCH_BASELINE.md) for reference numbers and
[docs/COVERAGE.md](docs/COVERAGE.md) for the coverage policy. Architecture and
current milestone: [conn/doc.go](conn/doc.go).

## License

MIT — see [LICENSE](LICENSE).
