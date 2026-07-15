# poseidon-http-client

[![Go Reference](https://pkg.go.dev/badge/github.com/lodgvideon/poseidon-http-client.svg)](https://pkg.go.dev/github.com/lodgvideon/poseidon-http-client)
[![CI](https://github.com/lodgvideon/poseidon-http-client/actions/workflows/ci.yml/badge.svg)](https://github.com/lodgvideon/poseidon-http-client/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/lodgvideon/poseidon-http-client?sort=semver)](https://github.com/lodgvideon/poseidon-http-client/releases)
[![Go 1.25](https://img.shields.io/badge/go-1.25-00ADD8)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A low-level HTTP client for Go that implements HTTP/1.1, HTTP/2, and HTTP/3 from scratch — its own framing, HPACK, QPACK, and a from-scratch QUIC stack. No `net/http`, no third-party protocol libraries, no cgo. It is built for load generators and tools that need fine-grained control over connections, streams, and flow control, and exposes all three protocol versions through one `Do`/`DoStream` API.

## Why poseidon

- **One client, three protocol versions.** HTTP/1.1, HTTP/2, and HTTP/3 through the same `Do`/`DoStream` API. The Go standard library has no HTTP/3; most stacks bolt it on via a separate library.
- **No third-party protocol code.** Every protocol stack — QUIC (RFC 9000/9001/9002), HTTP/3 (RFC 9114), QPACK (RFC 9204), HTTP/2 framing (RFC 7540), HPACK (RFC 7541), HTTP/1.1 — is written from scratch in this module. No `quic-go`, no `nghttp2`, no `net/http`, no cgo; the TLS 1.3 handshake uses the standard library `crypto/tls`. The four direct dependencies are crypto and compression primitives, taken deliberately rather than hand-rolled: `golang.org/x/net`, `golang.org/x/crypto` (ChaCha20-Poly1305 packet protection), `github.com/andybalholm/brotli`, and `github.com/klauspost/compress` (zstd). A decompressor is a prime attack surface; reimplementing Poly1305 or Brotli's 122 KB dictionary would add risk, not remove it.
- **Zero-alloc codec.** The wire codec for both protocol versions — HTTP/2 frame + HPACK, and QUIC + HTTP/3 frames + QPACK — runs at 0 B/op, 0 allocs/op, enforced by a CI bench gate. This matters at load-generator request rates.
- **Fine-grained control.** Direct access to streams, flow-control windows, SETTINGS, pooling policy, and congestion control (NewReno default, opt-in BBR) — knobs `net/http` hides.
- **Load-generation features built in.** Connection pooling, DNS service discovery (Resolver/Selector), bounded retries, token-bucket rate limiting, lifecycle hooks, and metrics — shared across HTTP/2 and HTTP/3.
- **Conformance-tested.** ~200 conformance tests keyed to RFC sections (CI-gated), a 3-server HTTP/3 interop matrix (Caddy/quic-go, nginx/C, aioquic/Python) over real UDP, fuzzed wire parsers, `-race` throughout.

If you want a general-purpose web client, use `net/http`. If you build load generators, or need HTTP/3 plus control over the wire, use poseidon.

## Install

Requires Go 1.25 or later.

```bash
go get github.com/lodgvideon/poseidon-http-client
```

## Quick start

### HTTP/2

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	c, err := client.NewSingleConnClient(
		"www.cloudflare.com:443",
		&conn.TLSDialer{Config: &tls.Config{
			ServerName: "www.cloudflare.com",
			NextProtos: []string{"h2"},
		}},
	)
	if err != nil {
		log.Fatalf("build client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Response is caller-owned and reusable; Reset() clears it between requests.
	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/"), resp); err != nil {
		log.Fatalf("GET /: %v", err)
	}
	fmt.Printf("HTTP/2 %d — %d bytes\n", resp.Status, len(resp.Body))
}
```

### HTTP/3

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/quic"
)

func main() {
	// The simple path: one QUIC connection, buffered response.
	//
	//	c, err := client.NewH3Client("www.cloudflare.com:443",
	//	    &tls.Config{ServerName: "www.cloudflare.com"})
	//
	// Below uses NewClient so we can also select BBR congestion control.
	c, err := client.NewClient(client.ClientOptions{
		Addr:      "www.cloudflare.com:443",
		Transport: client.TransportH3,
		TLSConfig: &tls.Config{ServerName: "www.cloudflare.com"},
		H3ConnOptions: []quic.ConnOption{
			quic.WithCongestionControl(quic.CCBBR), // default is NewReno
		},
	})
	if err != nil {
		log.Fatalf("build client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/"), resp); err != nil {
		log.Fatalf("GET /: %v", err)
	}
	fmt.Printf("HTTP/3 %d — %d bytes\n", resp.Status, len(resp.Body))
}
```

### HTTP/1.1

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	c, err := client.NewClient(client.ClientOptions{
		Addr:      "example.com:443",
		Transport: client.TransportH1SingleConn,
		ConnOpts: conn.ConnOptions{
			// Offer only http/1.1 so the server cannot select h2.
			Dialer: &conn.TLSDialer{Config: &tls.Config{
				ServerName: "example.com",
				NextProtos: []string{"http/1.1"},
			}},
		},
	})
	if err != nil {
		log.Fatalf("build client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/"), resp); err != nil {
		log.Fatalf("GET /: %v", err)
	}
	fmt.Printf("HTTP/1.1 %d — %d bytes\n", resp.Status, len(resp.Body))
}
```

Runnable versions of all three live in [`examples/`](examples/).

## Features

| | HTTP/1.1 | HTTP/2 | HTTP/3 |
|---|---|---|---|
| Spec | HTTP/1.1 over TLS | RFC 7540 + HPACK (RFC 7541) | RFC 9114 + QUIC (RFC 9000/9001/9002) + QPACK (RFC 9204) |
| Constructors | `NewH1Client`, `NewH1PoolClient`, `NewManagedH1Client` | `NewSingleConnClient`, `NewPoolClient`, `NewManagedClient` | `NewH3Client`, `NewH3PoolClient`, `NewManagedH3Client` |
| Concurrency model | Exclusive-checkout pool: one exchange per connection (no pipelining), so `MaxConnsPerHost` is the request concurrency; a request waits, bounded by ctx, for a free connection | Stream multiplexing, `MAX_CONCURRENT_STREAMS` gating | Concurrent in-flight requests over one QUIC connection |
| Streaming | Request bodies stream (`BodyReader`, chunked); responses are always buffered — `DoStream` and `BodyMode: BodyStream` return an error | `DoStream`, body streaming, request trailers | `DoStream`, body streaming |
| Flow control | — | Full bidirectional, dynamic SETTINGS, GOAWAY drain | QUIC stream + connection flow control |
| Header compression | — | HPACK, zero-alloc | Dynamic QPACK, both directions (encode + decode), zero-alloc |
| Extras | ALPN fallback: `TransportALPN` falls back to HTTP/1.1 when the server does not offer h2 | Server push (PUSH_PROMISE), request priority, extended CONNECT (RFC 8441, WebSockets over H2), CONTINUATION, HTTP CONNECT proxy dialers, H2C prior knowledge, PING keepalive | AES-128-GCM / AES-256-GCM / ChaCha20-Poly1305; NewReno default, opt-in BBR; GSO/GRO batched UDP and bounded ACK coalescing on Linux |

Shared across protocols:

- **Request API** — `client.GET(path)`, or `client.Request{Method, Scheme, Authority, Path, BodyMode}` (`BodyBuffer` or `BodyStream`). `Response{Status, Body}` is caller-owned; `Reset()` reuses it across requests.
- **Compression, both directions** — responses in gzip, deflate, br, and zstd are decoded automatically (the client advertises `accept-encoding: gzip, deflate, br, zstd`); a decompression-bomb guard (`WithMaxDecompressedSize`, default 10 MiB) rejects over-expanding bodies with `ErrBodyTooLarge`. Request bodies compress on demand: set `Request.CompressBody` to `EncodingGzip`, `EncodingDeflate`, `EncodingBrotli`, or `EncodingZstd` and the client compresses the body and sets `content-encoding` itself; the zero value sends the body unchanged at zero extra allocations. Setting `content-encoding` manually means "this body is already encoded" and it is left untouched; setting both returns `ErrConflictingContentEncoding`. Identical over HTTP/1.1, HTTP/2, and HTTP/3.

  ```go
  c.Do(ctx, &client.Request{
      Method: "POST", Path: "/ingest",
      Body:   payload,
      CompressBody: client.EncodingZstd, // client sets content-encoding itself
  }, &resp)
  ```

- **Pooling and discovery** — per-host pools (`PoolOptions{MaxConnsPerHost, MaxStreamsPerConn, HealthCheckPeriod}`) with least-loaded stream pick and idle eviction; Resolver + Selector service discovery.
- **Resilience** — opt-in `Retryer` (bounded retries of idempotent requests on REFUSED_STREAM / GOAWAY / dial errors, exponential backoff + jitter), `WithRateLimit(perSecond, burst)`, per-request timeouts via context.
- **Observability** — `Client.Hooks` (OnDial, OnRequestComplete, OnRetry, …), `Client.MetricsSnapshot()` (counters + latency histogram), `Client.PoolStats()`.
- **Codec-only use** — the `frame`, `hpack`, and `qpack` packages work standalone; the frame, HPACK, and QPACK hot paths — along with the QUIC and HTTP/3 frame codecs — are bench-gated at 0 B/op, 0 allocs/op.

## Documentation

Full documentation: **https://lodgvideon.github.io/poseidon-http-client/**

Available in English, 中文, Español, Русский, and 日本語.

## Non-goals

Deliberately out of scope for 1.0:

- 0-RTT / session resumption
- QUIC connection migration
- HTTP/3 server push

The client never initiates these; a peer offering them is simply not engaged — nothing fails. An unsupported TLS cipher suite fails cleanly with a typed `ErrCryptoSuite` (no hang, no panic).

## Disclaimer

This is young software — v1.0 is its first release. It is provided **as is, use at your own risk** (MIT license, no warranty). It implements security-sensitive protocols (TLS 1.3, QUIC, HPACK/QPACK) from scratch; while conformance-tested, fuzzed, and interop-verified, it has **not had a formal third-party security audit**. Do not treat it as a drop-in replacement for `net/http` in security-critical production without your own review. Report vulnerabilities privately — see [SECURITY.md](SECURITY.md) — not via public issues.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE).
