---
title: Features & advantages
weight: 5
---

# Features & advantages

## Support matrix

All three protocol versions share the same request API: `client.Do` / `client.DoStream` with a caller-owned, reusable `client.Response`. What differs is how much of the protocol each transport exposes.

| Protocol | Implementation | Constructors | Concurrent requests per connection | Pooling | Service discovery | Notable capabilities |
|---|---|---|---|---|---|---|
| HTTP/1.1 | From scratch | `NewClient` with `TransportH1SingleConn` | No — one connection, requests serialized (no pipelining) | No | No | ALPN fallback target: `TransportALPN` selects HTTP/1.1 automatically when a server does not offer h2 |
| HTTP/2 | RFC 7540 + HPACK (RFC 7541), from scratch | `NewSingleConnClient`, `NewPoolClient`, `NewManagedClient` | Yes — stream multiplexing, gated by `MAX_CONCURRENT_STREAMS` | `NewPoolClient` (per-host pool, least-loaded stream pick, idle eviction) | `NewManagedClient` (Resolver + Selector) | `DoStream` and request trailers; flow control; dynamic SETTINGS; GOAWAY drain; PING keepalive; server push (PUSH_PROMISE); request priority; extended CONNECT (RFC 8441, WebSockets over H2); CONTINUATION; HTTP CONNECT proxy dialers; h2c prior knowledge |
| HTTP/3 | RFC 9114 + QUIC (RFC 9000/9001/9002) + QPACK (RFC 9204), from scratch | `NewH3Client`, `NewH3PoolClient`, `NewManagedH3Client` | Yes — concurrent in-flight requests over one QUIC connection | `NewH3PoolClient` (multi-connection pool) | `NewManagedH3Client` | `DoStream`; dynamic QPACK in both directions (encode + decode); all TLS 1.3 AEADs (AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305); congestion control NewReno by default, opt-in BBR; on Linux: GSO batched send, GRO batched receive, bounded ACK coalescing |

HTTP/1.1 support is deliberately minimal — it exists so that a load test can hit the same target over all three versions with one codebase, and as the fallback for `TransportALPN`. HTTP/2 and HTTP/3 are the full-featured transports.

Opting into BBR on HTTP/3:

```go
client.ClientOptions{
    Transport: client.TransportH3,
    H3ConnOptions: []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)},
}
```

## Why poseidon

**One client, three protocol versions.** HTTP/1.1, HTTP/2, and HTTP/3 through the same `Do`/`DoStream` API. The Go standard library has no HTTP/3; most stacks bolt it on via a separate library with a separate API. Here, switching a load test from h2 to h3 is a constructor change, not a rewrite.

**From scratch, near-zero dependencies.** No `quic-go`, no `nghttp2`, no cgo. Direct dependencies are `golang.org/x/net` and `golang.org/x/crypto` (the latter only for ChaCha20-Poly1305 packet protection); the TLS 1.3 handshake uses the standard library `crypto/tls`. The protocol code is all in this module — auditable, with a small surface and no transitive supply-chain sprawl.

**Zero-alloc codec.** Frame and HPACK encode/decode run at 0 B/op, 0 allocs/op, and a CI bench gate fails the build if that regresses. At high request rates in a load generator, per-frame allocations show up directly as GC pressure; this codec does not contribute any. The `frame`, `hpack`, and `qpack` packages are usable standalone.

**Fine-grained control.** Direct access to streams, flow-control windows, SETTINGS, pooling policy, congestion control (NewReno or BBR), and pacing — knobs `net/http` hides behind its transport. If your tool needs to hold a window closed, pin stream concurrency, or measure the effect of a congestion controller, the levers are exposed.

**Load-generation features built in.** Connection pooling, DNS service discovery (Resolver/Selector), opt-in bounded retries of idempotent requests, token-bucket rate limiting (`WithRateLimit`), lifecycle hooks (`Client.Hooks`), and metrics (`Client.MetricsSnapshot()`, `Client.PoolStats()`). All of it is shared across HTTP/2 and HTTP/3 — you configure it once, not per protocol.

**Conformance-tested.** About 200 conformance tests keyed to specific RFC sections, gated in CI. A three-server HTTP/3 interop matrix (Caddy/quic-go, nginx/C, aioquic/Python) runs over real UDP. Wire parsers are fuzzed. The whole suite runs under `-race`.

## Compared with net/http

`net/http` is the batteries-included standard client. It handles redirects, cookies, proxies from the environment, and HTTP/1.1 + HTTP/2 negotiation with no configuration. poseidon trades that convenience for control: it adds HTTP/3, a zero-alloc codec, and load-generation tooling, and in exchange asks you to construct clients per target and manage responses yourself. If you want a general-purpose web client, use `net/http`. If you build load generators or need HTTP/3 with fine control, use poseidon.

## Compared with quic-go

`quic-go` is a mature, widely-used QUIC and HTTP/3 library, covering both server and client. poseidon reimplements QUIC to stay dependency-free and load-generation-focused. It is younger and narrower: client only, no server. If you need a battle-tested QUIC stack or a server, `quic-go` is the established choice.

## Non-goals for 1.0

The following are deliberately out of scope for this release:

- **0-RTT / session resumption.** The client never initiates it.
- **QUIC connection migration.** Not initiated.
- **HTTP/3 server push.** Not engaged.

A peer offering any of these is simply not engaged — nothing fails. An unsupported TLS cipher suite fails cleanly with a typed `ErrCryptoSuite`; there is no hang and no panic.
