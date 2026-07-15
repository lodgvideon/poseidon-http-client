---
title: Features & advantages
weight: 5
---

# Features & advantages

## Support matrix

All three protocol versions share the same request API: `client.Do` / `client.DoStream` with a caller-owned, reusable `client.Response`. Compression is shared too: responses are decoded for all of gzip, deflate, br, and zstd (the client advertises `accept-encoding: gzip, deflate, br, zstd`), and `Request.CompressBody` compresses request bodies — see [Compression](#compression) below. What differs is how much of the protocol each transport exposes.

| Protocol | Implementation | Constructors | Concurrent requests per connection | Pooling | Service discovery | Notable capabilities |
|---|---|---|---|---|---|---|
| HTTP/1.1 | From scratch | `NewH1Client`, `NewH1PoolClient`, `NewManagedH1Client` | No — one exchange per connection at a time (no pipelining) | `NewH1PoolClient` (exclusive-checkout pool: `MaxConnsPerHost` is the request concurrency) | `NewManagedH1Client` (Resolver + Selector) | Keep-alive connection reuse; request bodies stream (`Request.BodyReader`, chunked when length is unknown) but responses are always buffered into `Response.Body` — `DoStream` and `BodyStream` return an error; ALPN fallback target: `TransportALPN` selects HTTP/1.1 automatically when a server does not offer h2 |
| HTTP/2 | RFC 7540 + HPACK (RFC 7541), from scratch | `NewSingleConnClient`, `NewPoolClient`, `NewManagedClient` | Yes — stream multiplexing, gated by `MAX_CONCURRENT_STREAMS` | `NewPoolClient` (per-host pool, least-loaded stream pick, idle eviction) | `NewManagedClient` (Resolver + Selector) | `DoStream` and request trailers; flow control; dynamic SETTINGS; GOAWAY drain; PING keepalive; server push (PUSH_PROMISE); request priority; extended CONNECT (RFC 8441, WebSockets over H2); CONTINUATION; HTTP CONNECT proxy dialers; h2c prior knowledge |
| HTTP/3 | RFC 9114 + QUIC (RFC 9000/9001/9002) + QPACK (RFC 9204), from scratch | `NewH3Client`, `NewH3PoolClient`, `NewManagedH3Client` | Yes — concurrent in-flight requests over one QUIC connection | `NewH3PoolClient` (multi-connection pool) | `NewManagedH3Client` | `DoStream`; dynamic QPACK in both directions (encode + decode); all TLS 1.3 AEADs (AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305); congestion control NewReno by default, opt-in BBR; on Linux: GSO batched send, GRO batched receive, bounded ACK coalescing |

HTTP/1.1 has the same constructor set as the other two versions, but its pool is different in kind. HTTP/1.1 has no multiplexing: a single connection means strictly serial requests, so without a pool the client cannot generate HTTP/1.1 load at all. The pool hands out connections by exclusive checkout — one exchange per connection at a time — which makes `MaxConnsPerHost` the request concurrency; `MaxStreamsPerConn` does not apply. A request that finds every connection busy waits for one to free, bounded by the request context. Connections are kept alive and reused; a `Connection: close`, a dead connection, or an exchange error discards the connection and redials. Pipelining is deliberately not implemented. The dialer must not offer the `h2` ALPN token — use a plain TCP dialer, or a TLS dialer whose `NextProtos` is only `"http/1.1"`.

Opting into BBR on HTTP/3:

```go
client.ClientOptions{
    Transport: client.TransportH3,
    H3ConnOptions: []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)},
}
```

## Compression

Compression works identically over HTTP/1.1, HTTP/2, and HTTP/3.

**Responses.** The client advertises `accept-encoding: gzip, deflate, br, zstd` and decodes all four, with pooled readers. A caller-supplied accept-encoding header wins; `Request.DisableDecompression` suppresses both the header and the decoding. A decompression-bomb guard rejects bodies that expand past `MaxDecompressedSize` (default 10 MiB) with `ErrBodyTooLarge`, and the zstd window is capped at 8 MiB. `Content-Encoding` matching is case-insensitive (RFC 9110 §8.4.1).

**Requests.** Set `Request.CompressBody` and the client compresses the body and sets `content-encoding` itself:

```go
var resp client.Response
err := c.Do(ctx, &client.Request{
    Method: "POST", Path: "/ingest",
    Body:   payload,
    CompressBody: client.EncodingZstd, // client sets content-encoding itself
}, &resp)
```

`EncodingGzip`, `EncodingDeflate`, `EncodingBrotli`, and `EncodingZstd` are accepted. The zero value, `EncodingIdentity`, sends the body unchanged — callers who do not opt in pay nothing (0 allocations). Setting `content-encoding` manually still means "this body is already encoded", and the body is left untouched (RFC 9110 §8.4 — Content-Encoding describes the body; it is not an instruction). Setting both `CompressBody` and a manual `content-encoding` returns `ErrConflictingContentEncoding`. content-length is the compressed size for a buffered body, and is omitted for a streaming body (HTTP/1.1 then uses chunked transfer-encoding).

## Why poseidon

**One client, three protocol versions.** HTTP/1.1, HTTP/2, and HTTP/3 through the same `Do`/`DoStream` API. The Go standard library has no HTTP/3; most stacks bolt it on via a separate library with a separate API. Here, switching a load test from h2 to h3 is a constructor change, not a rewrite.

**No third-party protocol code.** Every protocol stack is written from scratch, in this module: QUIC (RFC 9000/9001/9002), HTTP/3 (RFC 9114), QPACK (RFC 9204), HTTP/2 framing (RFC 7540) with HPACK (RFC 7541), and HTTP/1.1. No `quic-go`, no `nghttp2`, no `net/http`, no cgo. The TLS 1.3 handshake uses the standard library `crypto/tls`. The four direct dependencies are crypto and compression primitives, taken deliberately rather than hand-rolled: `golang.org/x/net`, `golang.org/x/crypto` (ChaCha20-Poly1305 packet protection), `github.com/andybalholm/brotli`, and `github.com/klauspost/compress` (zstd). Reimplementing Poly1305 or Brotli would be a security liability with no upside — Brotli requires a 122 KB static dictionary plus 121 transforms, klauspost's zstd has years of fuzzing behind it, and a decompressor is a prime attack surface. So the boundary is: all protocol code is ours, auditable in one module; the crypto and compression primitives are borrowed, because that is where borrowing is the safer engineering call.

**Zero-alloc codec.** The whole wire codec runs at 0 B/op, 0 allocs/op for both protocol versions — HTTP/2 (frame and HPACK encode/decode) and HTTP/3 (QUIC frame and packet-header parsing and serialization, HTTP/3 frames, QPACK field sections) — and a CI bench gate fails the build if that regresses. At high request rates in a load generator, per-frame allocations show up directly as GC pressure; this codec does not contribute any. The `frame`, `hpack`, and `qpack` packages are usable standalone. One honest boundary: the QUIC packet send path (building and encrypting an outgoing packet) is low-alloc, not zero — the zero-alloc claim covers the codec, not the whole request.

**Fine-grained control.** Direct access to streams, flow-control windows, SETTINGS, pooling policy, congestion control (NewReno or BBR), and pacing — knobs `net/http` hides behind its transport. If your tool needs to hold a window closed, pin stream concurrency, or measure the effect of a congestion controller, the levers are exposed.

**Load-generation features built in.** Connection pooling, DNS service discovery (Resolver/Selector), opt-in bounded retries of idempotent requests, token-bucket rate limiting (`WithRateLimit`), lifecycle hooks (`Client.Hooks`), and metrics (`Client.MetricsSnapshot()`, `Client.PoolStats()`). All of it is shared across HTTP/1.1, HTTP/2, and HTTP/3 — you configure it once, not per protocol.

**Conformance-tested.** About 200 conformance tests keyed to specific RFC sections, gated in CI. A three-server HTTP/3 interop matrix (Caddy/quic-go, nginx/C, aioquic/Python) runs over real UDP. Wire parsers are fuzzed. The whole suite runs under `-race`.

## Compared with net/http

`net/http` is the batteries-included standard client. It handles redirects, cookies, proxies from the environment, and HTTP/1.1 + HTTP/2 negotiation with no configuration. poseidon trades that convenience for control: it adds HTTP/3, a zero-alloc codec, and load-generation tooling, and in exchange asks you to construct clients per target and manage responses yourself. If you want a general-purpose web client, use `net/http`. If you build load generators or need HTTP/3 with fine control, use poseidon.

## Compared with quic-go

`quic-go` is a mature, widely-used QUIC and HTTP/3 library, covering both server and client. poseidon reimplements QUIC to keep all protocol code in this module and to stay load-generation-focused. It is younger and narrower: an HTTP client. The `quic` package does expose a server role (`Listen` / `Accept`, stream accept) — it exists to give the client a real peer in tests — but there is no HTTP/3 server on top of it. If you need a battle-tested QUIC stack or an HTTP/3 server, `quic-go` is the established choice.

## Non-goals for 1.0

The following are deliberately out of scope for this release:

- **0-RTT / session resumption.** The client never initiates it.
- **QUIC connection migration.** Not initiated.
- **HTTP/3 server push.** Not engaged.

A peer offering any of these is simply not engaged — nothing fails. An unsupported TLS cipher suite fails cleanly with a typed `ErrCryptoSuite`; there is no hang and no panic.
