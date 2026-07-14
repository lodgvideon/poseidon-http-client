---
title: poseidon-http-client
type: docs
---

# poseidon-http-client

A low-level HTTP client for Go that implements HTTP/1.1, HTTP/2, and HTTP/3 from scratch — its own framing, HPACK, QPACK, and a from-scratch QUIC stack. It uses no `net/http` and no third-party protocol libraries; the only direct dependencies are `golang.org/x/net` and `golang.org/x/crypto` (ChaCha20-Poly1305), with TLS 1.3 from the standard library. All three protocol versions share one request API: `Do` and `DoStream`. It is built for load generators and tools that need fine-grained control over connections, streams, and flow control — not as a general-purpose replacement for `net/http`.

MIT licensed. Requires Go 1.25. Source: [github.com/lodgvideon/poseidon-http-client](https://github.com/lodgvideon/poseidon-http-client).

## Why poseidon

- **One client, three protocol versions** — HTTP/1.1, /2, and /3 through the same `Do`/`DoStream` API; the Go standard library has no HTTP/3.
- **From scratch, near-zero dependencies** — no `quic-go`, no `nghttp2`, no cgo; a small, auditable surface.
- **Zero-alloc wire codec** — HTTP/2 (frame, HPACK) and HTTP/3 (QPACK, HTTP/3 frames, QUIC frames and packet headers) encode/decode at 0 B/op, 0 allocs/op, enforced by a CI bench gate.
- **Fine-grained control** — streams, flow-control windows, SETTINGS, pooling policy, congestion control (NewReno or BBR); knobs `net/http` hides.
- **Load-generation features built in** — connection pooling, DNS service discovery, retries, rate limiting, hooks and metrics, shared across H2 and H3.
- **Conformance-tested** — ~200 RFC-keyed conformance tests, a 3-server HTTP/3 interop matrix (Caddy, nginx, aioquic) over real UDP, fuzzed wire parsers, `-race` throughout.

## Guides

- [Getting started]({{< relref "/docs/getting-started" >}})
- [HTTP/1.1]({{< relref "/docs/http1" >}})
- [HTTP/2]({{< relref "/docs/http2" >}})
- [HTTP/3]({{< relref "/docs/http3" >}})
- [Features & advantages]({{< relref "/docs/features" >}})
- [Disclaimer]({{< relref "/docs/disclaimer" >}})

{{< hint warning >}}
**Young software.** This is a first release. It implements security-sensitive protocols (TLS 1.3, QUIC, HPACK/QPACK) from scratch and has not had a third-party security audit. Provided as is, use at your own risk (MIT — no warranty). Read the [Disclaimer]({{< relref "/docs/disclaimer" >}}) before deploying.
{{< /hint >}}
