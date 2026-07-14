---
title: Documentation
weight: 1
bookCollapseSection: false
---

# Documentation

poseidon-http-client is a low-level HTTP client for Go that implements HTTP/1.1, HTTP/2, and HTTP/3 from scratch — its own framing, HPACK, QPACK, and QUIC stack, with no `net/http` and no third-party protocol libraries. All three protocol versions sit behind the same `Do`/`DoStream` API: you choose the transport, the request code stays the same. These pages cover installation, one page per protocol with a verified example, the feature set shared across transports (pooling, service discovery, retries, rate limiting, hooks, metrics), and a disclaimer you should read before using a first release anywhere security-critical.

- [Getting started](getting-started/) — install, requirements, first request.
- [HTTP/1.1](http1/) — single-connection transport, TLS dialer setup, ALPN fallback.
- [HTTP/2](http2/) — single-conn, pooled, and discovery-managed clients; streaming and flow control.
- [HTTP/3](http3/) — QUIC-based clients, dynamic QPACK, NewReno/BBR congestion control.
- [Features](features/) — the request API, pooling, resolver/selector discovery, retries, rate limiting, hooks, metrics.
- [Disclaimer](disclaimer/) — what "first release" means here, and how to report vulnerabilities.
