---
title: Getting started
weight: 1
---

# Getting started

poseidon-http-client is a low-level HTTP client for Go. It implements HTTP/1.1, HTTP/2, and HTTP/3 from scratch — its own framing, HPACK, QPACK, and QUIC stack — with no `net/http` and no third-party protocol libraries. It is built for load generators and tools that need direct control over connections, streams, and flow control.

## Install

Requires Go 1.25 or newer.

```bash
go get github.com/lodgvideon/poseidon-http-client
```

## The request model

All three protocol versions share the same API. You build a client with a constructor, then call `Do` with a context, a request, and a caller-owned `*client.Response`:

```go
c, err := client.NewSingleConnClient(addr, dialer)   // or any constructor above
defer c.Close()
resp := &client.Response{}                            // caller-owned, reusable
err = c.Do(ctx, client.GET("/path"), resp)            // resp.Status, resp.Body
resp.Reset()                                          // reuse for the next request
```

Two things differ from `net/http`:

- **The response is caller-owned.** You allocate one `client.Response`, pass it to `Do`, read `resp.Status` (an `int`) and `resp.Body` (a `[]byte`), then call `resp.Reset()` to reuse it for the next request. No response object is allocated per call — in a request loop, one `Response` can serve every iteration.
- **Requests are values, not pointers into a transport.** `client.GET(path)` builds a GET request. For full control, construct `client.Request{Method, Scheme, Authority, Path, BodyMode}` directly.

## First request over HTTP/2

This is `examples/http2/main.go` from the repository, unmodified. It issues one GET against a public HTTP/2 endpoint:

```go
// Command http2-example issues a single HTTP/2 GET with the poseidon client.
// TransportSingleConn (the default) negotiates h2 over TLS and reuses one
// connection with automatic redial.
//
//	go run ./examples/http2
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

The dialer decides the protocol here: `conn.TLSDialer` with `NextProtos: []string{"h2"}` offers only HTTP/2 in the ALPN negotiation. The client keeps one connection open and redials automatically if it drops.

## Picking a protocol

The constructor you call determines the protocol version:

| Constructor | Protocol | Connection model |
|---|---|---|
| `client.NewH1Client(addr, dialer, opts...)` | HTTP/1.1 | One connection, requests serialized |
| `client.NewH1PoolClient(addr, dialer, pool, opts...)` | HTTP/1.1 | Exclusive-checkout pool, one request per connection |
| `client.NewManagedH1Client(resolver, dialer, opts...)` | HTTP/1.1 | Service discovery (Resolver + Selector) |
| `client.NewSingleConnClient(addr, dialer, opts...)` | HTTP/2 | One connection, auto-redial |
| `client.NewPoolClient(addr, dialer, pool, opts...)` | HTTP/2 | Per-host connection pool |
| `client.NewManagedClient(resolver, dialer, opts...)` | HTTP/2 | Service discovery (Resolver + Selector) |
| `client.NewH3Client(addr, tlsConfig, opts...)` | HTTP/3 | One QUIC connection |
| `client.NewH3PoolClient(addr, tlsConfig, pool, opts...)` | HTTP/3 | Multi-connection QUIC pool |
| `client.NewManagedH3Client(resolver, tlsConfig, opts...)` | HTTP/3 | Service discovery over QUIC |

Notes:

- For HTTP/2 constructors, the dialer's `NextProtos` must offer `"h2"`. `client.TransportALPN` falls back to HTTP/1.1 if the server does not offer h2.
- HTTP/3 constructors take a `*tls.Config` directly; the transport is QUIC over UDP.
- All constructors return a client with the same `Do` / `DoStream` methods.

## Where to go next

Each protocol has its own guide with the full verified example, streaming, pooling, and the options specific to that version:

- [HTTP/1.1 guide](/docs/http1/)
- [HTTP/2 guide](/docs/http2/)
- [HTTP/3 guide](/docs/http3/)
