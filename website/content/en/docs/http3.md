---
title: HTTP/3
weight: 4
---

# HTTP/3

The HTTP/3 client (RFC 9114) runs over a QUIC stack (RFC 9000/9001/9002) and QPACK codec (RFC 9204) implemented in this repository — no `quic-go`, no cgo. Only the TLS 1.3 handshake itself comes from the standard library `crypto/tls`; packet protection, loss recovery, congestion control, and flow control are poseidon code.

The request API is the same `Do` / `DoStream` you use with HTTP/2. A `client.Request` and `client.Response` do not change between transports; switching a load test from H2 to H3 is a constructor change.

## Example

`examples/http3/main.go`:

```go
// Command http3-example issues a single HTTP/3 GET over QUIC with the poseidon
// client, and shows how to opt into BBR congestion control.
//
//	go run ./examples/http3
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

## Constructors

Three constructors, mirroring the HTTP/2 set:

```go
// One QUIC connection. Buffered (Do) and streaming (DoStream) requests.
client.NewH3Client(addr string, tlsConfig *tls.Config, opts ...Option) (*Client, error)

// Pool of QUIC connections to one host.
client.NewH3PoolClient(addr string, tlsConfig *tls.Config, pool PoolOptions, opts ...Option) (*Client, error)

// Service discovery: a Resolver supplies addresses, requests spread across them.
client.NewManagedH3Client(resolver Resolver, tlsConfig *tls.Config, opts ...Option) (*Client, error)
```

For settings the constructors do not expose — such as congestion control — build with `client.NewClient(client.ClientOptions{...})` and set `Transport: client.TransportH3`, as in the example above.

## Concurrent requests

One HTTP/3 connection carries multiple in-flight requests at once, each on its own QUIC stream. Issue `Do` / `DoStream` from concurrent goroutines against the same client; the pool and managed variants additionally spread streams across connections.

## Cipher suites

All three TLS 1.3 AEAD suites are supported for QUIC packet protection: AES-128-GCM, AES-256-GCM, and ChaCha20-Poly1305. The server picks during the handshake; no configuration is needed on the client. A suite outside this set (for example TLS_AES_128_CCM_8_SHA256) fails at key install with the typed error `quic.ErrCryptoSuite` — no hang, no panic.

## QPACK

Header compression uses dynamic-table QPACK in both directions: the client encodes request headers against the dynamic table and decodes server insertions on the decoder stream. This happens automatically; there is no knob to turn.

## Allocations

The HTTP/3 wire codec is zero-alloc: QUIC frames and packet headers, HTTP/3 frames, and QPACK field sections encode and decode at 0 B/op, 0 allocs/op. The same CI bench gate that enforces this for the HTTP/2 frame and HPACK codec covers the `qpack`, `quic`, and `http3` packages. The QUIC packet send path is the exception: building and sealing an outgoing packet allocates a small, bounded amount per packet. A request over HTTP/3 is therefore low-alloc, not allocation-free.

## Congestion control

The default is NewReno. BBR is available opt-in:

```go
H3ConnOptions: []quic.ConnOption{
	quic.WithCongestionControl(quic.CCBBR),
},
```

BBR ships correct and tested, but its throughput advantage over NewReno only shows on a bottlenecked WAN path with real queuing delay. On a LAN or loopback you will not measure a difference. If you cannot benchmark your target path, keep the NewReno default.

## Linux batched I/O

On Linux the QUIC transport uses GSO (generic segmentation offload) to hand the kernel batches of outgoing packets in one syscall, and GRO to receive coalesced batches. Both are automatic — no configuration, no build tags. On other platforms the client sends and receives one datagram per syscall.

## Non-goals

Deliberately out of scope for 1.0:

- **0-RTT / session resumption** — every connection does a full handshake.
- **QUIC connection migration** — a connection is bound to the path it was opened on.
- **HTTP/3 server push** — never enabled.

The client never initiates any of these. A peer offering them is simply not engaged; nothing fails, no error surfaces. The one hard failure mode in this area is an unsupported cipher suite, which returns the typed `quic.ErrCryptoSuite` described above.
