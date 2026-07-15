---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

Use HTTP/1.1 when the target server does not speak HTTP/2, or when you
want to test its HTTP/1.1 path specifically and must prevent the ALPN
negotiation from upgrading the connection to h2.

HTTP/1.1 has the same constructor set as HTTP/2 and HTTP/3:

| Constructor | Transport | Shape |
|---|---|---|
| `NewH1Client(addr, dialer, opts...)` | `TransportH1SingleConn` | One connection, auto-redial. Requests are serialized. |
| `NewH1PoolClient(addr, dialer, pool, opts...)` | `TransportH1Pool` | Up to `MaxConnsPerHost` connections. That number is the request concurrency. |
| `NewManagedH1Client(resolver, dialer, opts...)` | `TransportH1Managed` | Resolver + Selector service discovery; a per-address HTTP/1.1 sub-pool fans out. |

## Forcing HTTP/1.1

Two things pin the protocol:

1. An HTTP/1.1 transport — one of the constructors above, or the
   corresponding `Transport` value in `client.ClientOptions`.
2. A dialer that does not offer `h2`: a plain TCP dialer
   (`&conn.PlaintextDialer{}`), or a TLS dialer whose `NextProtos`
   offers only `"http/1.1"`, so the server cannot select `h2` during
   the handshake.

```go
// Command http1-example issues a single HTTP/1.1 request with the poseidon
// client. HTTP/1.1 is reached by pinning the transport to TransportH1SingleConn
// and offering only the "http/1.1" ALPN token, so the connection never upgrades
// to HTTP/2.
//
//	go run ./examples/http1
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

`client.NewH1Client(addr, dialer)` builds the same single-connection
client in one call. The request API is the same `Do` / `client.GET` /
`client.Response` used for HTTP/2 and HTTP/3, so switching a test
target between protocol versions is a constructor change, not a
rewrite.

## One connection means serial requests

HTTP/1.1 has no multiplexing: a connection carries exactly one
request/response exchange at a time. Pipelining is deliberately
unsupported. So `TransportH1SingleConn` serializes requests — each
request completes before the next one is written, and a second
concurrent `Do` waits for the first to finish. On a single connection,
HTTP/1.1 load is strictly serial. For concurrency, use the pool.

## Pooled connections

```go
c, err := client.NewH1PoolClient("example.com:443",
	&conn.TLSDialer{Config: &tls.Config{
		ServerName: "example.com",
		NextProtos: []string{"http/1.1"},
	}},
	client.PoolOptions{MaxConnsPerHost: 32},
)
if err != nil {
	log.Fatalf("build client: %v", err)
}
defer func() { _ = c.Close() }()
// Up to 32 requests in flight. The 33rd waits for a connection to free.
```

Unlike the HTTP/2 and HTTP/3 pools, this is an exclusive-checkout
pool. The H2/H3 pools hand the same connection to many concurrent
streams and pick the least-loaded one; the HTTP/1.1 pool takes a
connection out of the idle set for the duration of an exchange and
returns it on completion. Consequences:

- `MaxConnsPerHost` **is** the request concurrency. It is the only
  sizing knob that matters here, and the pool never dials past it.
- `MaxStreamsPerConn` does not apply to HTTP/1.1 and is ignored
  (`PoolOptions` is shared with the H2/H3 pools; the per-connection
  cap here is always 1).
- A request that finds every connection busy waits for one to free,
  bounded by its ctx (and `PoolOptions.AcquireTimeout`, if set). It is
  never serialized onto a busy connection.

Connections are kept alive and reused across exchanges. A connection
is discarded and redialed instead of reused when the response says it
will not persist (`Connection: close`, or HTTP/1.0 without
keep-alive), when the peer closed the socket, or after any exchange
error. Idle eviction and health-check sweeps match the H2/H3 pools.

For multi-address targets, `NewManagedH1Client(resolver, dialer)`
runs one exclusive-checkout sub-pool per resolved address;
`MaxConnsPerHost` then sets the per-address concurrency.

## Streaming

Response streaming is not available on HTTP/1.1: responses are always
buffered into `Response.Body`. `DoStream` returns an error on an
HTTP/1.1 transport, as does `Do` with `Request.BodyMode:
client.BodyStream`. This is deliberate — if you need to stream
responses, use HTTP/2 or HTTP/3. Request bodies do stream: a
`Request.BodyReader` is sent with `Transfer-Encoding: chunked` when
its length is not known up front, which is also how
`Request.CompressBody` sends a compressed streaming body.

## Automatic fallback

You do not need an HTTP/1.1 transport just to talk to a server that
happens to lack HTTP/2. `client.TransportALPN` negotiates via ALPN and
falls back to HTTP/1.1 on its own when the server does not offer `h2`.
Pin the transport only when you want a guarantee that the connection
stays on HTTP/1.1.
