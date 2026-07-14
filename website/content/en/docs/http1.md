---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

Use HTTP/1.1 when the target server does not speak HTTP/2, or when you
want to test its HTTP/1.1 path specifically and must prevent the ALPN
negotiation from upgrading the connection to h2.

## Forcing HTTP/1.1

Two things pin the protocol:

1. `Transport: client.TransportH1SingleConn` in `client.ClientOptions`.
2. A TLS dialer whose `NextProtos` offers only `"http/1.1"`, so the
   server cannot select `h2` during the handshake.

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

The request API is the same `Do` / `client.GET` / `client.Response` used
for HTTP/2 and HTTP/3, so switching a test target between protocol
versions is a constructor change, not a rewrite.

## Serialization

`TransportH1SingleConn` runs one connection and serializes requests on
it: each request completes before the next one is written. There is no
pipelining. For concurrent load against an HTTP/1.1 server, run
multiple clients.

## Automatic fallback

You do not need `TransportH1SingleConn` just to talk to a server that
happens to lack HTTP/2. `client.TransportALPN` negotiates via ALPN and
falls back to HTTP/1.1 on its own when the server does not offer `h2`.
Pin the transport only when you want a guarantee that the connection
stays on HTTP/1.1.
