---
title: HTTP/2
weight: 3
---

# HTTP/2

The HTTP/2 client implements RFC 7540 and HPACK (RFC 7541) from scratch — no `net/http`, no `golang.org/x/net/http2`. Three constructors cover the common topologies: one connection, a per-host pool, or a resolver-driven set of backends. All three return a `*client.Client` with the same `Do` / `DoStream` API.

## Single connection

`client.NewSingleConnClient` holds one connection and redials automatically when it drops. This is `examples/http2/main.go`:

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

`Response` is caller-owned: allocate it once, call `resp.Reset()` between requests. `client.GET(path)` is shorthand; build a `client.Request{Method, Scheme, Authority, Path, BodyMode}` directly for full control.

## Connection pooling

`client.NewPoolClient` maintains a per-host pool. It assigns each request to the least-loaded connection, dials up to `MaxConnsPerHost`, and evicts idle connections on the health-check tick.

```go
c, err := client.NewPoolClient("api.example.com:443", dialer,
	client.PoolOptions{
		MaxConnsPerHost:   4,
		MaxStreamsPerConn: 100,
		HealthCheckPeriod: 30 * time.Second,
	})
```

`MaxStreamsPerConn` is a soft cap; the effective limit is the minimum of this value and the peer's `SETTINGS_MAX_CONCURRENT_STREAMS`. `c.Warmup(n)` pre-dials connections; `c.PoolStats()` reports live counts; `c.Shutdown(timeout)` drains gracefully.

## Streaming

`DoStream` returns as soon as the response HEADERS arrive. The caller pumps `Recv` for DATA, trailers, and reset events, and must call `Close`.

```go
var sr client.StreamResponse
if err := c.DoStream(ctx, client.GET("/events"), &sr); err != nil {
	log.Fatal(err)
}
defer func() { _ = sr.Close() }() // mandatory: releases the stream slot

for {
	ev, err := sr.Recv(ctx)
	if errors.Is(err, client.ErrStreamEnded) {
		break
	}
	if err != nil {
		log.Fatal(err)
	}
	if ev.Type == client.EventData {
		// ev.Data aliases a pooled buffer recycled on the next Recv;
		// use ev.DataCopy() to retain the bytes.
		fmt.Printf("chunk: %d bytes\n", len(ev.Data))
	}
}
```

Related forms:

- `c.Stream(ctx, req, fn)` — callback form that always closes the stream for you.
- `req.BodyMode = client.BodyStream` with plain `Do` — the response body arrives lazily on `resp.BodyReader` (an `io.ReadCloser`; close it).
- `req.BodyReader` — stream a request body from an `io.Reader`, chunked into DATA frames under flow control.
- `sr.WaitTrailers(ctx)` — drain the body and return response trailers (e.g. a gRPC `grpc-status`).

## Service discovery

`client.NewManagedClient` takes a `Resolver` (which backends exist) and a `Selector` (which one gets the next request). One sub-pool per resolved address.

```go
resolver := client.StaticResolver(
	client.Address{Host: "10.0.0.1", Port: 443},
	client.Address{Host: "10.0.0.2", Port: 443},
)
c, err := client.NewManagedClient(resolver, dialer,
	client.WithSelector(client.RoundRobin()))
```

`client.DNSResolver(host, port, client.DNSOptions{TTL: 30 * time.Second})` re-resolves A/AAAA records on a TTL; the pool dials new backends and drains removed ones. Selectors: `RoundRobin()`, `Random(rng)`, `Hash(keyFn)` for session affinity. `Resolver` is an interface — implement `Resolve`/`Watch` to plug in your own discovery.

## Resilience

Retries are opt-in via `Retryer`. It re-issues idempotent requests on transient failures (REFUSED_STREAM, GOAWAY, dial errors) with exponential backoff and jitter, bounded by `MaxAttempts`.

```go
r := c.Retryer(client.RetryOptions{
	MaxAttempts: 5,
	IsRetryable: func(err error, resp *client.Response) bool {
		return err == nil && resp != nil && resp.Status == 503
	},
})
var resp client.Response
err = r.Do(ctx, client.GET("/v1/health"), &resp)
```

Non-idempotent methods are not retried unless you set `req.Idempotency = client.ForceIdempotent` (pair it with an idempotency key). Token-bucket rate limiting is a constructor option — `client.WithRateLimit(100, 20)` caps at 100 requests/s with bursts up to 20. Per-request deadlines: set `req.Timeout`; on expiry `Do` fails with `context.DeadlineExceeded` and the stream is reset with `RST_STREAM(CANCEL)`.

## Observability

`WithHooks` installs lifecycle callbacks; `MetricsSnapshot` returns a frozen view of counters and latency histograms.

```go
hooks := &client.Hooks{
	OnRequestComplete: func(ev client.RequestCompleteEvent) {
		log.Printf("%s %s -> %d in %s (%d B sent, %d B recv)",
			ev.Method, ev.Path, ev.Status, ev.Latency,
			ev.BytesSent, ev.BytesRecv)
	},
}
c, err := client.NewSingleConnClient(addr, dialer, client.WithHooks(hooks))
// ...
snap := c.MetricsSnapshot()
fmt.Println(snap.Counters.Responses2xx, snap.Latency.Request.Quantile(0.99))
```

Other hooks: `OnRequestStart`, `OnRetry`, `OnDial`, `OnConnClose`, `OnResolverUpdate`. Hooks must not block. `c.PoolStats()` exposes live pool state for a `/debug` endpoint.

## Advanced protocol features

**Server push (RFC 7540 §8.2).** Register a handler with `WithPushHandler`; this enables push in SETTINGS. The client drains each pushed stream into a `Response` and hands it to the callback. Without a handler, PUSH_PROMISE is a protocol error.

```go
push := func(ctx context.Context, promised []conn.HeaderField, resp *client.Response, err error) {
	if err != nil {
		log.Printf("push failed: %v", err)
		return
	}
	log.Printf("pushed -> %d (%d bytes)", resp.Status, len(resp.Body))
}
c, err := client.NewSingleConnClient(addr, dialer, client.WithPushHandler(push))
```

**Extended CONNECT (RFC 8441).** Tunnel a WebSocket (or any protocol) over a single HTTP/2 stream. The server must advertise `SETTINGS_ENABLE_CONNECT_PROTOCOL=1`.

```go
req := &client.Request{
	Method:   "CONNECT",
	Protocol: "websocket",
	Path:     "/chat",
	BodyMode: client.BodyStream,
}
var sr client.StreamResponse
err = c.DoStream(ctx, req, &sr)
```

**H2C (prior knowledge).** Cleartext HTTP/2 without TLS or ALPN: use a `PlaintextDialer` and set the default scheme to `http`.

```go
c, err := client.NewSingleConnClient("localhost:8080",
	&conn.PlaintextDialer{},
	client.WithDefaultScheme("http"))
```

Also supported: request priority (`req.Priority = &frame.Priority{...}`, RFC 7540 §5.3), request trailers (`req.TrailerFunc`), and HTTP CONNECT proxy dialers (`conn.ProxyTLSDialer`).

## Load generator example

`examples/loadgen` is a complete HTTP/2 load generator in one file: a pooled client, N worker goroutines, an optional global QPS cap, and a summary derived from `MetricsSnapshot`.

```bash
go run ./examples/loadgen -url https://localhost:8443/ \
    -conns 4 -workers 64 -duration 30s -rps 5000
```
