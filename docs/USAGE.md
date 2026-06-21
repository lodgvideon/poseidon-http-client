# poseidon-http-client — Usage Guide

Low-level HTTP/2 (RFC 7540) + HTTP/1.1 client in pure Go.
No `net/http`, no `golang.org/x/net/http2`. Full flow-control,
HPACK, zero-alloc codec on the H2 fast path, and a keep-alive
HTTP/1.1 implementation with writev scatter-gather writes.

---

## Table of Contents

1. [Quick start — HTTP/2 single connection](#1-quick-start--http2-single-connection)
2. [HTTP/1.1 explicit](#2-http11-explicit)
3. [ALPN auto-detection (H2 → H1.1 fallback)](#3-alpn-auto-detection)
4. [Connection pool](#4-connection-pool)
5. [Managed transport + service discovery](#5-managed-transport--service-discovery)
6. [Request body — fixed bytes](#6-request-body--fixed-bytes)
7. [Request body — streaming reader](#7-request-body--streaming-reader)
8. [Response body streaming (DoStream)](#8-response-body-streaming-dostream)
9. [Request trailers](#9-request-trailers)
10. [Server push](#10-server-push)
11. [H2C (plaintext HTTP/2)](#11-h2c-plaintext-http2)
12. [Warmup](#12-warmup)
13. [Graceful shutdown](#13-graceful-shutdown)
14. [Hooks (lifecycle callbacks)](#14-hooks-lifecycle-callbacks)
15. [Metrics](#15-metrics)
16. [Rate limiting](#16-rate-limiting)
17. [Decompression](#17-decompression)
18. [Request timeout and context cancellation](#18-request-timeout-and-context-cancellation)
19. [Custom headers and authority](#19-custom-headers-and-authority)
20. [H2 Priority](#20-h2-priority)
21. [Retry policy](#21-retry-policy)

---

## 1. Quick start — HTTP/2 single connection

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/lodgvideon/poseidon-http-client/client"
    "github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
    c, err := client.NewClient(client.ClientOptions{
        Addr:     "example.com:443",
        ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{}},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    var resp client.Response
    resp.Reset()
    if err := c.Do(context.Background(), &client.Request{
        Method:   "GET",
        Path:     "/",
        WantBody: true,
    }, &resp); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("status=%d body=%s\n", resp.Status, resp.Body)
}
```

**Key points:**
- `conn.TLSDialer{}` dials TCP + TLS and asserts ALPN `h2`. Returns
  `ErrALPNFailed` if the server does not advertise HTTP/2.
- Allocate one `client.Response` per goroutine and call `resp.Reset()`
  before each `Do` call to recycle backing arrays without allocation.
- `WantBody: true` accumulates the response body in `resp.Body`.
  Omit it when you only need the status code and headers (e.g., HEAD
  checks, fire-and-forget POSTs).

---

## 2. HTTP/1.1 explicit

Use `TransportH1SingleConn` with `conn.PlaintextDialer` for plain HTTP/1.1,
or with a custom TLS dialer that advertises only `http/1.1` in ALPN.

```go
c, err := client.NewClient(client.ClientOptions{
    Transport: client.TransportH1SingleConn,
    Addr:      "example.com:80",
    ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
})
```

For TLS-protected HTTP/1.1 (e.g. legacy servers that don't support H2):

```go
c, err := client.NewClient(client.ClientOptions{
    Transport: client.TransportH1SingleConn,
    Addr:      "legacy.example.com:443",
    ConnOpts: conn.ConnOptions{
        Dialer: &conn.TLSDialer{
            Config: &tls.Config{
                NextProtos: []string{"http/1.1"},
                MinVersion: tls.VersionTLS12,
            },
        },
    },
})
```

**Key points:**
- H1.1 transport serializes requests (no pipelining). Only one request
  is in-flight at a time per `Client`; concurrent `Do` callers block
  until the previous exchange completes.
- `DoStream` and `StreamBody` are not supported on H1.1 — they return
  `ErrNotSupported`-style errors. Use the regular `Do` path instead.
- Connection keep-alive is automatic; the connection is recycled unless
  the server sends `Connection: close`.

---

## 3. ALPN auto-detection

`TransportALPN` + `conn.FlexDialer` dials once, reads the ALPN result,
and permanently routes to H2 or H1.1 for all subsequent requests:

```go
c, err := client.NewClient(client.ClientOptions{
    Transport: client.TransportALPN,
    Addr:      "mixed.example.com:443",
    ConnOpts: conn.ConnOptions{
        Dialer: &conn.FlexDialer{
            Config: &tls.Config{MinVersion: tls.VersionTLS12},
        },
    },
})
```

`conn.FlexDialer` offers `["h2", "http/1.1"]` in ALPN. The server
chooses. After the first successful connection the `Client` locks in the
negotiated protocol and never re-checks — use a separate `Client` per
service if you need to handle servers that change protocols.

---

## 4. Connection pool

`TransportPool` multiplexes requests across up to `MaxConnsPerHost`
persistent connections. Each connection can carry up to
`MaxStreamsPerConn` simultaneous HTTP/2 streams.

```go
c, err := client.NewClient(client.ClientOptions{
    Transport: client.TransportPool,
    Addr:      "api.example.com:443",
    ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
    Pool: &client.PoolOptions{
        MaxConnsPerHost:   4,
        MaxStreamsPerConn: 100,
        DialBackoff:       500 * time.Millisecond,
    },
})
```

Get a live snapshot of pool state:

```go
stats := c.PoolStats()
fmt.Printf("active=%d idle=%d inflight=%d\n",
    stats.ActiveConns, stats.IdleConns, stats.InflightRequests)
```

---

## 5. Managed transport + service discovery

`TransportManaged` fans requests across a dynamic set of backend addresses
resolved by a `Resolver`. Built-in resolvers:

```go
// Static list of addresses:
resolver := client.StaticResolver(
    client.Address{Host: "10.0.0.1", Port: 443},
    client.Address{Host: "10.0.0.2", Port: 443},
)

// Round-robin selector (default when Selector is nil):
c, err := client.NewClient(client.ClientOptions{
    Transport: client.TransportManaged,
    ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
    Resolver:  resolver,
    Pool: &client.PoolOptions{
        MaxConnsPerHost:   2,
        MaxStreamsPerConn: 50,
    },
})
```

For load-balanced load generators, implement `client.Resolver` and return
a fresh `[]Address` slice each time `Resolve(ctx)` is called. Use
`DrainGraceful` (default) or `DrainHard` to control what happens when an
address is removed:

```go
c, err := client.NewClient(client.ClientOptions{
    Transport: client.TransportManaged,
    DrainMode: client.DrainGraceful, // in-flight requests complete
    Resolver:  myDNSResolver,
    Selector:  myHashSelector,        // or nil for RoundRobin
    ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
})
```

---

## 6. Request body — fixed bytes

```go
err := c.Do(ctx, &client.Request{
    Method: "POST",
    Path:   "/api/data",
    Headers: []conn.HeaderField{
        {Name: []byte("content-type"), Value: []byte("application/json")},
    },
    Body:     []byte(`{"key":"value"}`),
    WantBody: true,
}, &resp)
```

For H2 the body is sent in one DATA frame. For H1.1, it is sent with
`Transfer-Encoding: chunked` (no `Content-Length` is set in the
pseudo-header block by default; the H1.1 layer chunks it automatically).

---

## 7. Request body — streaming reader

When body size is known upfront, set `ContentLength` so the H1.1 layer
sends `Content-Length` instead of chunked encoding:

```go
f, _ := os.Open("upload.bin")
defer f.Close()
info, _ := f.Stat()

err := c.Do(ctx, &client.Request{
    Method:        "PUT",
    Path:          "/upload",
    BodyReader:    f,
    ContentLength: info.Size(),
}, &resp)
```

Without `ContentLength` (unknown size):

```go
err := c.Do(ctx, &client.Request{
    Method:     "POST",
    Path:       "/stream",
    BodyReader: myReader, // ContentLength is 0 → chunked on H1.1
}, &resp)
```

---

## 8. Response body streaming (DoStream)

`DoStream` returns after the initial HEADERS frame. The caller pumps
`StreamResponse.Recv` for DATA / trailer events:

```go
var sr client.StreamResponse
if err := c.DoStream(ctx, &client.Request{
    Method: "GET",
    Path:   "/sse",
}, &sr); err != nil {
    log.Fatal(err)
}
defer sr.Close()

fmt.Printf("status=%d\n", sr.Status)

for {
    ev, err := sr.Recv(ctx)
    if err != nil {
        break
    }
    switch ev.Type {
    case conn.EventData:
        process(ev.Data)
        if ev.EndStream {
            return
        }
    case conn.EventTrailers:
        // ev.Headers contains trailer fields
    case conn.EventReset:
        log.Printf("stream reset: %v", ev.RSTCode)
        return
    }
}
```

`DoStream` is only available on HTTP/2 connections (`TransportSingleConn`,
`TransportPool`, `TransportManaged`, `TransportALPN` after H2 is detected).

---

## 9. Request trailers

Attach trailers via `Trailers` (static) or `TrailerFunc` (computed after
body is written):

```go
// Static trailers:
err := c.Do(ctx, &client.Request{
    Method: "POST",
    Path:   "/upload",
    Body:   data,
    Trailers: []conn.HeaderField{
        {Name: []byte("x-checksum"), Value: []byte("abc123")},
    },
}, &resp)

// Dynamic trailers computed after body:
err := c.Do(ctx, &client.Request{
    Method:     "POST",
    Path:       "/upload",
    BodyReader: r,
    TrailerFunc: func() []conn.HeaderField {
        return []conn.HeaderField{
            {Name: []byte("x-checksum"), Value: computedChecksum()},
        }
    },
}, &resp)
```

Read response trailers:

```go
err := c.Do(ctx, &client.Request{
    Method:       "GET",
    Path:         "/data",
    WantTrailers: true,
}, &resp)
// resp.Trailers is populated after Do returns.
```

---

## 10. Server push

```go
pushHandler := func(ctx context.Context, promised []conn.HeaderField, pushed *client.Response, err error) {
    if err != nil {
        log.Printf("push error: %v", err)
        return
    }
    // promised contains the :method, :path etc. the server promised.
    // pushed.Body is always populated (WantBody=true is implicit for push).
    log.Printf("pushed %s → status=%d len=%d",
        pushed.Headers, pushed.Status, len(pushed.Body))
}

c, err := client.NewClient(client.ClientOptions{
    Addr:        "push.example.com:443",
    ConnOpts:    conn.ConnOptions{Dialer: &conn.TLSDialer{}},
    PushHandler: pushHandler,
    // EnablePush is set automatically when PushHandler != nil.
})
```

---

## 11. H2C (plaintext HTTP/2)

RFC 7540 §3.4 prior-knowledge upgrade — TLS not required:

```go
c, err := client.NewClient(client.ClientOptions{
    Addr: "h2c.internal:80",
    ConnOpts: conn.ConnOptions{
        Dialer: &conn.PlaintextDialer{},
    },
    DefaultScheme: "http",
})
```

---

## 12. Warmup

Pre-dial connections before the first request to avoid paying TLS
handshake latency on the critical path:

```go
c, _ := client.NewClient(opts)
c.Warmup(4) // dial up to 4 connections in background
// ... start sending traffic
```

`Warmup` is idempotent and returns immediately; dial errors surface via
the `OnDial` hook.

---

## 13. Graceful shutdown

```go
// Stop accepting new requests and wait up to 5s for in-flight ones.
if err := c.Shutdown(5 * time.Second); err != nil {
    log.Printf("shutdown: %v", err)
}
```

For the single-conn transport, `Shutdown` sends a GOAWAY frame and waits
for the in-flight stream count to reach zero within the timeout. For pool
transports, all connections are force-closed after the timeout (no per-conn
GOAWAY draining at the pool level).

---

## 14. Hooks (lifecycle callbacks)

```go
hooks := &client.Hooks{
    OnDial: func(e client.DialEvent) {
        if e.Err != nil {
            log.Printf("dial %s failed (%v): %v", e.Addr, e.Duration, e.Err)
        }
    },
    OnRequestStart: func(e client.RequestStartEvent) {
        log.Printf("→ %s %s%s", e.Method, e.Authority, e.Path)
    },
    OnRequestComplete: func(e client.RequestCompleteEvent) {
        log.Printf("← %d %s in %v (sent=%d recv=%d err=%v)",
            e.Status, e.Path, e.Latency, e.BytesSent, e.BytesRecv, e.Err)
    },
}

c, _ := client.NewClient(client.ClientOptions{
    // ...
    Hooks: hooks,
})

// Replace hooks at runtime (atomic, safe for concurrent use):
c.SetHooks(&client.Hooks{OnRequestComplete: myNewHook})
```

---

## 15. Metrics

All counters and latency histograms are thread-safe `sync/atomic`-backed
accumulators.

```go
// Live pointer — safe to read from multiple goroutines.
m := c.Metrics()
fmt.Println(m.Counters.RequestsSucceeded.Load())
fmt.Println(m.Counters.RequestsErrored.Load())

// Value-copy snapshot — safe to store and compare.
snap := c.MetricsSnapshot()
fmt.Printf("p99 request latency: %v\n", snap.Latency.Request.P99())
fmt.Printf("dials attempted: %d\n", snap.Counters.DialsAttempted)
```

---

## 16. Rate limiting

Token-bucket rate limiter built in. `Do` blocks until a token is
available or the context is cancelled:

```go
c, err := client.NewClient(client.ClientOptions{
    // ...
    RateLimitPerSecond: 1000,  // QPS cap
    RateLimitBurst:     200,   // burst allowance above steady-state
})
```

---

## 17. Decompression

Gzip and zlib response decompression are automatic. The client sends
`Accept-Encoding: gzip` by default and decompresses transparently.

```go
// Disable if you need the raw compressed bytes:
err := c.Do(ctx, &client.Request{
    Method:              "GET",
    Path:                "/compressed",
    WantBody:            true,
    DisableDecompression: true,
}, &resp)
```

Bomb protection is applied before decompression:

```go
c, _ := client.NewClient(client.ClientOptions{
    // ...
    MaxDecompressedSize: 50 << 20, // 50 MiB cap (default 10 MiB)
    MaxResponseBodySize: 100 << 20, // raw body cap (default 32 MiB)
})
```

---

## 18. Request timeout and context cancellation

Per-request timeout via `Request.Timeout`:

```go
err := c.Do(ctx, &client.Request{
    Method:  "GET",
    Path:    "/slow",
    Timeout: 2 * time.Second,
}, &resp)
```

Or pass a deadline context:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
err := c.Do(ctx, req, &resp)
```

Both work. `Request.Timeout` is layered on top of the caller's context,
so the tighter of the two wins.

---

## 19. Custom headers and authority

```go
err := c.Do(ctx, &client.Request{
    Method:    "GET",
    Path:      "/api/v1/resource",
    Authority: "service.internal:443", // overrides :authority
    Scheme:    "https",                // overrides :scheme
    Headers: []conn.HeaderField{
        {Name: []byte("authorization"), Value: []byte("Bearer " + token)},
        {Name: []byte("x-request-id"), Value: []byte(reqID)},
        {Name: []byte("accept"),        Value: []byte("application/json")},
    },
    WantBody: true,
}, &resp)
```

Header names must be lowercase (HTTP/2 requirement; enforced by the
linter). Pseudo-headers (`:method`, `:path`, etc.) are managed by the
client and must not appear in `Headers`.

---

## 20. H2 Priority

RFC 7540 §5.3 stream priorities for quality-of-service:

```go
import "github.com/lodgvideon/poseidon-http-client/frame"

err := c.Do(ctx, &client.Request{
    Method: "GET",
    Path:   "/critical",
    Priority: &frame.Priority{
        StreamDep: 0,      // parent stream (0 = root)
        Weight:    255,    // 0-255; higher = more weight
        Exclusive: false,
    },
}, &resp)
```

Priorities are advisory — servers may ignore them.

---

## 21. Retry policy

Built-in retry on `503 Service Unavailable`, `RST_STREAM(REFUSED_STREAM)`,
and connection-level errors for idempotent methods:

```go
c, err := client.NewClient(client.ClientOptions{
    // ...
    // Default retry: 2 retries on retryable errors for GET/HEAD/OPTIONS.
    // Override by setting ConnOpts.RetryPolicy (if exposed) or implementing
    // a retry loop at the call site for full control.
})
```

For explicit control, implement a retry loop at the call site:

```go
var resp client.Response
var err error
for attempt := 0; attempt < 3; attempt++ {
    resp.Reset()
    err = c.Do(ctx, req, &resp)
    if err == nil || !isRetryable(err) {
        break
    }
    time.Sleep(backoff(attempt))
}
```

---

## Error types

| Error | Meaning |
|---|---|
| `client.ErrClosed` | Client was closed |
| `client.ErrConnDraining` | Shutdown in progress |
| `*client.DialError` | TCP/TLS dial failure; `.Err` wraps the cause |
| `client.ErrRedialBackoff` | Dial suppressed (DialBackoff window) |
| `client.ErrNoAddresses` | Managed pool: resolver returned no addresses |
| `*client.StreamResetError` | Peer sent RST_STREAM; `.Code` is the H2 error code |
| `conn.ErrALPNFailed` | TLS peer did not negotiate h2 |
| `conn.ErrGoAway` | Peer sent GOAWAY; create a new Client |
| `conn.ErrTooManyStreams` | Pool stream limit reached |
| `client.ErrBodyTooLarge` | Response body exceeded MaxResponseBodySize |

---

## Connection options reference

```go
conn.ConnOptions{
    Dialer: &conn.TLSDialer{Config: myTLSConfig},

    Settings: conn.AdvertisedSettings{
        MaxConcurrentStreams: 100,    // default
        InitialWindowSize:   65535,  // default (RFC minimum)
        MaxFrameSize:        16384,  // default
        HeaderTableSize:     4096,   // default
    },

    StreamEventBuffer:  8,               // per-stream event channel depth
    KeepaliveInterval:  30 * time.Second, // 0 = disabled
    KeepaliveTimeout:   5 * time.Second,  // 0 = 5x interval
    EnablePush:         false,            // set true via PushHandler
}
```
