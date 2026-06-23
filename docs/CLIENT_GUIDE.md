# poseidon-http-client — Client Guide

`poseidon-http-client` is a low-level, zero-allocation HTTP/2 client built for
load generators that need a reusable request/response codec and fine-grained
control over connections, streams, flow control, and pooling. The high-level
API lives in package
`github.com/lodgvideon/poseidon-http-client/client`; HTTP/2 wire types
(`HeaderField`, `ErrCode`, `Priority`) come from `.../conn` and `.../frame`.
This guide walks the public surface — construction, unary and streaming
requests, resilience, pooling/service discovery, and observability — with
verified, copy-pasteable examples.

## Quick start

The canonical import block used throughout this guide:

```go
import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)
```

A minimal single-connection TLS (`h2`) GET, plus the canonical
`Response`-reuse loop. Allocate one `Response`, call `Reset()` before every
`Do`, and treat the result's backing bytes as valid only until the next
`Reset()`:

```go
func main() {
	c, err := client.NewClient(client.ClientOptions{
		Addr: "example.com:443",
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}},
		},
		// Transport omitted → TransportSingleConn (the default).
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close() // REQUIRED — leaks the conn + reader goroutine otherwise.

	req := &client.Request{Method: "GET", Path: "/metrics", WantBody: true}

	var resp client.Response // one per goroutine; reuse across calls.
	for {
		resp.Reset() // MUST reset before every (re)use, including after an error.
		if err := c.Do(context.Background(), req, &resp); err != nil {
			log.Fatal(err)
		}
		_ = resp.Status        // e.g. 200
		_ = resp.Headers       // []conn.HeaderField, valid until next Reset()
		_ = resp.Body          // body bytes,        valid until next Reset()
		_ = resp.BytesReceived // raw wire DATA bytes

		time.Sleep(time.Second)
	}
}
```

## Table of contents

- [Client construction & transports](#client-construction--transports)
- [Unary requests (`Do`)](#unary-requests-do)
- [Streaming responses & body upload](#streaming-responses--body-upload)
- [Retry, idempotency, rate limiting, and timeouts](#retry-idempotency-rate-limiting-and-timeouts)
- [Connection pooling & service discovery](#connection-pooling--service-discovery)
- [Observability & advanced protocol](#observability--advanced-protocol)
- [Required-call contracts](#required-call-contracts)
- [Errors](#errors)
- [Convenience helpers](#convenience-helpers)

## Client Construction & Transports

Every Poseidon client starts with one call to `client.NewClient`. `NewClient` validates the options, picks a *transport strategy*, and returns a `*client.Client` — but it does **not** dial. The first `Do` / `DoStream` triggers a lazy connection establish (except for the managed transport, which resolves addresses eagerly at construction).

```go
import (
    "github.com/lodgvideon/poseidon-http-client/client"
    "github.com/lodgvideon/poseidon-http-client/conn"
)

func client.NewClient(opts client.ClientOptions) (*client.Client, error)
```

A `Client` is safe for concurrent use by multiple goroutines. Whatever transport you pick, the same three lifecycle methods apply:

| Method | Signature | Effect |
|---|---|---|
| `Close` | `func (c *Client) Close() error` | Releases the underlying transport. Subsequent `Do`/`DoStream` return `ErrClosed`. Idempotent. |
| `Shutdown` | `func (c *Client) Shutdown(gracefulTimeout time.Duration) error` | Graceful close. For single-conn it sends GOAWAY and waits for in-flight streams up to `gracefulTimeout`, then force-closes. **Pool / managed / H1 transports ignore `gracefulTimeout` and behave exactly like `Close`** (they have no per-conn drain). Idempotent. |
| `Warmup` | `func (c *Client) Warmup(n int)` | Pre-dials up to `n` connections in the background and returns immediately. `n` is capped at the transport's `MaxConnsPerHost` (always 1 for single-conn). Dial errors surface via the `OnDial` hook. Idempotent. |

> **Required-call contract:** you MUST call `Close` (or `Shutdown`) on every `Client` you construct, or you leak the underlying connection(s), reader goroutines, and the pool actor goroutine.

### Focused constructors (the easy path)

`ClientOptions` is one flat struct with a cross-field validity matrix (`Addr` is required for single-conn/pool but must be empty for managed; `Pool` is required iff `TransportPool`; `Resolver` is required iff managed). For the common setups, prefer the focused constructors — each encodes a valid transport + required-field combination in its signature, so invalid combinations are unrepresentable:

```go
// Single connection (the default transport), with auto-redial.
c, err := client.NewSingleConnClient("api.example.com:443", dialer)

// Pool of up to N connections to one backend.
c, err := client.NewPoolClient("api.example.com:443", dialer,
    client.PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 100})

// Managed multi-backend: a Resolver discovers addresses, a Selector picks one.
c, err := client.NewManagedClient(resolver, dialer,
    client.WithSelector(client.RoundRobin()))
```

`dialer` is any `conn.Dialer` (e.g. `&conn.TLSDialer{Config: tlsCfg}`). Tune everything else with functional `Option`s, applied in order after the base fields:

```go
c, err := client.NewSingleConnClient(addr, dialer,
    client.WithDefaultScheme("http"),           // H2C
    client.WithRateLimit(1000, 1000),           // 1000 QPS, burst 1000
    client.WithHooks(hooks),
    client.WithMaxResponseBodySize(64<<20),
    client.WithConnOptions(func(co *conn.ConnOptions) {
        co.KeepaliveInterval = 30 * time.Second
    }),
)
```

Available options: `WithHooks`, `WithPushHandler`, `WithDefaultScheme`, `WithRateLimit`, `WithMaxResponseBodySize`, `WithMaxDecompressedSize`, `WithDialBackoff`, `WithSelector` (managed), `WithDrainMode` (managed), and `WithConnOptions` (escape hatch to mutate the underlying `conn.ConnOptions`). Drop to `NewClient(ClientOptions{...})` when you need full control — the sections below document every field it accepts.

### `ClientOptions` — every field

```go
type ClientOptions struct {
    Addr                 string             // "host:port"; required except for TransportManaged
    ConnOpts             conn.ConnOptions   // forwarded verbatim to conn.Dial; ConnOpts.Dialer REQUIRED
    DialBackoff          time.Duration      // single-conn: suppress redial within this window after a failed dial; 0 = immediate retry
    Transport            client.TransportKind // strategy; zero value = TransportSingleConn
    Pool                 *client.PoolOptions  // required iff Transport==TransportPool; MUST be nil otherwise
    Resolver             client.Resolver      // required iff Transport==TransportManaged
    Selector             client.Selector      // managed only; nil → RoundRobin()
    DrainMode            client.DrainMode     // managed only; zero value = DrainGraceful
    Hooks                *client.Hooks        // optional lifecycle callbacks; nil = no hooks; replaceable via Client.SetHooks
    PushHandler          client.PushHandler   // server-push callback; non-nil auto-sets ConnOpts.EnablePush=true
    DefaultScheme        string             // :scheme when Request.Scheme empty; "" → "https"; set "http" for H2C
    RateLimitPerSecond   float64            // token-bucket QPS cap; 0 = disabled
    RateLimitBurst       float64            // burst capacity; 0 → equals RateLimitPerSecond; only meaningful when RateLimitPerSecond>0
    MaxDecompressedSize  int64              // gzip-bomb guard; 0 → DefaultMaxDecompressedSize (10 MiB)
    MaxResponseBodySize  int64              // raw body cap; 0 → DefaultMaxResponseBodySize (32 MiB)
}
```

Validation enforced by `NewClient` (returns a wrapped sentinel error):

- `ConnOpts.Dialer == nil` → `"client: ClientOptions.ConnOpts.Dialer is required"`.
- For `TransportSingleConn` / `TransportPool` / `TransportH1SingleConn` / `TransportALPN`: `Addr` must be a non-empty `host:port` with no whitespace, else error.
- `Pool != nil` with any non-`TransportPool` kind → `ErrInvalidPoolOptions`. `Pool == nil` with `TransportPool` → `ErrInvalidPoolOptions`.
- `TransportManaged`: `Resolver` required (`ErrInvalidOptions` if nil) and `Addr` **must be empty** (`ErrInvalidOptions` otherwise — the Resolver owns addressing).
- Unknown `Transport` value → `ErrInvalidTransportKind`.

The `:authority` for requests defaults to a port-stripped form of `Addr` (`deriveAuthority` removes `:80`/`:443` and re-brackets IPv6 literals); per-request `Request.Authority` overrides it.

### Dialers (`ConnOpts.Dialer`)

`ConnOpts.Dialer` is always required. The `conn` package ships four:

- `&conn.TLSDialer{Config: *tls.Config}` — TCP + TLS, asserts ALPN `h2` (default; `Config` nil → TLS 1.2+ with `NextProtos=["h2"]`). Returns `conn.ErrALPNFailed` if the peer does not select `h2`.
- `&conn.PlaintextDialer{}` — raw TCP for H2C prior-knowledge (no TLS/ALPN). Pair with `DefaultScheme: "http"`.
- `&conn.FlexDialer{Config: *tls.Config}` — TCP + TLS offering both `h2` and `http/1.1`; use with `TransportALPN`.
- A plain-TCP or `http/1.1`-only TLS dialer for `TransportH1SingleConn`.

### The five transport kinds

```go
const (
    client.TransportSingleConn   client.TransportKind = iota // default (zero value)
    client.TransportPool
    client.TransportManaged
    client.TransportH1SingleConn
    client.TransportALPN
)
```

**`TransportSingleConn` (default).** At most one `*conn.Conn`. Lazy dial on the first request; if the cached conn dies (`IsAlive()==false`) the next request auto-redials. Concurrent dials are de-duplicated (only one goroutine dials; others wait). `DialBackoff` suppresses retries within the window after a failed dial — a request during the window fails fast with a `*DialError` wrapping `ErrRedialBackoff`. `Warmup(n)` ignores `n>1`. Use for: a single backend where one HTTP/2 connection's stream multiplexing is enough throughput.

**`TransportPool`.** Routes through a `*client.Pool` (actor-goroutine model) of up to `PoolOptions.MaxConnsPerHost` conns to one `Addr`, each carrying up to `min(MaxStreamsPerConn, peer SETTINGS_MAX_CONCURRENT_STREAMS)` concurrent streams. Adds idle eviction, health-check sweeps, dial backoff, acquire timeout, and dial timeout. `Warmup(n)` pre-dials up to `MaxConnsPerHost`. Use for: a single backend where one conn's stream cap throttles you, or where you want idle-eviction / dead-conn reaping. **Note:** `Shutdown(timeout)` on a pool transport is just `Close()` — `timeout` is ignored; closes all conns in parallel.

**`TransportManaged`.** Multi-address fan-out: a `Resolver` discovers backends, a `Selector` picks one per acquire, and the managed pool keeps a per-address sub-`Pool` (each configured by the optional shared `PoolOptions`). `Addr` MUST be empty. On a *dial-only* failure (`*DialError`, `ErrDialBackoff`, `ErrPoolClosed`) it fails over to the next address; non-dial errors propagate. When the Resolver drops an address, `DrainMode` governs the sub-pool's teardown. `NewClient` does an eager initial `Resolve` (surfaces hard errors immediately). Use for: load generators hitting a service with several backends / DNS-discovered endpoints.

**`TransportH1SingleConn`.** HTTP/1.1 analogue of single-conn: at most one `*http1.Conn`, requests **serialized** (one in-flight exchange at a time, no pipelining). `ConnOpts.Dialer` must NOT assert ALPN `h2` — use `PlaintextDialer` or a TLS dialer whose `NextProtos` is `http/1.1`-only. `DoStream`/`StreamBody` and request trailers are **not** supported (the latter returns `ErrTrailersUnsupportedH1`). Use for: an HTTP/1.1-only origin you must talk to with the same `Request`/`Response` API.

**`TransportALPN`.** Dials once with a `FlexDialer` (offers `h2` + `http/1.1`), detects the negotiated protocol, and permanently delegates to a single-conn (H2) or H1 single-conn (H1.1). Identical to `TransportSingleConn` against H2 servers; falls back to HTTP/1.1 automatically. `ConnOpts.Dialer` should be `*conn.FlexDialer`. Use for: a target whose protocol you don't know in advance.

### `PoolOptions` — every field (zero values defaulted at `newPool`)

```go
type PoolOptions struct {
    MaxConnsPerHost   int           // live conns cap; 0 → 1
    MaxStreamsPerConn int           // soft per-conn stream cap; effective = min(this, peer MAX_CONCURRENT_STREAMS); 0 → peer value (or 100 if peer unbounded)
    IdleTimeout       time.Duration // close conn idle (active==0) longer than this; 0 → never
    HealthCheckPeriod time.Duration // actor tick for idle/dead sweeps; 0 → 30s
    DialBackoff       time.Duration // refuse new dials within window after a dial failure; 0 → 1s
    AcquireTimeout    time.Duration // bound on Acquire waiting for capacity; 0 → ctx only
    DialTimeout       time.Duration // bound on a single conn.Dial; 0 → 30s
}
```

`Client.PoolStats()` returns a `client.Stats` snapshot for pool and managed transports (zero `Stats` for single-conn / H1 / closed pools):

```go
type Stats struct {
    ActiveConns      int
    InFlightStreams  int
    Waiters          int
    InFlightDials    int
    Addresses        int // managed only
    DrainingSubpools int // managed only
}
```

### `DrainMode` (managed transport only)

```go
const (
    client.DrainGraceful client.DrainMode = iota // default: refuse new acquires; in-flight completes; sub-pool closes when InFlightStreams==0
    client.DrainHard                              // close every conn immediately; in-flight streams reset RST_STREAM(CANCEL)
    client.DrainLazy                              // refuse new acquires; idle eviction eventually closes conns
)
```

### H2C (plaintext, prior-knowledge)

```go
c, err := client.NewClient(client.ClientOptions{
    Addr:          "127.0.0.1:8080",
    DefaultScheme: "http", // H2C targets
    ConnOpts: conn.ConnOptions{
        Dialer: &conn.PlaintextDialer{},
    },
})
```

### HTTP/1.1 origin (`TransportH1SingleConn`)

```go
c, err := client.NewClient(client.ClientOptions{
    Addr:      "legacy.internal:80",
    Transport: client.TransportH1SingleConn,
    ConnOpts: conn.ConnOptions{
        Dialer: &conn.PlaintextDialer{}, // NOT a TLSDialer (which asserts ALPN h2)
    },
})
// Requests serialized; DoStream / Request.StreamBody / trailers unsupported.
```

### Auto-negotiating origin (`TransportALPN`)

```go
c, err := client.NewClient(client.ClientOptions{
    Addr:      "maybe-h2.example.com:443",
    Transport: client.TransportALPN,
    ConnOpts: conn.ConnOptions{
        Dialer: &conn.FlexDialer{}, // offers h2 + http/1.1, server chooses
    },
})
```

### Pooled production setup (single backend, multiple conns)

```go
package main

import (
    "context"
    "crypto/tls"
    "log"
    "time"

    "github.com/lodgvideon/poseidon-http-client/client"
    "github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
    c, err := client.NewClient(client.ClientOptions{
        Addr:      "api.example.com:443",
        Transport: client.TransportPool,
        ConnOpts: conn.ConnOptions{
            Dialer:            &conn.TLSDialer{Config: &tls.Config{ServerName: "api.example.com"}},
            KeepaliveInterval: 15 * time.Second, // detect dead conns proactively
        },
        Pool: &client.PoolOptions{
            MaxConnsPerHost:   8,
            MaxStreamsPerConn: 100,
            IdleTimeout:       2 * time.Minute,
            HealthCheckPeriod: 30 * time.Second,
            DialBackoff:       time.Second,
            AcquireTimeout:    5 * time.Second,
            DialTimeout:       10 * time.Second,
        },
        Hooks: &client.Hooks{
            OnDial: func(e client.DialEvent) {
                if e.Err != nil {
                    log.Printf("dial %s failed in %s: %v", e.Addr, e.Duration, e.Err)
                }
            },
            OnConnClose: func(e client.ConnCloseEvent) {
                log.Printf("conn %s closed: %s", e.Addr, e.Reason)
            },
        },
        RateLimitPerSecond: 5000, // token-bucket QPS budget
    })
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    c.Warmup(8) // pre-dial the whole pool before the burst; returns immediately

    req := &client.Request{Method: "GET", Path: "/health", WantBody: true}
    var resp client.Response
    for i := 0; i < 100000; i++ {
        resp.Reset()
        if err := c.Do(context.Background(), req, &resp); err != nil {
            log.Printf("request %d: %v", i, err)
            continue
        }
        _ = resp.Status
    }

    st := c.PoolStats()
    log.Printf("conns=%d inflight=%d waiters=%d", st.ActiveConns, st.InFlightStreams, st.Waiters)
}
```

### Managed pool (multi-backend, DNS-discovered)

```go
package main

import (
    "crypto/tls"
    "log"
    "time"

    "github.com/lodgvideon/poseidon-http-client/client"
    "github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
    // Static set:
    res := client.StaticResolver(
        client.Address{Host: "10.0.0.1", Port: 443},
        client.Address{Host: "10.0.0.2", Port: 443},
    )
    // ...or DNS-backed, re-resolved every TTL:
    // res := client.DNSResolver("api.example.com", 443, client.DNSOptions{TTL: 30 * time.Second, PreferIPv4: true})

    c, err := client.NewClient(client.ClientOptions{
        // Addr MUST be empty for TransportManaged.
        Transport: client.TransportManaged,
        Resolver:  res,
        Selector:  client.RoundRobin(), // nil also defaults to RoundRobin()
        DrainMode: client.DrainGraceful,
        ConnOpts: conn.ConnOptions{
            Dialer: &conn.TLSDialer{Config: &tls.Config{ServerName: "api.example.com"}},
        },
        Pool: &client.PoolOptions{ // applied to every per-address sub-pool
            MaxConnsPerHost: 4,
            IdleTimeout:     time.Minute,
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer c.Shutdown(10 * time.Second) // managed transport ignores the timeout; equivalent to Close()
    _ = c
}
```

A `Selector` is `interface{ Pick(set []Address, pc PickContext) (Address, error) }`; built-ins are `client.RoundRobin()`, `client.Random(rng *rand.Rand)` (nil rng → time-seeded), and `client.Hash(keyFn func(client.PickContext) string)` (returns `(Selector, error)`; `ErrNilKeyFn` if `keyFn` is nil). A `Resolver` is `interface{ Resolve(ctx) ([]Address, error); Watch(ctx) (<-chan []Address, error) }`; a Resolver without push support returns `ErrWatchUnsupported` from `Watch`, and the managed pool falls back to polling `Resolve` (every `DNSOptions.TTL`, else 30s).

### Construction-time sentinel errors

| Error | Cause |
|---|---|
| `ErrInvalidPoolOptions` | `Pool` set with a non-pool transport, or nil with `TransportPool` |
| `ErrInvalidTransportKind` | `Transport` not one of the five defined kinds |
| `ErrInvalidOptions` | `TransportManaged` with nil `Resolver` or non-empty `Addr` |
| (plain `fmt.Errorf`) | nil `ConnOpts.Dialer`; empty/whitespace `Addr` for a kind that requires it |

Runtime dial/lifecycle errors you'll see from `Do`/`DoStream`: `ErrClosed` (after `Close`), `*DialError` (lazy dial failed; `Unwrap`-able), `ErrRedialBackoff` (single-conn within `DialBackoff`), `ErrDialBackoff` / `ErrAcquireTimeout` / `ErrPoolClosed` (pool), `ErrNoAddresses` (managed, empty set).

## Unary requests (`Do`)

`Client.Do` is the synchronous request primitive: you hand it a context, a
`*Request`, and a caller-owned `*Response`; it sends the request, drains the
full response, and writes the result into your `Response` in place.

```go
func (c *Client) Do(ctx context.Context, req *Request, resp *Response) error
```

`Do` is safe for concurrent use across goroutines, but a single `*Response`
value is **not** — give each goroutine its own `Response` and reuse it across
that goroutine's calls.

### Building a `Request`

`Request.Method` and `Request.Path` are the only required fields; both must be
non-empty and contain no whitespace or `Do` returns an error wrapping
`client.ErrInvalidRequest`. `Scheme` and `Authority` default from the client
when left empty (`Scheme` falls back to `ClientOptions.DefaultScheme`, default
`"https"`; `Authority` falls back to the authority derived from
`ClientOptions.Addr`).

Headers are `conn.HeaderField` values whose `Name` and `Value` are `[]byte`
(`conn.HeaderField` is an alias for `hpack.HeaderField`; it also has a
`Sensitive bool` field that forces never-indexed HPACK encoding). The regular
`Headers` slice MUST NOT contain pseudo-headers (names starting with `:`) or
HTTP/2-forbidden connection headers (`connection`, `keep-alive`,
`proxy-connection`, `transfer-encoding`, `upgrade`); `te` is allowed only with
value `trailers`. Violations are rejected up front with `ErrInvalidRequest`.

```go
req := &client.Request{
	Method: "GET",
	Path:   "/api/v1/health",
	// Scheme and Authority omitted -> default from the Client.
	Headers: []conn.HeaderField{
		{Name: []byte("accept"), Value: []byte("application/json")},
		{Name: []byte("user-agent"), Value: []byte("poseidon-loadgen/1.0")},
	},
	WantBody: true, // opt the response body buffer in
}
```

Useful optional `Request` fields for unary calls:

- `Body []byte` — request body sent as DATA frames. For `[]byte` bodies the
  content-length is derived automatically and `ContentLength` is ignored.
- `BodyReader io.Reader` — streams the body in DATA frames; takes precedence
  over `Body` when non-nil. When `BodyReader` is set **and** `ContentLength > 0`
  the client emits a `content-length` header.
- `ContentLength int64` — body byte count; only emitted as a header when
  `BodyReader` is non-nil and the value is `> 0`.
- `WantBody bool` — see below.
- `WantTrailers bool` — see below.
- `DisableDecompression bool` — when false (default) the client auto-adds
  `accept-encoding: gzip` (unless you already set an `accept-encoding` header)
  and transparently decodes `content-encoding: gzip`/`deflate` responses.
- `Timeout time.Duration` — per-request deadline; when `> 0` the client derives
  a sub-context from `ctx`. On expiry the request fails (the in-flight stream is
  reset) and `Do` returns a context-deadline error.
- `Priority *frame.Priority`, `Protocol string` (extended CONNECT, RFC 8441;
  requires `Method == "CONNECT"`), `Idempotency IdempotencyMode`, `Trailers` /
  `TrailerFunc` (request trailers).

### The `Response` struct and the `Reset()` reuse contract

`Response` is **caller-provided and reused** for zero-allocation operation. You
allocate one `Response` and call `Reset()` before each reuse:

```go
type Response struct {
	Status        int                // parsed from :status
	Headers       []conn.HeaderField // regular response headers (no pseudo-headers)
	Body          []byte             // nil unless Request.WantBody == true
	Trailers      []conn.HeaderField // nil unless WantTrailers and peer sent trailers
	BytesReceived int64              // total DATA payload received, even when WantBody == false
	BodyReader    io.ReadCloser      // non-nil only for Request.StreamBody (not covered here)
	// unexported slab pointers backing Headers/Trailers bytes
}
```

Contract:

- The backing bytes of `Headers`, `Body`, and `Trailers` are valid **only until
  the next `Reset()`**. Do not retain those slices (or `Header.Name`/`.Value`
  byte slices) past a `Reset()` — `Reset()` returns the pooled slab buffers that
  back them. Copy out anything you need to keep.
- `Reset()` clears the exported fields, retains the backing arrays for reuse,
  and (on the first call against a zero-value `Response`) preallocates the
  `Headers`/slab arrays so subsequent appends do not allocate.
- On error from `Do`, the `Response` fields are undefined; call `Reset()` before
  reuse regardless.

#### `WantBody` / `WantTrailers` opt-in flags

The response body and trailers are **opt-in** to keep the zero-body path
allocation-free:

- `WantBody`: when false, response DATA frames are still consumed (so HTTP/2
  flow-control refunds run) but the payload is dropped before `Do` returns and
  `Response.Body` stays nil. When true, the body is buffered into
  `Response.Body`.
- `WantTrailers`: when false, response trailers are ignored. When true and the
  peer sends a trailers frame, they are captured into `Response.Trailers`.

`BytesReceived` reflects the raw on-the-wire DATA byte count and is populated
**regardless of `WantBody`**. When decompression is active, `BytesReceived` is
the compressed wire size while `Response.Body` holds the decompressed bytes.

### POST with a body, reading status, headers, body, and trailers

```go
func postJSON(ctx context.Context, c *client.Client, payload []byte) error {
	req := &client.Request{
		Method: "POST",
		Path:   "/api/v1/orders",
		Headers: []conn.HeaderField{
			{Name: []byte("content-type"), Value: []byte("application/json")},
		},
		Body:         payload, // content-length derived automatically
		WantBody:     true,    // buffer the response body
		WantTrailers: true,    // capture grpc-style trailers if the peer sends them
		Timeout:      5 * time.Second,
	}

	var resp client.Response
	resp.Reset()
	if err := c.Do(ctx, req, &resp); err != nil {
		return err // resp fields are undefined on error
	}

	status := resp.Status
	bodyLen := len(resp.Body)
	wireBytes := resp.BytesReceived

	// Read a response header by name (bytes comparison).
	var contentType []byte
	for i := range resp.Headers {
		if string(resp.Headers[i].Name) == "content-type" {
			contentType = resp.Headers[i].Value
		}
	}

	// Trailers populated only because WantTrailers was true.
	for i := range resp.Trailers {
		_ = resp.Trailers[i].Name
		_ = resp.Trailers[i].Value
	}

	_, _, _, _ = status, bodyLen, wireBytes, contentType
	return nil
}
```

To stream the body instead of buffering it (`Request.StreamBody`), or to send a
streaming `BodyReader`, the body is read via `Response.BodyReader`
(`io.ReadCloser`) which the caller must `Close()` — covered in the next section.

### Errors

`Do` returns:

- `client.ErrInvalidRequest` (wrapped) for failed up-front validation.
- `client.ErrClosed` after `Client.Close`.
- `client.ErrEmptyResponse` / `client.ErrInvalidStatus` when the response
  HEADERS frame lacks a usable `:status` pseudo-header.
- `*client.StreamResetError{Code}` when the peer sends `RST_STREAM` mid-response.
- `client.ErrBodyTooLarge` when the body (raw or decompressed) exceeds the
  configured `MaxResponseBodySize` / `MaxDecompressedSize` limits.
- A context error (e.g. `context.DeadlineExceeded`) on `ctx` cancellation or
  `Request.Timeout` expiry.
- `*client.DialError{Addr, Err}` when the lazy first dial fails.

## Streaming responses & body upload

The synchronous `Client.Do` buffers the entire response body in memory (`Response.Body`). For large or open-ended payloads — log tails, file downloads, server-sent event streams — that is wasteful or impossible. The `client` package offers two streaming reception models plus a streaming request-body model, all built on the same zero-allocation buffer-pool machinery as the rest of the client.

### Two ways to stream a response

| Approach | Entry point | Body surface | Reuse object |
|---|---|---|---|
| Event loop | `Client.DoStream` | `StreamResponse.Recv` → `StreamEvent` | `*StreamResponse` |
| io.Reader  | `Client.Do` with `Request.StreamBody = true` | `Response.BodyReader` (`io.ReadCloser`) | `*Response` |

Both return *after the initial HEADERS frame arrives* (status + headers are available immediately) and defer DATA-frame reception to the caller. Both require an HTTP/2 connection — they are **not supported on the HTTP/1.1 fallback transports** (`TransportH1SingleConn`, or `TransportALPN` after negotiating `http/1.1`); attempting either returns an error from `Do`/`DoStream`.

### Event-driven streaming: `DoStream` + `StreamResponse`

```go
func (c *Client) DoStream(ctx context.Context, req *Request, sr *StreamResponse) error
```

`DoStream` sends the request, waits for the response HEADERS frame, parses `:status` into `sr.Status` and the regular headers into `sr.Headers`, then returns. The caller pumps `sr.Recv` for the rest of the stream.

`StreamResponse` is **caller-provided and reusable**: `DoStream` calls `sr.reset()` internally before populating it, so you allocate one per goroutine and reuse it across calls. Its exported fields:

```go
type StreamResponse struct {
	Status  int                 // parsed from :status
	Headers []conn.HeaderField  // regular response headers; valid until Close()
	// ...unexported state...
}
```

#### The Recv loop and `StreamEvent`

```go
func (sr *StreamResponse) Recv(ctx context.Context) (StreamEvent, error)
```

`Recv` blocks until the next event arrives, the stream terminates, or `ctx` is cancelled. After it delivers the event whose `EndStream` is `true`, every subsequent call returns `ErrStreamEnded`.

```go
type StreamEvent struct {
	Type      EventType          // discriminator
	Data      []byte             // EventData payload (aliases a pooled buffer)
	Trailers  []conn.HeaderField // EventTrailers fields (aliases slab memory)
	ResetCode conn.ErrCode       // EventReset code
	EndStream bool               // true on the final event of the stream
}

type EventType uint8
const (
	EventData     EventType = iota + 1 // chunk of DATA payload, in StreamEvent.Data
	EventTrailers                      // response trailers, in StreamEvent.Trailers
	EventReset                         // peer sent RST_STREAM; code in ResetCode, EndStream always true
)
```

`EventType` has a `String()` method returning `"data"`, `"trailers"`, `"reset"`, or `"unknown"`.

> **Aliasing / recycle contract (critical).**
> `StreamEvent.Data` aliases a pooled connection-layer buffer that is **recycled on the next `Recv` or on `Close`**. `StreamEvent.Trailers` alias the response's header-slab memory, valid only **until `Close`**. If you need to retain either past that point, **copy the bytes** (e.g. `append([]byte(nil), ev.Data...)`). Never hold an `ev.Data` slice across a subsequent `Recv`/`Close` — the underlying bytes will be overwritten by a later DATA frame.

> **Close contract (critical).** `StreamResponse.Close` MUST be called if you do not drain the stream all the way to `EndStream`. It is idempotent (guarded by `sync.Once`), returns pooled header slabs and the current data buffer, and sends `RST_STREAM(CANCEL)` when neither side reached END_STREAM. `defer sr.Close()` is the safe idiom — it is harmless even after a full drain.

##### Download-stream example (write each chunk to a file as it arrives)

```go
func download(ctx context.Context, c *client.Client, path, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	req := &client.Request{
		Method: "GET",
		Path:   path,
		Headers: []conn.HeaderField{
			{Name: []byte("accept"), Value: []byte("application/octet-stream")},
		},
	}

	var sr client.StreamResponse // allocate once; reuse across calls
	if err := c.DoStream(ctx, req, &sr); err != nil {
		return err
	}
	defer sr.Close() // mandatory: cancels the stream if we bail early

	if sr.Status != 200 {
		return errors.New("unexpected status")
	}

	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			return nil // delivered the final event already
		}
		if err != nil {
			return err
		}
		switch ev.Type {
		case client.EventData:
			// ev.Data is valid only until the next Recv — consume it now.
			if _, werr := f.Write(ev.Data); werr != nil {
				return werr
			}
		case client.EventTrailers:
			// ev.Trailers valid until sr.Close(); copy if retaining.
			for _, t := range ev.Trailers {
				_ = t // e.g. checksum trailer
			}
		case client.EventReset:
			return errors.New("stream reset: " + ev.ResetCode.String())
		}
		if ev.EndStream {
			return nil
		}
	}
}
```

#### `WaitTrailers`: skip to the trailer block

```go
func (sr *StreamResponse) WaitTrailers(ctx context.Context) ([]conn.HeaderField, error)
```

`WaitTrailers` pumps `Recv` internally, **discarding any remaining `EventData`**, until `EventTrailers` arrives or the stream ends. Return-value semantics:

- Server sent trailers → returns the trailer fields (non-nil), `nil` error.
- Server sent an **empty** trailer block → returns a **non-nil empty slice** (so you can distinguish "trailers received" from "no trailers").
- Server sent **no** trailers, or the stream was **reset** → returns `(nil, nil)`. Use `Recv` directly if you must tell these two apart.
- Context cancelled → returns `(nil, ctx.Err())`.

If a prior `Recv` already delivered `EventTrailers`, the cached result is returned immediately with no further network I/O.

```go
func fetchTrailers(ctx context.Context, c *client.Client, path string) ([]conn.HeaderField, error) {
	var sr client.StreamResponse
	if err := c.DoStream(ctx, c.streamReq(path), &sr); err != nil {
		return nil, err
	}
	defer sr.Close()

	trailers, err := sr.WaitTrailers(ctx) // discards body, waits for trailers
	if err != nil {
		return nil, err
	}
	if trailers == nil {
		return nil, nil // no trailers (or reset) — body was consumed and dropped
	}
	// Copy out: trailer bytes are only valid until sr.Close().
	out := make([]conn.HeaderField, len(trailers))
	for i, t := range trailers {
		out[i] = conn.HeaderField{
			Name:  append([]byte(nil), t.Name...),
			Value: append([]byte(nil), t.Value...),
		}
	}
	return out, nil
}
```

### `io.ReadCloser` streaming: `Request.StreamBody` + `Response.BodyReader`

Set `Request.StreamBody = true` and call the ordinary `Client.Do`. `Do` returns as soon as the response HEADERS frame arrives; `Response.Status` and `Response.Headers` are populated, and the body is exposed as an `io.ReadCloser`:

```go
// On Response:
BodyReader io.ReadCloser // non-nil when the request had StreamBody=true
```

Behavior:

- `WantBody` is **ignored** when `StreamBody` is true (the body is not buffered into `Response.Body`).
- The reader streams DATA frames; `Read` returns `io.EOF` when END_STREAM or a trailers frame is observed.
- If the peer sends trailers, they are written into `Response.Trailers` just before `Read` returns `io.EOF`.
- If the peer resets the stream mid-body, `Read` returns a `*client.StreamResetError{Code: ...}`.
- Automatic decompression still applies: unless `Request.DisableDecompression` is set, a `content-encoding: gzip`/`deflate` body is decoded transparently and `BodyReader` yields the decompressed bytes.

> **Required-call contract (critical).** You **MUST** call `Response.BodyReader.Close()` (or `Response.Reset()`, which calls it for you) **before the next `Do` call** on that `Response`. `Close` is idempotent, returns the connection to the pool, and sends `RST_STREAM(CANCEL)` if the body was not fully drained. `Reset()` automatically invokes `BodyReader.Close()` when `BodyReader` is non-nil — so the standard reuse loop is already safe.

##### Download via `io.ReadCloser` (stream straight into a hash / writer)

```go
func downloadReader(ctx context.Context, c *client.Client, path, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	req := &client.Request{
		Method:     "GET",
		Path:       path,
		StreamBody: true, // Do returns after HEADERS; body via Response.BodyReader
	}

	var resp client.Response
	resp.Reset() // first Reset preallocates backing arrays
	if err := c.Do(ctx, req, &resp); err != nil {
		return err
	}
	// Reset() (or BodyReader.Close()) MUST run before reusing resp.
	defer resp.Reset()

	if resp.Status != 200 || resp.BodyReader == nil {
		return errors.New("unexpected response")
	}

	// io.Copy drives Read until io.EOF; surplus tail is buffered internally.
	if _, err := io.Copy(f, resp.BodyReader); err != nil {
		var rst *client.StreamResetError
		if errors.As(err, &rst) {
			return errors.New("peer reset stream: " + rst.Code.String())
		}
		return err
	}
	// Trailers (if the server sent any) are now in resp.Trailers.
	return nil
}
```

### Streaming the request body (upload)

The request side mirrors the response side. Instead of buffering bytes in `Request.Body []byte`, set `Request.BodyReader io.Reader`; the client reads from it in `16 KiB` chunks and emits DATA frames (the `conn` layer further chunks at the peer's `MAX_FRAME_SIZE` and respects flow control). Relevant `Request` fields:

```go
// On Request:
Body          []byte    // buffered body; ignored when BodyReader != nil
BodyReader    io.Reader // streaming request body; takes precedence over Body
ContentLength int64     // emits a content-length header iff BodyReader != nil && > 0
```

Rules verified from the code:

- `BodyReader` **takes precedence** over `Body` when non-nil — at most one is honored.
- A `content-length` request header is emitted **only** when `BodyReader != nil` *and* `ContentLength > 0`. Zero or negative `ContentLength` sends no `content-length` header (chunked, length-unknown upload). For a plain `Body []byte` the length is derived automatically and `ContentLength` is ignored.
- The reader is consumed to `io.EOF`; the final DATA frame carries END_STREAM (unless request trailers follow, in which case END_STREAM is set on the trailer HEADERS frame instead). A read error other than `io.EOF` aborts the upload.

> **Retry caveat.** A one-shot `io.Reader` (e.g. an `*os.File` already advanced, a network stream) cannot be re-read. If you rely on the client's automatic retry of idempotent requests, supply a `BodyReader` that can be re-created, or use `Request.Body` for retryable payloads.

##### Upload-stream example (stream a file, with content-length)

```go
func upload(ctx context.Context, c *client.Client, path, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	req := &client.Request{
		Method:        "POST",
		Path:          path,
		BodyReader:    f,         // streamed in DATA frames as it is read
		ContentLength: fi.Size(), // > 0 → emit content-length header
		Headers: []conn.HeaderField{
			{Name: []byte("content-type"), Value: []byte("application/octet-stream")},
		},
		WantBody: true,                 // buffer the (small) response body
		Timeout:  30 * time.Second,     // per-request deadline
	}

	var resp client.Response
	resp.Reset()
	defer resp.Reset()
	if err := c.Do(ctx, req, &resp); err != nil {
		return err
	}
	if resp.Status != 200 && resp.Status != 201 {
		return fmt.Errorf("upload rejected: status %d", resp.Status)
	}
	return nil
}
```

##### Length-unknown upload (no content-length)

```go
// pr is the read end of an io.Pipe being fed concurrently; total size unknown.
req := &client.Request{
	Method:     "POST",
	Path:       "/ingest",
	BodyReader: pr, // ContentLength left at 0 → no content-length header
}
```

You can also combine a streaming request body with a **streaming response** by setting both `BodyReader` and `StreamBody` on the same `Request` — the upload streams out while you `Read` the response back through `Response.BodyReader`.

### Quick contract checklist

- `DoStream`: always `defer sr.Close()`. `sr` is reusable across calls.
- `Recv`: copy `ev.Data` / `ev.Trailers` before the next `Recv`/`Close` if retaining; stop on `ErrStreamEnded` or `ev.EndStream`.
- `StreamBody`: always `Response.Reset()` (or `BodyReader.Close()`) before the next `Do`; not supported over HTTP/1.1.
- Upload: `BodyReader` beats `Body`; set `ContentLength > 0` only when known and you want a `content-length` header.

## Retry, idempotency, rate limiting, and timeouts

This section covers the resilience and traffic-shaping layers of the `client` package: the `Retryer` wrapper for bounded automatic retry, how the client classifies a request as idempotent (and how to override it), the built-in token-bucket rate limiter, and the per-request timeout.

### Idempotency classification

A request is *idempotent* — and therefore eligible for transport-level retry — based on its `Method`, unless explicitly overridden. The rule (`isIdempotent`) is:

- `GET`, `HEAD`, `OPTIONS`, `PUT`, `DELETE`, `TRACE` → idempotent.
- Everything else (notably `POST`, `PATCH`) → **not** idempotent.

To override the method-based default, set `Request.Idempotency`, an `IdempotencyMode`. The zero value `IdempotencyAuto` classifies by method; `ForceIdempotent` / `ForceNotIdempotent` force the answer. This lets you opt a `POST` into retry (when you know it is safe, e.g. an idempotency-key-protected endpoint) or opt a `PUT` out — with no addr-of-a-local `*bool` dance.

```go
// Force a POST to be treated as idempotent (safe because the handler
// dedupes on an Idempotency-Key header).
req := &client.Request{
	Method:    "POST",
	Path:      "/v1/charge",
	Authority: "api.example.com",
	Headers: []conn.HeaderField{
		{Name: []byte("idempotency-key"), Value: []byte("a1b2c3d4")},
		{Name: []byte("content-type"), Value: []byte("application/json")},
	},
	Body:        []byte(`{"amount":100}`),
	Idempotency: client.ForceIdempotent,
	WantBody:    true,
}
```

Note that `Idempotency` is only one of three gates the `Retryer` checks (see "What makes a request retryable" below). A request with a streaming `BodyReader` is never retried regardless of idempotency, because the reader cannot be rewound.

### Retryer

`Retryer` wraps a `*Client` and adds bounded automatic retry on transient transport failures. It is goroutine-safe and is constructed with `NewRetryer` — or, equivalently and more discoverably, with the `Client.Retryer` method:

```go
func NewRetryer(c *Client, opts RetryOptions) *Retryer
func (c *Client) Retryer(opts RetryOptions) *Retryer // == NewRetryer(c, opts)

r := c.Retryer(client.RetryOptions{MaxAttempts: 5})
err := r.Do(ctx, req, &resp)
```

`NewRetryer` fills zero-value fields in `opts` with defaults and preserves any non-zero values verbatim. The two methods mirror `Client`:

```go
func (r *Retryer) Do(ctx context.Context, req *Request, resp *Response) error
func (r *Retryer) DoStream(ctx context.Context, req *Request, sr *StreamResponse) error
```

`Retryer.Do`/`DoStream` delegate to the wrapped `*Client` and only loop when retry is enabled for the request; otherwise they fall through to a single `Client.Do`/`Client.DoStream` call.

#### RetryOptions

```go
type RetryOptions struct {
	MaxAttempts int                                   // total attempts; 1 = no retry. Zero → 3.
	Backoff     func(attempt int) time.Duration       // wait before attempt i (0-indexed; attempt 0 must return 0). nil → default.
	IsRetryable func(err error, resp *Response) bool   // supplements built-in classification. nil → built-ins only.
	Rand        *rand.Rand                             // seeds default-backoff jitter; ignored when Backoff is set.
}
```

Field semantics, exactly as implemented:

- **`MaxAttempts`** — the maximum *total* attempts including the first. `1` disables retry; `0` is replaced by the default of `3`.
- **`Backoff`** — returns the wait before attempt `i`. It is called with `attempt` 0-indexed; attempt `0` is the first try and is never delayed (the loop only calls `Backoff` for `attempt > 0`). `nil` installs the default: truncated exponential backoff `100ms, 200ms, 400ms, …` capped at `5s`, with ±25% uniform jitter. The default backoff serializes its non-goroutine-safe RNG behind an internal mutex, so it is safe under concurrent `Do` calls.
- **`IsRetryable`** — an optional predicate consulted *in addition to* the built-in classification. It is invoked in two distinct shapes: on a transport error it is called as `IsRetryable(err, nil)`; on a *successful* return it is called as `IsRetryable(nil, resp)` so you can retry on, e.g., a `503` status. Always guard `resp != nil` before dereferencing. `nil` means only the built-in transport errors trigger retry, and a successful response is never retried.
- **`Rand`** — seeds the default backoff's jitter. `nil` uses a time-seeded source owned by the `Retryer`. Ignored entirely when `Backoff` is non-nil.

#### What makes a request retryable

`Retryer` only enters its retry loop when **all** of the following hold (`canRetry`):

1. `MaxAttempts > 1`, and
2. the request is idempotent (`isIdempotent`, including the `Idempotency` override), and
3. `req.BodyReader == nil` (a streaming body cannot be replayed).

If any fails, `Retryer.Do`/`DoStream` performs exactly one underlying call.

Within the loop, each failure is classified:

- **Built-in retryable errors** (`builtinShouldRetry`): a `*StreamResetError` whose `Code` is `conn.ErrCodeRefusedStream`, `frame.ErrCodeInternalError`, or `frame.ErrCodeEnhanceYourCalm`; any error wrapping `conn.ErrGoAway`; any `*DialError`; and `ErrDialBackoff`.
- **Hard-stop errors** (`isHardStop`) are *never* retried, even if your `IsRetryable` returns true: `context.Canceled`, `context.DeadlineExceeded`, `ErrPoolClosed`, `ErrClosed`, and `ErrInvalidRequest`.
- Otherwise the user-supplied `IsRetryable(err, nil)` decides.

On a *successful* `Do` (err == nil), the loop consults `IsRetryable(nil, resp)`: if it returns true the response is retried (after `resp.Reset()`), otherwise `Do` returns `nil`. `DoStream` never consults `IsRetryable` on success — a successful return hands stream ownership to the caller, and any response-status-based retry decision is the caller's concern.

#### Required-call contracts

- The same `Response`/`StreamResponse` reuse contract as `Client` applies. The retry loop itself calls `resp.Reset()` (for `Do`) or `sr.reset()` (for `DoStream`) **between attempts**, so you do not Reset in the loop body — but you still must allocate them and treat their backing bytes as valid only until the next call. For `DoStream`, you must still `Close()` the returned `StreamResponse` when done.
- The `OnRetry` hook (if set on the `Client`) fires once per retry attempt (before the backoff sleep), carrying `RetryEvent{Method, Path, Attempt, Err, Backoff}`. Each retry also increments `Metrics.Counters.Retries`.

#### A retrying client

```go
package main

import (
	"context"
	"crypto/tls"
	"log"
	"math/rand"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	c, err := client.NewClient(client.ClientOptions{
		Addr: "api.example.com:443",
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{NextProtos: []string{"h2"}}},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 4, // 1 initial + up to 3 retries
		// Custom backoff: fixed 50ms steps (attempt 0 must return 0).
		Backoff: func(attempt int) time.Duration {
			if attempt <= 0 {
				return 0
			}
			return time.Duration(attempt) * 50 * time.Millisecond
		},
		// Also retry on 503 Service Unavailable in addition to the
		// built-in transport-error classification.
		IsRetryable: func(err error, resp *client.Response) bool {
			return err == nil && resp != nil && resp.Status == 503
		},
		Rand: rand.New(rand.NewSource(1)), // ignored here because Backoff is set
	})

	req := &client.Request{
		Method:    "GET", // idempotent → eligible for retry
		Path:      "/v1/status",
		Authority: "api.example.com",
		WantBody:  true,
	}

	var resp client.Response
	resp.Reset() // first Reset preallocates backing arrays
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The Retryer Resets resp between attempts internally; you only
	// Reset before the first use and before reusing across iterations.
	if err := r.Do(ctx, req, &resp); err != nil {
		log.Fatalf("request failed after retries: %v", err)
	}
	log.Printf("status=%d body=%q", resp.Status, resp.Body)
}
```

For the default backoff, simply omit `Backoff` (and optionally seed `Rand`):

```go
r := client.NewRetryer(c, client.RetryOptions{
	MaxAttempts: 3, // default; same as leaving it 0
	// Backoff nil → exponential 100ms…5s with ±25% jitter.
})
```

Retrying a streaming request retries only *before* the initial HEADERS frame arrives; once headers are delivered, ownership passes to you:

```go
var sr client.StreamResponse
if err := r.DoStream(ctx, req, &sr); err != nil {
	log.Fatal(err)
}
defer sr.Close() // mandatory if you don't drain to EndStream
for {
	ev, err := sr.Recv(ctx)
	if err == client.ErrStreamEnded {
		break
	}
	if err != nil {
		log.Fatal(err)
	}
	_ = ev // handle ev.Type / ev.Data ...
}
```

### Rate limiting

Rate limiting is configured on the `Client` itself (there is no exported `RateLimiter` type — the token bucket is internal and engaged automatically). Set two fields on `ClientOptions`:

- **`RateLimitPerSecond float64`** — caps outgoing request rate via a token bucket. Zero disables rate limiting (the default).
- **`RateLimitBurst float64`** — maximum tokens consumable back-to-back without replenishment. Zero defaults the burst to `RateLimitPerSecond` (one second of accumulated tokens). Only meaningful when `RateLimitPerSecond > 0`.

When rate limiting is enabled, every `Client.Do` and `Client.DoStream` call acquires one token *before* dialing or sending anything: it blocks until a token is available **or `ctx` is cancelled**, in which case the call returns `ctx.Err()`. Tokens replenish continuously at `RateLimitPerSecond`, tracked against a monotonic clock so wall-clock adjustments do not affect the rate. The limiter is goroutine-safe, so a single `Client` shared across many goroutines enforces one global QPS budget.

Because the token is taken inside `Client.Do`, a `Retryer` wrapping that client is also rate-limited: every attempt (initial and each retry) consumes its own token.

#### A rate-limited client

```go
package main

import (
	"context"
	"crypto/tls"
	"log"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	c, err := client.NewClient(client.ClientOptions{
		Addr: "api.example.com:443",
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{NextProtos: []string{"h2"}}},
		},
		RateLimitPerSecond: 200, // at most ~200 req/s
		RateLimitBurst:     50,  // allow short bursts of up to 50
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fan out load across goroutines; the shared limiter enforces the
	// global 200 req/s budget. Do blocks on the token, returning ctx.Err()
	// if the deadline fires while waiting.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var resp client.Response // one Response per goroutine
			req := &client.Request{
				Method:    "GET",
				Path:      "/v1/ping",
				Authority: "api.example.com",
				WantBody:  true,
			}
			for ctx.Err() == nil {
				resp.Reset() // mandatory before each reuse
				if err := c.Do(ctx, req, &resp); err != nil {
					if ctx.Err() != nil {
						return // deadline/cancel while rate-limited or in-flight
					}
					log.Printf("request error: %v", err)
					continue
				}
				_ = resp.Status
			}
		}()
	}
	wg.Wait()
}
```

### Per-request timeout

`Request.Timeout` (a `time.Duration`) is an independent per-request deadline. When `> 0`, `Client.Do`/`DoStream` derives a child context with `context.WithTimeout(ctx, req.Timeout)`; when it fires, the request fails with `context.DeadlineExceeded` and the in-flight stream is reset with `RST_STREAM(CANCEL)`. Zero means "use the parent `ctx`'s deadline (or none)". The per-request timeout composes with — does not replace — any deadline already on `ctx`; whichever fires first wins.

Note the ordering inside `Do`: validation runs first, then the rate-limiter token is acquired (which may block on the *parent* ctx), and only then is the `Timeout` sub-context derived. So `Request.Timeout` bounds the request exchange, not the time spent waiting for a rate-limit token.

Because `context.DeadlineExceeded` is a hard-stop error, a request that times out is **never** retried by the `Retryer`, even with a custom `IsRetryable`.

```go
req := &client.Request{
	Method:    "GET",
	Path:      "/v1/slow",
	Authority: "api.example.com",
	WantBody:  true,
	Timeout:   2 * time.Second, // independent per-request deadline
}

var resp client.Response
resp.Reset()
err := c.Do(context.Background(), req, &resp)
if errors.Is(err, context.DeadlineExceeded) {
	log.Println("request exceeded its 2s budget; stream was reset with CANCEL")
}
```

Combining all three layers — a rate-limited client, automatic retry, and a per-request timeout — is the typical load-generator configuration: configure `RateLimitPerSecond`/`RateLimitBurst` on the `Client`, set `Request.Timeout` per request, and wrap the client in a `Retryer`. Just remember that `context.DeadlineExceeded` from `Timeout` is a hard stop, so a timed-out attempt ends the retry loop rather than triggering another attempt.

## Connection Pooling & Service Discovery

This section covers how the `client` package multiplexes load across many
connections and many backends:

- **`TransportPool`** — a single-address pool of `*conn.Conn` connections
  (`Pool` + `PoolOptions` + `Stats`).
- **`TransportManaged`** — a *managed pool* that fans the single-address pool
  logic across an address set discovered by a `Resolver` and chosen by a
  `Selector`, with per-address drain lifecycle controlled by `DrainMode`.
- **Service discovery** — `Resolver` (with `StaticResolver` and `DNSResolver`)
  and load balancing via `Selector` (`RoundRobin`, `Random`, `Hash`).
- **Observability & pre-warming** — `Client.PoolStats()` and `Client.Warmup(n)`.

The pool dials HTTP/2 connections through `conn.Dial`, so `ConnOpts.Dialer`
is always required (`NewClient` rejects a nil dialer). For TLS+ALPN h2 use
`&conn.TLSDialer{}`; for h2c prior-knowledge use `&conn.PlaintextDialer{}`.

### 1. Single-address pool — `TransportPool`

Select `TransportPool` and supply a non-nil `*PoolOptions`. The pool keeps up
to `MaxConnsPerHost` live connections to one `Addr`, multiplexing concurrent
streams onto each connection up to the effective per-conn stream cap.

#### `PoolOptions`

```go
type PoolOptions struct {
	MaxConnsPerHost   int           // live conns cap; 0 → 1
	MaxStreamsPerConn int           // soft cap; effective = min(this, peer SETTINGS_MAX_CONCURRENT_STREAMS); 0 → peer (or 100 if both unbounded)
	IdleTimeout       time.Duration // close conn idle (active==0) longer than this; 0 → never
	HealthCheckPeriod time.Duration // actor sweep tick (idle/dead/cap-refresh); 0 → 30s
	DialBackoff       time.Duration // refuse new dials this long after a dial failure; 0 → 1s
	AcquireTimeout    time.Duration // bound on waiting for capacity; 0 → governed by ctx only
	DialTimeout       time.Duration // bound a single conn.Dial; 0 → 30s
}
```

Note the defaults are applied at construction: a zero `MaxConnsPerHost`
becomes **1** (effectively single-connection). When neither
`MaxStreamsPerConn` nor the peer's `SETTINGS_MAX_CONCURRENT_STREAMS` bounds
the stream count, the pool falls back to **100** streams per connection
(`defaultMaxConcurrentStreams`).

```go
func newPooledClient() (*client.Client, error) {
	opts := client.ClientOptions{
		Addr:     "api.example.com:443",
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   4,
			MaxStreamsPerConn: 64,
			IdleTimeout:       90 * time.Second,
			HealthCheckPeriod: 15 * time.Second,
			DialBackoff:       2 * time.Second,
			AcquireTimeout:    5 * time.Second,
			DialTimeout:       10 * time.Second,
		},
	}
	return client.NewClient(opts)
}
```

> **Contract:** `Pool` is required iff `Transport == TransportPool`. Passing a
> non-nil `Pool` with any other transport — or a nil `Pool` with
> `TransportPool` — fails `NewClient` with `ErrInvalidPoolOptions`.

Once constructed, the pool is invisible to request code — you issue requests
through the normal `Do` / `DoStream` API and the pool acquires/releases a
connection per request automatically:

```go
func sendOne(ctx context.Context, c *client.Client) error {
	req := &client.Request{
		Method:   "GET",
		Path:     "/healthz",
		WantBody: true,
		Headers: []conn.HeaderField{
			{Name: []byte("accept"), Value: []byte("application/json")},
		},
	}
	var resp client.Response
	resp.Reset() // REQUIRED before each (re)use; retains backing arrays
	if err := c.Do(ctx, req, &resp); err != nil {
		return err
	}
	_ = resp.Status
	_ = resp.Body // valid only until the next resp.Reset()
	return nil
}
```

#### Pool-specific acquire errors

`Do`/`DoStream` (and `PoolStats`-adjacent code) can surface these sentinels:

- `ErrAcquireTimeout` — `PoolOptions.AcquireTimeout` elapsed before capacity
  freed up.
- `ErrDialBackoff` — a recent dial failed and the `DialBackoff` window has not
  elapsed (returned only when there is no live capacity at all).
- `ErrPoolClosed` — returned after the pool is closed.

```go
var resp client.Response
resp.Reset()
switch err := c.Do(ctx, req, &resp); {
case err == nil:
	// success
case errors.Is(err, client.ErrAcquireTimeout):
	// all conns saturated within AcquireTimeout
case errors.Is(err, client.ErrDialBackoff):
	// backend unreachable; backoff window still open
default:
	// other transport / protocol error
}
```

### 2. Lifecycle: `Close`, `Shutdown`, `Warmup`

These methods live on `*client.Client` and delegate to the underlying
transport (single-conn, pool, or managed).

```go
// Pre-dial connections before a traffic burst so the first requests don't
// pay TLS + HTTP/2 handshake latency. Returns immediately; dial errors are
// surfaced through the OnDial hook. Idempotent and capped at the
// transport's MaxConnsPerHost (1 for TransportSingleConn).
c.Warmup(4)

// Graceful: new requests get ErrConnDraining; in-flight requests have
// gracefulTimeout to finish, then conns are force-closed. Idempotent.
_ = c.Shutdown(10 * time.Second)

// Hard close: releases the transport; later Do/DoStream return ErrClosed.
// Idempotent.
_ = c.Close()
```

`Warmup(n)` works by submitting `n` short-lived acquires (each capped at
`MaxConnsPerHost`); each one that resolves to a connection within the warm-up
window is immediately released. For a managed pool, the `n` dials are
distributed across the currently-known sub-pools (`ceil(n / numSubPools)`
each), so warm-up only pre-dials addresses that have already been resolved.

### 3. Pool statistics — `Stats` and `PoolStats()`

`Client.PoolStats()` returns a coherent snapshot of the underlying pool. It
returns the **zero `Stats`** when the transport is neither a pool nor a managed
pool (e.g. `TransportSingleConn`) or when the pool is already closed — it never
panics.

```go
type Stats struct {
	ActiveConns      int // live conns in the pool
	InFlightStreams  int // sum of active streams across conns
	Waiters          int // acquires queued waiting for capacity
	InFlightDials    int // dials currently in progress
	Addresses        int // managed pool only: addresses in the resolved set
	DrainingSubpools int // managed pool only: sub-pools currently draining
}
```

`Addresses` and `DrainingSubpools` are populated only for `TransportManaged`;
they are zero for single-address `TransportPool`.

```go
func logPool(c *client.Client) {
	st := c.PoolStats()
	log.Printf("conns=%d streams=%d waiters=%d dials=%d addrs=%d draining=%d",
		st.ActiveConns, st.InFlightStreams, st.Waiters,
		st.InFlightDials, st.Addresses, st.DrainingSubpools)
}
```

`PoolStats()` is safe to call concurrently with in-flight requests.

### 4. Managed pool — `TransportManaged` + `DrainMode`

`TransportManaged` drives a pool *per discovered address*. It requires a
`Resolver` (and optionally a `Selector`, defaulting to `RoundRobin()`); it
**must not** set `Addr` — the resolver owns addressing.

```go
func newManagedClient() (*client.Client, error) {
	sel := client.RoundRobin()
	opts := client.ClientOptions{
		// Addr MUST be empty for TransportManaged.
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: client.TransportManaged,
		Resolver: client.StaticResolver(
			client.Address{Host: "10.0.0.1", Port: 443},
			client.Address{Host: "10.0.0.2", Port: 443},
			client.Address{Host: "10.0.0.3", Port: 443},
		),
		Selector:  sel,
		DrainMode: client.DrainGraceful,
		// Pool is OPTIONAL here: when set, its PoolOptions are applied to
		// every per-address sub-pool. Nil → zero-value PoolOptions per sub-pool.
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 100,
			IdleTimeout:       60 * time.Second,
		},
	}
	return client.NewClient(opts)
}
```

> **Contract:** `NewClient` returns `ErrInvalidOptions` if `Resolver` is nil
> with `TransportManaged`, or if `Addr` is non-empty with `TransportManaged`.
> If the resolver's initial `Resolve` returns a non-nil error **and** zero
> addresses, `NewClient` returns that error. A zero-address-but-no-error
> initial resolve starts the pool empty (acquires return `ErrNoAddresses`
> until an address appears).

Each request picks an address via the `Selector`, acquires from that address's
sub-pool, and on a *dial-only* failure (a `*DialError`, `ErrDialBackoff`, or a
racing `ErrPoolClosed`) transparently fails over to the next untried address in
the active set. Non-dial errors (protocol, stream reset) are returned to the
caller without failover. When the active set is empty the acquire returns
`ErrNoAddresses`.

#### `DrainMode`

When the resolver *removes* an address, the corresponding sub-pool is marked
draining (no new acquires) and handled per `DrainMode`:

```go
const (
	DrainGraceful DrainMode = iota // refuse new acquires; close sub-pool when InFlightStreams hits 0
	DrainHard                      // close every conn immediately; in-flight streams get RST_STREAM(CANCEL)
	DrainLazy                      // refuse new acquires; idle eviction eventually closes conns
)
```

`DrainGraceful` (the zero value) polls the removed sub-pool's `Stats` with
exponential backoff (20 ms → 5 s) and closes it once `InFlightStreams == 0`.
`DrainHard` closes immediately. `DrainLazy` leaves closure to `IdleTimeout`
eviction. Set it via `ClientOptions.DrainMode`.

#### Observing set changes — `OnResolverUpdate`

The managed pool fires `Hooks.OnResolverUpdate` (only for `TransportManaged`)
whenever it applies a changed address set:

```go
hooks := &client.Hooks{
	OnResolverUpdate: func(e client.ResolverUpdateEvent) {
		log.Printf("resolver update: +%d -%d total=%d",
			len(e.Added), len(e.Removed), e.Total)
	},
	OnConnClose: func(e client.ConnCloseEvent) {
		log.Printf("conn %s closed: %s", e.Addr, e.Reason) // reason: idle|dead|goaway|manual
	},
}
opts.Hooks = hooks
```

> Hooks must not block — pool hooks (`OnResolverUpdate`, `OnConnClose`,
> `OnDial`) fire on the pool actor goroutine and will stall it.

### 5. Resolvers — `Resolver`, `StaticResolver`, `DNSResolver`

```go
type Resolver interface {
	// Resolve returns the current address set. (err!=nil, len==0) is a hard
	// failure; (err!=nil, len>0) is a soft warning (use cached set).
	Resolve(ctx context.Context) ([]Address, error)
	// Watch streams the FULL address set (never deltas); first message is the
	// current set. Implementations without push return ErrWatchUnsupported,
	// and the managed pool falls back to polling Resolve.
	Watch(ctx context.Context) (<-chan []Address, error)
}
```

#### `Address`

```go
type Address struct {
	Host       string            // IP literal or DNS name (never re-resolved by the pool)
	Port       int
	Attributes map[string]string // optional metadata for custom Selectors; built-ins ignore it
}
// a.String() == net.JoinHostPort(Host, port) — IPv6 literals get brackets.
```

> **Gotcha:** an `Address` with a non-nil `Attributes` map is **not
> comparable** — it cannot be a Go map key or compared with `==`. The
> managed pool keys sub-pools by `Address.String()`, so this is internally
> safe, but your own code must not use such an `Address` as a map key.

#### `StaticResolver`

Returns a fixed set; `Watch` emits it once and closes (so the managed pool
then runs on its ticker). The slice is **copied** — later caller mutation has
no effect.

```go
r := client.StaticResolver(
	client.Address{Host: "10.0.0.1", Port: 8443},
	client.Address{Host: "10.0.0.2", Port: 8443},
)
```

#### `DNSResolver` + `DNSOptions`

DNS-backed resolver over A/AAAA lookups with TTL caching. `Watch` re-resolves
every `TTL` and emits only when the set changes.

```go
type DNSOptions struct {
	TTL        time.Duration   // cache lifetime AND Watch ticker period; 0 → 30s
	Resolver   *net.Resolver   // underlying resolver; nil → net.DefaultResolver
	PreferIPv4 bool            // drop AAAA results when set
}

func newDNSManagedClient() (*client.Client, error) {
	resolver := client.DNSResolver("backend.svc.cluster.local", 8443, client.DNSOptions{
		TTL:        15 * time.Second,
		PreferIPv4: true,
		// Resolver: net.DefaultResolver, // optional
	})
	sel, err := client.Hash(func(pc client.PickContext) string {
		// Sticky routing by request path; nil-safe.
		if pc.Request != nil {
			return pc.Request.Path
		}
		return ""
	})
	if err != nil {
		return nil, err
	}
	return client.NewClient(client.ClientOptions{
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: client.TransportManaged,
		Resolver:  resolver,
		Selector:  sel,
		DrainMode: client.DrainGraceful,
		Pool:      &client.PoolOptions{MaxConnsPerHost: 2},
	})
}
```

> **`DNSResolver.Resolve` semantics:** a *lookup error* with a non-empty cache
> returns `(cache, err)` — the cache wins and the error is a soft warning. A
> *successful* lookup that yields zero addresses (every endpoint deregistered,
> or all filtered out by `PreferIPv4`) is authoritative: the cache is cleared
> and `ErrNoAddresses` is returned so dead backends drain. The managed pool
> treats `ErrNoAddresses` as an empty-but-valid set, not a hard failure.

### 6. Selectors — `Selector`, `RoundRobin`, `Random`, `Hash`, `PickContext`

```go
type Selector interface {
	Pick(set []Address, pc PickContext) (Address, error)
}

type PickContext struct {
	Request *Request // the in-flight request when Pick runs on the acquire path; may be nil
}
```

All built-in selectors are goroutine-safe and return `ErrNoAddresses` on an
empty candidate set.

| Constructor | Behavior |
|---|---|
| `RoundRobin() Selector` | Stateful; rotates the set in order via a shared atomic counter (exact under concurrency). |
| `Random(rng *rand.Rand) Selector` | Uniform random. `nil` rng → a time-seeded `*rand.Rand` owned by the selector (serialized internally). |
| `Hash(keyFn func(PickContext) string) (Selector, error)` | Deterministic FNV-1a hash of `keyFn(pc)` mod `len(set)`. **Returns `ErrNilKeyFn` for a nil keyFn** — check the error. A `keyFn` returning `""` makes `Pick` return `ErrNoAddresses`. |

```go
// Round-robin (default if ClientOptions.Selector is nil):
rr := client.RoundRobin()

// Random with explicit RNG:
rnd := client.Random(rand.New(rand.NewSource(42)))

// Consistent hashing keyed on an authority/header — note the error return:
hsel, err := client.Hash(func(pc client.PickContext) string {
	if pc.Request == nil {
		return "" // → Pick returns ErrNoAddresses
	}
	return pc.Request.Authority
})
if err != nil {
	log.Fatal(err) // ErrNilKeyFn
}
_ = rr
_ = rnd
_ = hsel
```

A custom `Selector` can read `Address.Attributes` (e.g. for zone- or
weight-aware balancing); the built-in selectors ignore it.

### 7. End-to-end: static multi-backend with a selector

```go
func staticFleet(ctx context.Context) error {
	c, err := client.NewClient(client.ClientOptions{
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: client.TransportManaged,
		Resolver: client.StaticResolver(
			client.Address{Host: "10.0.0.1", Port: 443},
			client.Address{Host: "10.0.0.2", Port: 443},
			client.Address{Host: "10.0.0.3", Port: 443},
		),
		Selector:  client.RoundRobin(),
		DrainMode: client.DrainGraceful,
		Pool:      &client.PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 100},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	c.Warmup(6) // pre-dial across the three backends

	req := &client.Request{
		Method:   "GET",
		Path:     "/v1/ping",
		WantBody: true,
	}
	var resp client.Response
	for i := 0; i < 100; i++ {
		resp.Reset()
		if err := c.Do(ctx, req, &resp); err != nil {
			return err
		}
		_ = resp.Status
	}

	st := c.PoolStats()
	log.Printf("addrs=%d conns=%d", st.Addresses, st.ActiveConns)
	return nil
}
```

This is the canonical load-generator setup: a managed pool over a fixed backend
fleet, round-robin selection, two connections per backend each multiplexing up
to 100 streams, pre-warmed before the burst, with a single reused `Request` and
`Response`.

## Observability & Advanced Protocol

This section covers the client's instrumentation surface (Hooks and Metrics),
HTTP/2 server push, request prioritization, extended CONNECT (RFC 8441),
request trailers, response-decompression control, and the typed error model.
Every symbol referenced below lives in package
`github.com/lodgvideon/poseidon-http-client/client`; HTTP/2 wire types such as
`HeaderField`, `ErrCode`, and the per-frame `Priority` come from
`.../conn` and `.../frame`.

### 1. Hooks

`client.Hooks` is an optional set of lifecycle callbacks. Every field is a
function value; a `nil` field is skipped at zero cost. Supply it via
`ClientOptions.Hooks`, or swap it at runtime with `Client.SetHooks`.

```go
type Hooks struct {
	OnRequestStart    func(RequestStartEvent)
	OnRequestComplete func(RequestCompleteEvent)
	OnRetry           func(RetryEvent)
	OnDial            func(DialEvent)
	OnConnClose       func(ConnCloseEvent)
	OnResolverUpdate  func(ResolverUpdateEvent)
}
```

When each hook fires (from the doc comments in `hooks.go`):

- `OnRequestStart` — at the top of `Do`/`DoStream`, before transport acquire.
- `OnRequestComplete` — when `Do` returns, or when `DoStream` returns its
  initial `StreamResponse` (or an error).
- `OnRetry` — inside the `Retryer` between attempts, *before* the backoff
  sleep; the event carries the computed `Backoff`.
- `OnDial` — after a transport dial completes (success or error).
- `OnConnClose` — when a conn is evicted from a pool.
- `OnResolverUpdate` — when the managed pool applies a new address set from the
  `Resolver`. It does **not** fire for `TransportSingleConn` or `TransportPool`.

The `*Event` payload types, with their exact fields:

```go
type RequestStartEvent struct {
	Method, Path, Authority string
	Attempt                 int // 0 for first try, >=1 for retries
}

type RequestCompleteEvent struct {
	Method, Path, Authority string
	Status                  int // 0 if no headers received
	Err                     error
	Latency                 time.Duration
	BytesSent, BytesRecv    int64 // BytesSent = len(req.Body); BytesRecv = total DATA payload
	Attempt                 int
}

type RetryEvent struct {
	Method, Path string
	Attempt      int
	Err          error
	Backoff      time.Duration
}

type DialEvent struct {
	Addr     string
	Err      error
	Duration time.Duration
}

type ConnCloseEvent struct {
	Addr   string
	Reason CloseReason
}

type ResolverUpdateEvent struct {
	Added, Removed []Address
	Total          int
}
```

`ConnCloseEvent.Reason` is a `CloseReason` enum with a stable lowercase
`String()` suitable for metric labels:

```go
const (
	CloseIdle   CloseReason = iota // idle past PoolOptions.IdleTimeout
	CloseDead                      // conn.IsAlive() returned false at eviction
	CloseGoAway                    // peer sent GOAWAY
	CloseManual                    // closed via Pool.Close / Client.Close
)
// CloseReason.String() => "idle" | "dead" | "goaway" | "manual" | "unknown"
```

Contract: hooks **must not block**. Request hooks run on the caller's
goroutine; pool hooks (`OnConnClose`, `OnResolverUpdate`) run on the pool
actor goroutine — a blocking hook stalls request processing or the pool actor.
Hook panics propagate; wrap with `recover()` if you need isolation.

```go
hooks := &client.Hooks{
	OnRequestStart: func(e client.RequestStartEvent) {
		fmt.Printf("start %s %s (attempt %d)\n", e.Method, e.Path, e.Attempt)
	},
	OnRequestComplete: func(e client.RequestCompleteEvent) {
		fmt.Printf("done %s %s status=%d sent=%d recv=%d in %s err=%v\n",
			e.Method, e.Path, e.Status, e.BytesSent, e.BytesRecv, e.Latency, e.Err)
	},
	OnRetry: func(e client.RetryEvent) {
		fmt.Printf("retry %s attempt=%d backoff=%s err=%v\n",
			e.Path, e.Attempt, e.Backoff, e.Err)
	},
	OnDial: func(e client.DialEvent) {
		fmt.Printf("dial %s in %s err=%v\n", e.Addr, e.Duration, e.Err)
	},
	OnConnClose: func(e client.ConnCloseEvent) {
		fmt.Printf("conn %s closed: %s\n", e.Addr, e.Reason) // Reason.String()
	},
}

c, err := client.NewClient(client.ClientOptions{
	Addr:     "example.com:443",
	ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{NextProtos: []string{"h2"}}}},
	Hooks:    hooks,
})
if err != nil {
	// handle
}
defer c.Close()

// Replace the hook set atomically at any time; nil disables all hooks.
c.SetHooks(nil)
```

### 2. Metrics

Each `Client` owns a live `*Metrics`. `Client.Metrics()` returns the stable
pointer (do **not** value-copy it); `Client.MetricsSnapshot()` returns a frozen,
value-copyable `MetricsSnapshot`.

```go
type Metrics struct {
	Counters Counters
	Latency  struct {
		Request Histogram
		Dial    Histogram
		Acquire Histogram
	}
}
```

`Counters` is a lock-free struct of `atomic.Int64` fields. Read individual
fields with `.Load()`, or call `Snapshot()` for a value-copyable
`CountersSnapshot`:

```go
type Counters struct {
	RequestsStarted   atomic.Int64
	RequestsSucceeded atomic.Int64 // a response was received — ANY status
	RequestsErrored   atomic.Int64 // Do returned non-nil err (transport/protocol)
	Responses2xx      atomic.Int64 // completed with a 2xx status
	ResponsesNon2xx   atomic.Int64 // completed with a non-2xx status (1xx/3xx/4xx/5xx)
	Retries           atomic.Int64
	DialsAttempted    atomic.Int64
	DialsFailed       atomic.Int64
	ConnsClosed       atomic.Int64 // summed across all CloseReason values
	GoAwaysReceived   atomic.Int64
}
```

> For a load generator, measure real success rate with `Responses2xx` /
> (`Responses2xx`+`ResponsesNon2xx`). `RequestsSucceeded` only means "a response
> arrived" — it counts 4xx/5xx as well, so it conflates "got a response" with
> "got a good one". `Responses2xx + ResponsesNon2xx == RequestsSucceeded`.

`Histogram` is a lock-free log2-bucket latency histogram (64 buckets spanning
`[1ns, 2^63 ns)`). `Observe(d time.Duration)` is allocation-free. `Snapshot()`
freezes it into a `HistogramSnapshot`, which exposes `Mean()` and `Quantile(q)`:

```go
func (h *Histogram) Observe(d time.Duration)
func (h *Histogram) Snapshot() HistogramSnapshot

func (s HistogramSnapshot) Mean() time.Duration              // 0 on empty
func (s HistogramSnapshot) Quantile(q float64) time.Duration // 0<=q<=1, 0 on empty
```

`Quantile` returns the **upper edge** of the bucket containing the q-th
observation, so it is a bucket-edge approximation precise only to a factor of 2.
`Mean` returns 0 when no observations were recorded.

`MetricsSnapshot` mirrors `Metrics` but with the snapshot value types:

```go
type MetricsSnapshot struct {
	Counters CountersSnapshot
	Latency  struct {
		Request HistogramSnapshot
		Dial    HistogramSnapshot
		Acquire HistogramSnapshot
	}
}
```

```go
// Read live counters cheaply (each field is goroutine-safe individually).
started := c.Metrics().Counters.RequestsStarted.Load()

// For a coherent, value-safe view, take a snapshot.
snap := c.MetricsSnapshot()
fmt.Printf("started=%d ok=%d err=%d retries=%d goaways=%d\n",
	snap.Counters.RequestsStarted,
	snap.Counters.RequestsSucceeded,
	snap.Counters.RequestsErrored,
	snap.Counters.Retries,
	snap.Counters.GoAwaysReceived,
)
fmt.Printf("request p50=%s p99=%s mean=%s\n",
	snap.Latency.Request.Quantile(0.50),
	snap.Latency.Request.Quantile(0.99),
	snap.Latency.Request.Mean(),
)
fmt.Printf("dial mean=%s acquire p99=%s\n",
	snap.Latency.Dial.Mean(),
	snap.Latency.Acquire.Quantile(0.99),
)
```

Note: `Snapshot()` (on `Counters`, `Histogram`, and `Metrics`) reads each field
with a single atomic load. Field-to-field consistency is best-effort — a
concurrent update may land between individual loads.

#### Instrumented client (Hooks + Metrics together)

```go
func newInstrumentedClient() (*client.Client, error) {
	hooks := &client.Hooks{
		OnRequestComplete: func(e client.RequestCompleteEvent) {
			if e.Err != nil {
				fmt.Printf("FAIL %s %s: %v\n", e.Method, e.Path, e.Err)
			}
		},
	}
	c, err := client.NewClient(client.ClientOptions{
		Addr: "example.com:443",
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{NextProtos: []string{"h2"}}},
		},
		Hooks: hooks,
	})
	if err != nil {
		return nil, err
	}

	// Periodically scrape metrics into your monitoring system.
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for range t.C {
			s := c.MetricsSnapshot()
			fmt.Printf("qps_started=%d p99=%s\n",
				s.Counters.RequestsStarted, s.Latency.Request.Quantile(0.99))
		}
	}()
	return c, nil
}
```

### 3. Server push (PUSH_PROMISE)

Server push is opt-in via `ClientOptions.PushHandler`. When you set a non-nil
handler, `NewClient` automatically flips `ConnOpts.EnablePush = true` so the
client advertises `SETTINGS_ENABLE_PUSH=1` at handshake. When `PushHandler` is
nil, push is disabled and any `PUSH_PROMISE` frame is a `PROTOCOL_ERROR` at the
conn layer.

```go
type PushHandler func(ctx context.Context, promisedHeaders []conn.HeaderField, resp *Response, err error)
```

The handler runs in a **dedicated goroutine**. `promisedHeaders` are the request
headers the server promised to fulfil (decoded from the `PUSH_PROMISE` frame and
deep-copied, so they are safe to retain past the call). `resp` is the
**fully drained** pushed response — its `Body` is always populated regardless of
`Request.WantBody`. `err` is non-nil if the push failed (RST_STREAM, connection
closed, etc.), in which case `resp` may be partially populated.

Under the hood the client uses `conn.Conn.LookupStream(id uint32) (*conn.Stream, bool)`
to resolve the promised even-numbered stream ID delivered on the parent stream's
`EventPushPromise`. You normally never call `LookupStream` yourself — the client
drives it for you and hands you the drained `*Response`.

```go
c, err := client.NewClient(client.ClientOptions{
	Addr: "example.com:443",
	ConnOpts: conn.ConnOptions{
		Dialer: &conn.TLSDialer{Config: &tls.Config{NextProtos: []string{"h2"}}},
		// EnablePush is set automatically because PushHandler is non-nil,
		// but you may also set it explicitly:
		// EnablePush: true,
	},
	PushHandler: func(ctx context.Context, promised []conn.HeaderField, resp *client.Response, err error) {
		if err != nil {
			fmt.Printf("push failed: %v\n", err)
			return
		}
		// promised carries the :path / :method the server pushed for.
		for _, hf := range promised {
			fmt.Printf("pushed %s = %s\n", hf.Name, hf.Value)
		}
		fmt.Printf("pushed status=%d body=%d bytes\n", resp.Status, len(resp.Body))
		// NOTE: do NOT call resp.Reset() here — the push goroutine owns this
		// Response and the client does not reuse it.
	},
})
if err != nil {
	// handle
}
defer c.Close()

req := &client.Request{Method: "GET", Path: "/index.html", WantBody: true}
var resp client.Response
resp.Reset()
if err := c.Do(context.Background(), req, &resp); err != nil {
	// handle
}
// Pushed sub-resources (e.g. /style.css) are delivered to PushHandler
// asynchronously while/after this Do returns.
```

### 4. Request priority (RFC 7540 §5.3)

`Request.Priority` is a `*frame.Priority`. When non-nil, the HEADERS frame
carries the PRIORITY flag and a 5-byte priority payload. The field type:

```go
type Priority struct { // package frame
	StreamDep uint32 // 0 = root stream (no parent)
	Exclusive bool   // make this stream the sole dependent of its parent
	Weight    uint8  // RFC weight = Weight + 1, i.e. wire 0 means weight 1
}
```

The server may use this to weight response delivery (e.g. CSS before images).
`StreamDep=0` means depend on the root. Use `Exclusive=true` to make this stream
the sole dependent of its parent.

```go
// High-priority CSS request: root-dependent, near-maximum weight.
req := &client.Request{
	Method: "GET",
	Path:   "/style.css",
	WantBody: true,
	Priority: &frame.Priority{
		StreamDep: 0,
		Exclusive: false,
		Weight:    200, // RFC weight 201
	},
}
var resp client.Response
resp.Reset()
_ = c.Do(context.Background(), req, &resp)
```

### 5. Extended CONNECT (RFC 8441) — WebSockets over HTTP/2

`Request.Protocol` is the `:protocol` pseudo-header for extended CONNECT. When
non-empty, `Method` **MUST** be `"CONNECT"` (validated up front:
sending `Protocol` with any other method returns `ErrInvalidRequest`). The
server must have advertised `SETTINGS_ENABLE_CONNECT_PROTOCOL=1` — that check is
the server's responsibility, not the client's.

The canonical use is tunneling WebSockets. Open the stream with `DoStream`
(so the tunnel stays open bidirectionally) and pump frames via `Recv`. Send
the WebSocket payload as the request body / DATA frames.

```go
// WebSocket-over-HTTP/2 (RFC 8441). The :scheme for WS is "https".
req := &client.Request{
	Method:    "CONNECT",
	Protocol:  "websocket", // sets the :protocol pseudo-header
	Scheme:    "https",
	Path:      "/chat",
	Authority: "example.com",
	Headers: []conn.HeaderField{
		{Name: []byte("sec-websocket-version"), Value: []byte("13")},
		{Name: []byte("sec-websocket-protocol"), Value: []byte("chat")},
	},
	// A streaming body keeps the CONNECT tunnel open; pr is the write side.
	// BodyReader: pr,
}

var sr client.StreamResponse
if err := c.DoStream(context.Background(), req, &sr); err != nil {
	// handle
}
defer sr.Close() // MUST Close if you do not drain to EndStream

if sr.Status != 200 { // RFC 8441: 2xx accepts the tunnel
	fmt.Printf("CONNECT rejected: %d\n", sr.Status)
	return
}

// Read tunneled frames off the stream.
for {
	ev, err := sr.Recv(context.Background())
	if errors.Is(err, client.ErrStreamEnded) {
		break
	}
	if err != nil {
		// handle
		break
	}
	switch ev.Type {
	case client.EventData:
		handleWSFrame(ev.Data) // ev.Data aliases a pooled buffer — copy to retain
	case client.EventReset:
		fmt.Printf("tunnel reset: %v\n", ev.ResetCode)
	}
}
```

### 6. Request trailers

There are two ways to attach request trailers, both `[]conn.HeaderField`:
`Request.Trailers` (a static slice) and `Request.TrailerFunc` (computed after the
body is flushed, e.g. checksums). Neither may contain pseudo-headers (names
beginning with `:`); that is validated and rejected with `ErrInvalidRequest`.

- The client announces the trailer field names in the initial HEADERS frame via
  the `trailer` request header (required by the Go `net/http2` server).
- `TrailerFunc` wins when non-nil and returns non-nil; otherwise `Trailers` is
  the fallback.
- **Contract for `TrailerFunc`:** it is called **twice per `Do`** (once to read
  trailer *keys* for the announcement before the body, once to send keys+values
  after the body) and may be called again on retry. It must be idempotent and
  return the same set of keys each time; values may differ between calls.
- Trailers over HTTP/1.1 are rejected with `ErrTrailersUnsupportedH1` (the H1
  fallback transport does not implement request trailers).

```go
// Static trailers.
req := &client.Request{
	Method:   "POST",
	Path:     "/upload",
	Body:     payload,
	Trailers: []conn.HeaderField{
		{Name: []byte("x-checksum"), Value: []byte("deadbeef")},
	},
}

// Computed trailers (e.g. a running hash finalized after the body is sent).
h := newRollingHash()
req2 := &client.Request{
	Method:     "POST",
	Path:       "/upload",
	BodyReader: teeReaderInto(h), // body streamed, hash updated as it flows
	TrailerFunc: func() []conn.HeaderField {
		// MUST return the same KEY set on every call; value may change.
		return []conn.HeaderField{
			{Name: []byte("x-content-sha256"), Value: []byte(h.HexSum())},
		}
	},
}

// To read response trailers, opt in with WantTrailers; they land in resp.Trailers.
respReq := &client.Request{Method: "GET", Path: "/data", WantBody: true, WantTrailers: true}
var resp client.Response
resp.Reset()
_ = c.Do(context.Background(), respReq, &resp)
for _, t := range resp.Trailers {
	fmt.Printf("trailer %s=%s\n", t.Name, t.Value)
}
```

### 7. Disabling response decompression

By default the client sends `accept-encoding: gzip` (unless your `Headers`
already include an `accept-encoding`) and transparently decodes
`content-encoding: gzip` and `deflate` responses. The supported encodings are
modeled by `client.ContentEncoding` (`EncodingIdentity`, `EncodingGzip`,
`EncodingDeflate`).

Set `Request.DisableDecompression = true` to turn this off entirely: no
`accept-encoding` is auto-added, and any compressed body is delivered verbatim.
Note `Response.BytesReceived` always reflects **wire** bytes; with decompression
enabled, `Response.Body` holds the **decompressed** bytes.

Decompressed size is capped by `ClientOptions.MaxDecompressedSize`
(default `DefaultMaxDecompressedSize` = 10 MiB) and raw size by
`MaxResponseBodySize` (default `DefaultMaxResponseBodySize` = 32 MiB); exceeding
either yields `ErrBodyTooLarge` (gzip-bomb protection).

```go
req := &client.Request{
	Method:               "GET",
	Path:                 "/already-compressed.gz",
	WantBody:             true,
	DisableDecompression: true, // deliver raw bytes; no accept-encoding sent
}
var resp client.Response
resp.Reset()
if err := c.Do(context.Background(), req, &resp); err != nil {
	if errors.Is(err, client.ErrBodyTooLarge) {
		// raw body exceeded MaxResponseBodySize
	}
}
// resp.Body holds raw (still-compressed) bytes; resp.BytesReceived == len(resp.Body).
```

### 8. Error model

The client exposes sentinel errors (compare with `errors.Is`) and two typed
error structs (inspect with `errors.As`).

Sentinels (all in `errors.go`):

| Sentinel | Returned when |
|---|---|
| `ErrInvalidRequest` | `Request` failed up-front validation (wraps a detail) |
| `ErrClosed` | `Do`/`DoStream`/acquire after `Client.Close` |
| `ErrRedialBackoff` | single-conn dial within `DialBackoff` window |
| `ErrEmptyResponse` | response HEADERS had no `:status` pseudo-header |
| `ErrInvalidStatus` | `:status` present but not a valid integer |
| `ErrPoolClosed` | pool op after `Pool.Close` |
| `ErrAcquireTimeout` | `PoolOptions.AcquireTimeout` elapsed |
| `ErrDialBackoff` | pool dial within its `DialBackoff` window |
| `ErrInvalidPoolOptions` | `Transport`/`Pool` inconsistent (from `NewClient`) |
| `ErrInvalidTransportKind` | `Transport` is not a defined `TransportKind` |
| `ErrWatchUnsupported` | a `Resolver.Watch` does not support push updates |
| `ErrNoAddresses` | resolver yielded zero addresses (no cache) / empty selector set |
| `ErrInvalidOptions` | `ClientOptions` internally inconsistent (e.g. both `Addr` and `Resolver`) |
| `ErrBodyTooLarge` | response body (raw or decompressed) exceeded its limit |
| `ErrNilKeyFn` | `Hash` selector built with a nil keyFn |
| `ErrTrailersUnsupportedH1` | request carried trailers over HTTP/1.1 |
| `ErrStreamEnded` | (in `response.go`) `StreamResponse.Recv` called past the final event |

Typed errors:

```go
// Peer sent RST_STREAM mid-response. Returned from Do, or surfaced via
// DoStream's EventReset (where the code is in StreamEvent.ResetCode).
type StreamResetError struct {
	Code conn.ErrCode // e.g. frame.ErrCodeCancel, frame.ErrCodeRefusedStream
}
func (e *StreamResetError) Error() string
func (e *StreamResetError) Unwrap() error // returns nil

// Lazy dial failed. Returned from Do/DoStream.
type DialError struct {
	Addr string
	Err  error
}
func (e *DialError) Error() string
func (e *DialError) Unwrap() error // returns e.Err
```

`StreamResetError.Unwrap()` deliberately returns nil (it carries a code, not a
wrapped cause) — it exists for structural symmetry so you can uniformly call
`errors.Is`/`errors.As` on client errors. `DialError.Unwrap()` exposes the
underlying dial cause, so `errors.Is(err, someNetError)` works through it.

```go
var resp client.Response
resp.Reset()
err := c.Do(context.Background(), req, &resp)

switch {
case err == nil:
	// success
case errors.Is(err, client.ErrInvalidRequest):
	// bad Request fields — not retryable
case errors.Is(err, client.ErrClosed):
	// client was closed
case errors.Is(err, client.ErrBodyTooLarge):
	// response exceeded size limits
default:
	var rst *client.StreamResetError
	var dialErr *client.DialError
	switch {
	case errors.As(err, &rst):
		if rst.Code == frame.ErrCodeRefusedStream {
			// peer refused the stream — typically safe to retry
		}
	case errors.As(err, &dialErr):
		fmt.Printf("dial to %s failed: %v\n", dialErr.Addr, dialErr.Err)
		// errors.Is(err, <netErr>) also works via DialError.Unwrap
	}
}
```

When streaming, a mid-response reset surfaces as an event rather than a `Recv`
error:

```go
ev, err := sr.Recv(ctx)
if err == nil && ev.Type == client.EventReset {
	fmt.Printf("peer reset stream: %v\n", ev.ResetCode) // conn.ErrCode
}
```

## Required-call contracts

These are the cleanup obligations that, if skipped, leak connections, reader
goroutines, pool actor goroutines, or pooled buffers. All cleanup methods are
idempotent.

- **`Client.Close()` / `Client.Shutdown(gracefulTimeout)`** — you MUST call one
  on every `Client` you construct. `Close` releases the transport immediately;
  `Shutdown` drains in-flight work first but only honors `gracefulTimeout` for
  `TransportSingleConn` (sends GOAWAY, waits for in-flight). For
  `TransportPool` / `TransportManaged` / `TransportH1SingleConn` the timeout is
  ignored and `Shutdown` behaves exactly like `Close`. After either, `Do` /
  `DoStream` return `ErrClosed`.

- **`Response.Reset()`** — call before **every** `Do`, including after an error
  (on error `Response` fields are undefined). `Reset()` returns the pooled slab
  buffers backing `Headers`/`Body`/`Trailers`; those bytes are valid only until
  the next `Reset()`, so copy anything you retain.

- **`Response.BodyReader.Close()`** (the `StreamBody` path) — you MUST call it
  (or `Response.Reset()`, which calls it for you when `BodyReader != nil`)
  before the next `Do` on that `Response`. It returns the connection to the
  pool and sends `RST_STREAM(CANCEL)` if the body was not fully drained.

- **`StreamResponse.Close()`** (the `DoStream` path) — you MUST call it whenever
  you do not drain the stream to `EndStream`. It returns pooled header slabs and
  the current data buffer and sends `RST_STREAM(CANCEL)` when neither side
  reached END_STREAM. `defer sr.Close()` is the safe idiom; it is harmless after
  a full drain. (The `Client.Stream` convenience helper does this for you.)

> **Catching a missing Close in dev.** Build/test with `-tags poseidondebug`
> (or run `make test-debug`) to compile in a finalizer-based leak detector: a
> `StreamResponse` or `Response.BodyReader` garbage-collected without `Close()`
> logs a loud, attributable warning. It is compiled out — zero cost — in normal
> builds, so it is a development/CI aid, not a production mechanism.

## Errors

All sentinels are compared with `errors.Is`; the two typed errors are inspected
with `errors.As`.

Sentinels in `client/errors.go`:

| Sentinel | Returned when |
|---|---|
| `ErrInvalidRequest` | `Request` failed up-front validation (wraps a detail) |
| `ErrClosed` | `Do`/`DoStream`/acquire after `Client.Close` |
| `ErrRedialBackoff` | single-conn dial within the `DialBackoff` window |
| `ErrEmptyResponse` | response HEADERS had no `:status` pseudo-header |
| `ErrInvalidStatus` | `:status` present but not a valid integer |
| `ErrPoolClosed` | pool op after `Pool.Close` |
| `ErrAcquireTimeout` | `PoolOptions.AcquireTimeout` elapsed |
| `ErrDialBackoff` | pool dial within its `DialBackoff` window |
| `ErrInvalidPoolOptions` | `Transport`/`Pool` inconsistent (from `NewClient`) |
| `ErrInvalidTransportKind` | `Transport` is not a defined `TransportKind` |
| `ErrWatchUnsupported` | a `Resolver.Watch` does not support push updates |
| `ErrNoAddresses` | resolver yielded zero addresses (no cache) / empty selector set |
| `ErrInvalidOptions` | `ClientOptions` internally inconsistent (managed nil `Resolver` or non-empty `Addr`) |
| `ErrBodyTooLarge` | response body (raw or decompressed) exceeded its limit |
| `ErrNilKeyFn` | `Hash` selector built with a nil keyFn |
| `ErrTrailersUnsupportedH1` | request carried trailers over HTTP/1.1 |

One additional sentinel, `ErrStreamEnded`, lives in `client/response.go` and is
returned from `StreamResponse.Recv` once the final `EndStream` event has been
delivered.

Typed errors (inspect with `errors.As`):

```go
// Peer sent RST_STREAM mid-response. Returned from Do, or surfaced via
// DoStream's EventReset (code in StreamEvent.ResetCode).
type StreamResetError struct {
	Code conn.ErrCode
}
func (e *StreamResetError) Error() string
func (e *StreamResetError) Unwrap() error // returns nil

// Lazy dial failed. Returned from Do/DoStream.
type DialError struct {
	Addr string
	Err  error
}
func (e *DialError) Error() string
func (e *DialError) Unwrap() error // returns e.Err — errors.Is reaches the net error
```

`StreamResetError.Unwrap()` returns nil (it carries a code, not a wrapped
cause); it exists only for structural symmetry so `errors.Is`/`errors.As` work
uniformly across client errors. `DialError.Unwrap()` exposes the underlying dial
cause, so `errors.Is(err, someNetError)` matches through it. See
[the error-model walkthrough](#8-error-model) above for the full
`errors.Is`/`errors.As` dispatch example.

## Convenience helpers

`client/sugar.go` is an **opt-in** ergonomic layer for one-off requests,
scripts, and tests where clarity matters more than the last allocation. None of
it sits on the zero-allocation hot path — the helpers allocate only when you
call them. The expert path is unchanged: build a `Request` literal and reuse a
caller-owned `Response`.

- `type HeaderField = conn.HeaderField` — re-exported alias so you need not
  import `conn` for the most common type.
- `func H(name, value string) HeaderField` — builds a regular header; the name
  is lower-cased (RFC 7540 §8.1.2 requires lowercase field names).
- `func NewRequest(method, path string) *Request` — returns a `*Request` with
  `WantBody` enabled. `func GET(path string) *Request` and
  `func POST(path string, body []byte) *Request` are shorthands (`POST`
  references `body`, does not copy it).
- `func (r *Request) WithHeaders(h ...HeaderField) *Request` — sets `r.Headers`
  and returns `r` for chaining.
- `func (r *Response) Header(name string) ([]byte, bool)` — first matching
  response header value (case-insensitive); aliases `Response`-owned memory,
  valid until the next `Reset`. Allocation-free.
- `func (r *Response) HeaderString(name string) (string, bool)` — same, but
  returns a freshly-allocated string copy safe to retain.
- `func (r *Response) CopyBody() []byte` — heap copy of the body, safe past the
  next `Reset` (nil when empty).
- `func (r *Response) Clone() *Response` — detached deep copy of `Status`,
  `Headers`, `Body`, `Trailers`, `BytesReceived` (the streaming `BodyReader` and
  pooled slabs are not carried over).
- `func (e StreamEvent) DataCopy() []byte` — heap copy of the event's DATA
  payload, safe past the next `Recv`/`Close` (nil for non-data / empty events).
- `func (c *Client) Stream(ctx context.Context, req *Request, fn func(StreamEvent) error) error`
  — issues a streaming request and invokes `fn` for each event until the stream
  ends, `fn` errors, or `ctx` is cancelled. It **always** closes the underlying
  `StreamResponse`, so you cannot leak the pooled connection slot by forgetting
  `Close` — the most common `DoStream` footgun. `fn` must not retain
  `StreamEvent.Data` past its return; use `StreamEvent.DataCopy()` to keep bytes.

```go
resp := &client.Response{}
_ = c.Do(ctx, client.GET("/v1/things").WithHeaders(
	client.H("Accept", "application/json"),
), resp)
if ct, ok := resp.HeaderString("content-type"); ok {
	_ = ct
}

err := c.Stream(ctx, client.GET("/events"), func(ev client.StreamEvent) error {
	if ev.Type == client.EventData {
		process(ev.DataCopy()) // copy to retain past this callback
	}
	return nil
})
```
