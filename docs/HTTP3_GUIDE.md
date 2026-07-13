# HTTP/3 Usage & Protocol Selection

This guide covers the HTTP/3 client (`http3` package) and how to choose
between it and the HTTP/1.1 + HTTP/2 client (`client` package). For the
HTTP/2 client's full surface, see [CLIENT_GUIDE.md](CLIENT_GUIDE.md); for
the HTTP/3 internals, see [HTTP3_DESIGN.md](HTTP3_DESIGN.md).

> **Two clients, one philosophy.** poseidon ships two independent clients
> that share a from-scratch, zero-dependency, wire-level-control design but
> **do not share a `Do()` surface today**. The HTTP/1.1 + HTTP/2 client
> (`client.Client`) is the mature, load-generator-grade path. The HTTP/3
> client (`http3.Client`) is protocol-complete but has a narrower feature
> set (see [Maturity](#maturity--limitations)). Pick per the
> [decision table](#choosing-a-protocol).

## HTTP/3 quick start

`http3.Dial` establishes a QUIC connection and returns a ready client. It
sets the `h3` ALPN token and a TLS 1.3 minimum for you; you must set
`ServerName` on the `tls.Config`.

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http3"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Dial "host:port" over UDP. tlsConfig.ServerName is required.
	c, err := http3.Dial(ctx, "example.com:443", &tls.Config{
		ServerName: "example.com",
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	req := &http3.Request{
		Method:    "GET",
		Scheme:    "https",
		Authority: "example.com",
		Path:      "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("user-agent"), Value: []byte("poseidon-h3")},
		},
	}

	// Do returns the parsed response head, the full response body, and an error.
	resp, body, err := c.Do(ctx, req)
	if err != nil {
		panic(err)
	}
	fmt.Printf("status=%d bytes=%d\n", resp.Status, len(body))
}
```

### Types

- **`http3.Request`** — `Method`, `Scheme`, `Authority` (omitted from the
  field section when empty), `Path`, `Headers []hpack.HeaderField`, and an
  optional `Body []byte` sent in a DATA frame after HEADERS. CONNECT and
  asterisk-form (`OPTIONS *`) requests are not yet supported.
- **`http3.Response`** — `Status int`, `Headers`, `Interim []*Response`
  (any 1xx informational responses that preceded the final one, in receive
  order), and `Trailers` (the trailing field section after the body).
- **`Client.Do(ctx, *Request) (*Response, []byte, error)`** — the body is
  returned fully buffered (there is no streaming-body H3 API yet), bounded
  by an internal response-size cap that returns `ErrResponseTooLarge`
  against an oversized or hostile server.

### Retry classification

A server may reject a request stream with `RESET_STREAM` / `STOP_SENDING`.
`Do` surfaces this as a `*http3.StreamResetError`; call `Retryable()` to
tell a safely-retryable rejection (the request was not processed —
`H3_REQUEST_REJECTED`) from one that may have had side effects:

```go
resp, body, err := c.Do(ctx, req)
var rst *http3.StreamResetError
if errors.As(err, &rst) && rst.Retryable() {
	// Safe to retry on a fresh stream/connection — the server did not act on it.
}
```

Unlike the HTTP/2 client, the HTTP/3 client has **no built-in retry loop**;
you drive retries yourself using this classification.

## Choosing a protocol

| Use… | When |
|------|------|
| **`client.Client`** (HTTP/1.1 or HTTP/2) | Load generation, benchmarking, or any workload needing connection pooling, automatic retry/backoff, service discovery, rate limiting, per-request metrics, or request-body streaming. This is the production path. |
| **HTTP/2** specifically (`client` with an H2 transport) | Multiplexed streams over one TCP+TLS connection; the default for high-throughput HTTPS load. |
| **HTTP/1.1** (`client` with `TransportH1SingleConn`/ALPN) | Talking to servers without H2, or when you need strict one-request-per-connection semantics. |
| **`http3.Client`** (HTTP/3 over QUIC) | Exercising an HTTP/3 / QUIC server, conformance testing, or measuring a single-stream QUIC exchange. **Not** the path for high-concurrency load generation yet — see below. |

Protocol selection in `client.Client` is **static**, chosen once at
construction via `TransportKind` (or negotiated once via ALPN with
`TransportALPN`, then pinned). It is not per-request. HTTP/3 is **not** one
of the `TransportKind` values — the two clients are reached through
different entry points (`client.NewClient…` vs `http3.Dial`).

## Maturity & limitations

The HTTP/3 stack is protocol-complete and conformance-tested against three
independent servers (Caddy, nginx, aioquic — see
[HTTP3_INTEROP_TEST_PLAN.md](HTTP3_INTEROP_TEST_PLAN.md)), but the *client
ergonomics* deliberately trail the HTTP/2 client:

- **One request in flight per connection.** `http3.Client.Do` serializes
  requests on the connection; there is no request-level stream
  multiplexing or connection pool. Concurrent load needs many clients.
- **No pooling, retry loop, resolver, selector, rate limiter, or metrics.**
  These live only in `client.Client`. HTTP/3 has no `TransportH3` under
  `client.Do`, so none of that machinery applies to it.
- **Buffered response bodies only** — no streaming-body read API.
- **Static-table-only QPACK** (advertises
  `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0`): a server that references the
  dynamic table is rejected cleanly as `QPACK_DECOMPRESSION_FAILED`.
- **AES-GCM header protection only** — ChaCha20 header protection is not
  implemented; a non-AES-GCM cipher suite is refused (fail-closed).

**Why HTTP/3 is not the load-generation path yet.** Load generation is the
project's core use case, and it needs concurrent in-flight requests +
pooling — exactly what `http3.Client` lacks. Wiring today's serial,
one-per-connection `http3.Client` under `client.Do()` would ship a degraded
half-feature and break `client.Do`'s goroutine-safety and pooling
contracts. The planned order of work (see the Phase status in
[CLAUDE.md](../CLAUDE.md)) is: concurrent multiplexing → connection pooling
→ retry/metrics/resolver parity → a `TransportH3` unified under
`client.Do` → zero-alloc / bench-gate performance parity with the HTTP/2
hot path. Until then, use `client.Client` for load and `http3.Client` for
HTTP/3 protocol work.
