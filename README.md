# poseidon-http-client

A low-level, zero-allocation HTTP/2 client for Go, designed for load
generators. Implements RFC 7540 (HTTP/2) and RFC 7541 (HPACK) from
scratch without `net/http` or `golang.org/x/net/http2`.

**Status:** v0.3.0 — client hardening (graceful shutdown, body
decompression, priority hints, request timeout, client rate
limiting, connection warmup, RateLimitBurst option, warmup
lifecycle fix). See [CHANGELOG.md](CHANGELOG.md) for details. See
[C.1 design](docs/superpowers/specs/2026-05-07-poseidon-client-c1-design.md),
[C.2 design](docs/superpowers/specs/2026-05-08-poseidon-client-c2-pool-design.md),
[C.3/C.4 design](docs/superpowers/specs/2026-05-09-c3-c4-design.md),
[D.1 design](docs/superpowers/specs/2026-05-13-d1-zero-alloc-request-path-design.md),
[D.2 design](docs/superpowers/specs/2026-05-15-d2-request-response-body-streaming-design.md),
[D.3 design](docs/superpowers/specs/2026-05-15-d3-h2c-design.md),
[D.4 design](docs/superpowers/specs/2026-05-15-d4-ping-keepalive-design.md),
[D.5 design](docs/superpowers/specs/2026-05-15-d5-trailers-design.md).

## Phases

- **A — Frame layer + HPACK** *(released)*: codec only, no networking.
- **B.1 — Connection layer (single stream)** *(released)*: TLS+ALPN dial,
  SETTINGS handshake, one in-flight stream end-to-end against
  net/http2.Server.
- **B.2.1 — Multi-stream foundation** *(released)*: configurable
  `MaxConcurrentStreams`, deferred stream-id allocation under the writer
  mutex (RFC 7540 §5.1.1 monotonic ordering).
- **B.2.2 — Flow control IN** *(released)*: per-stream + connection
  recv windows, batched WINDOW_UPDATE refunds at 32 KiB threshold,
  typed `FLOW_CONTROL_ERROR` on peer overrun (RFC 7540 §6.9.1).
- **B.2.3 — Flow control OUT** *(released)*: chunked DATA writes
  at `min(peer MAX_FRAME_SIZE, our advertised MAX_FRAME_SIZE)`,
  blocking `acquireSendCredits` until per-stream + connection
  send-window credit, `OnWindowUpdate` replenishment, 2^31-1
  overflow as typed `StreamError` / `ConnError`.
- **B.2.4 — Dynamic SETTINGS** *(released)*:
  `connHandler.OnSettings` merges non-ACK frames into
  `c.peerSettings`, applies side effects (HPACK encoder resize,
  retroactive `INITIAL_WINDOW_SIZE` delta on every open stream — RFC
  §6.9.2), and emits a SETTINGS ACK (RFC §6.5.3).
- **B.2.5 — Peer-advertised `MAX_CONCURRENT_STREAMS`** *(released)*:
  `NewStream` gates inflight on
  `min(local advertised, peer-advertised)`; dynamic shrinks refuse
  new streams without disturbing open ones (RFC §6.5.2).
- **B.2.6 — GOAWAY drain + PING ACK** *(released)*: peer GOAWAY records
  state on `*Conn`, drains streams above `lastStreamID` with
  `EventReset(REFUSED_STREAM)`, blocks new `NewStream` with `ErrGoAway`,
  wakes writers stuck on send credit (RFC §6.8); inbound non-ACK PING
  echoes back with `ACK=1` and the same 8-byte payload (RFC §6.7).
- **C.1 — Public client API** *(released)*: `client.Client`, `Request`,
  `Response`, sync `Do` and streaming `DoStream`, single-connection
  transport with conn-level auto-redial, `(*conn.Conn).IsAlive` helper
  for transport reuse decisions.
- **C.2 — connection pool** *(released)*: per-host `Pool` with lazy-grow,
  least-loaded stream selection, idle-timeout eviction, GOAWAY-aware
  drain, dial backoff, and `MAX_CONCURRENT_STREAMS` enforcement
  (RFC 7540 §5.1.2, §6.8). Enable via
  `ClientOptions{Transport: client.TransportPool}`.
- **C.3 — service discovery** *(released)*: per-address sub-pool fan-out
  via `Resolver` + `Selector`. Built-in `StaticResolver`, `DNSResolver`
  with TTL cache + Watch, and `RoundRobin` / `Random` / `Hash` selectors.
  DrainGraceful / DrainHard / DrainLazy modes.
- **C.4 — metrics & hooks** *(released)*: lifecycle `Hooks`
  (`OnRequestStart`, `OnRequestComplete`, `OnRetry`, `OnDial`,
  `OnConnClose`, `OnResolverUpdate`); lock-free counters and log-bucket
  latency histograms exposed via `(*Client).Metrics()` and
  `(*Client).MetricsSnapshot()`. Zero hot-path overhead when
  `ClientOptions.Hooks == nil`.
- **D.1 — zero-alloc request path** *(released)*: caller-provided
  `*Response`/`*StreamResponse` eliminates per-call heap alloc;
  `conn.HeaderSlabPool` slab allocator for HPACK header bytes;
  per-`Conn` stream `sync.Pool`; `encBufPool` for HPACK encode buffers;
  `hdrSlicePool` + const name bytes in `buildHeaders`. 33 allocs/op,
  down from 49 (−33%).
- **D.2 — request/response body streaming** *(released)*:
  `Request.ContentLength int64` emits `content-length` header;
  `Request.StreamBody bool` + `Response.BodyReader io.ReadCloser` stream
  response bodies without buffering (caller drains + `Close()`);
  `uploadBufPool` recycles upload read buffers; `Response.Reset()` closes
  `BodyReader` automatically. RFC 7540 §8.1 conformance test added.
- **D.3 — H2C (plaintext HTTP/2)** *(released)*: `conn.PlaintextDialer`
  for unencrypted TCP; H2C prior-knowledge handshake (RFC 7540 §3.4).
  Set `ClientOptions.DefaultScheme = "http"` for H2C targets.
- **D.4 — PING / keepalive** *(released)*: `(*conn.Conn).Ping(ctx)`
  measures round-trip time; background keepalive loop with
  `ConnOptions.KeepaliveInterval` + `KeepaliveTimeout` closes dead
  connections automatically (RFC 7540 §6.7).
- **D.5 — HTTP request trailers** *(released)*: `Request.Trailers
  []hpack.HeaderField` and `Request.TrailerFunc` send trailer
  HEADERS+END_STREAM after body DATA frames (RFC 7540 §8.1.3);
  `StreamResponse.WaitTrailers(ctx)` pumps events until trailers arrive
  or stream ends; `trailerAnnouncement` emits `Trailer:` header for
  Go net/http server compatibility.

## Quick start

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func get(ctx context.Context, addr string) error {
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}},
		},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &res); err != nil {
		return err
	}
	fmt.Println("status:", res.Status, "bytes:", res.BytesReceived)
	return nil
}
```

### Pool transport

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func poolGet(ctx context.Context, addr string) error {
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   4,
			MaxStreamsPerConn: 100,
			IdleTimeout:       30 * time.Second,
		},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &res); err != nil {
		return err
	}
	fmt.Println("status:", res.Status, "bytes:", res.BytesReceived)
	return nil
}
```

For codec-only usage (no networking), the `frame` and `hpack`
packages remain importable directly:

```go
import (
    "github.com/lodgvideon/poseidon-http-client/frame"
    "github.com/lodgvideon/poseidon-http-client/hpack"
)
```

## Limits and contracts

- `conn.Conn` is goroutine-safe across `NewStream` / `Close`. `Stream`
  methods may be called from one goroutine at a time.
- B.2.1 enforces a **locally advertised `MaxConcurrentStreams`** cap
  per Conn (default 100; returns `ErrTooManyStreams` if exceeded).
  Peer-advertised limit enforcement is B.2.5.
- `frame.Framer`, `hpack.Encoder`, `hpack.Decoder` are NOT
  goroutine-safe — `conn.Conn` owns one of each per connection and
  serializes access internally.
- Decoder / `StreamEvent` slice fields alias internal scratch buffers
  and are valid only until the next `Recv`/`Close` on the same stream.
  Copy if you must retain.
- Codec hot path: `0 B/op` and `0 allocs/op` (bench gate enforced in
  CI). `client` steady state: D.1 reduced to 33 allocs/op (−33%).

## Local development

Requirements: Go 1.24, `golangci-lint` v1.62 (optional).

```bash
make tidy
make lint
make test-race
make bench
```

Optional pre-commit hook:

```bash
git config core.hooksPath .githooks
```

See [docs/BENCH_BASELINE.md](docs/BENCH_BASELINE.md) for reference
numbers and [docs/COVERAGE.md](docs/COVERAGE.md) for coverage policy.

## License

MIT — see [LICENSE](LICENSE).
