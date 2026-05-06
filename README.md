# poseidon-http-client

A low-level, zero-allocation HTTP/2 client for Go, designed for load
generators. Implements RFC 7540 (HTTP/2) and RFC 7541 (HPACK) from
scratch without `net/http` or `golang.org/x/net/http2`.

**Status:** Phase B.2.5 — TLS+ALPN connection, multi-stream
(configurable `MaxConcurrentStreams`, default 100, gated on
`min(local, peer-advertised)`), bidirectional flow control with
chunked DATA writes and batched WINDOW_UPDATE refunds, dynamic
SETTINGS apply + ACK with retroactive `INITIAL_WINDOW_SIZE` resize.
See
[design](docs/superpowers/specs/2026-05-05-poseidon-conn-layer-b1-design.md).

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
- **B.2.5 — Peer-advertised `MAX_CONCURRENT_STREAMS`** *(this
  release)*: `NewStream` gates inflight on
  `min(local advertised, peer-advertised)`; dynamic shrinks refuse
  new streams without disturbing open ones (RFC §6.5.2).
- **B.2.6 — GOAWAY drain + PING ACK** *(planned)*.
- **C — Client + pool + discovery + stats** *(planned)*: public API for
  load generators.

## Quick start

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func get(ctx context.Context, addr string) error {
	c, err := conn.Dial(ctx, addr, conn.ConnOptions{
		Dialer: &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		return err
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		return err
	}
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			return err
		}
		if ev.Type == conn.EventHeaders {
			for _, f := range ev.Headers {
				if string(f.Name) == ":status" {
					fmt.Println("status:", string(f.Value))
				}
			}
		}
		if ev.EndStream {
			return nil
		}
	}
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
- Codec hot path: `0 B/op` and `0 allocs/op` (Phase A bench gate
  enforced in CI). `conn` steady state currently allocates per
  request — zero-alloc work is deferred to B.2.

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
