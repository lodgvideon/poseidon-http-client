# poseidon-http-client

A low-level, zero-allocation HTTP/2 client for Go, designed for load
generators. Implements RFC 7540 (HTTP/2) and RFC 7541 (HPACK) from
scratch without `net/http` or `golang.org/x/net/http2`.

**Status:** Phase B.1 — TLS+ALPN connection, single in-flight stream.
See [design](docs/superpowers/specs/2026-05-05-poseidon-conn-layer-b1-design.md).

## Phases

- **A — Frame layer + HPACK** *(released)*: codec only, no networking.
- **B.1 — Connection layer (single stream)** *(this release)*: TLS+ALPN
  dial, SETTINGS handshake, one in-flight stream end-to-end against
  net/http2.Server.
- **B.2 — Multiplex + flow control** *(planned)*: full RFC 7540 §5.1
  state machine, per-stream and per-conn flow control,
  GOAWAY/keep-alive.
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
- B.1 enforces **one in-flight stream per Conn** (returns
  `ErrTooManyStreams` if exceeded). Sequential streams on the same
  Conn are allowed.
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
