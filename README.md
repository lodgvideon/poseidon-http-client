# poseidon-http-client

A low-level, zero-allocation HTTP/2 client codec for Go, designed for load
generators. Implements RFC 7540 (HTTP/2) and RFC 7541 (HPACK) from scratch
without `net/http` or `golang.org/x/net/http2`.

**Status:** Phase A — frame codec + HPACK. See [design](docs/superpowers/specs/2026-05-02-poseidon-frame-layer-design.md).

## Phases

- **A — Frame layer + HPACK** *(this release)*: codec only, no networking.
- **B — Connection layer** *(planned)*: TLS, ALPN, stream state machine, flow control.
- **C — Client + pool + discovery + stats** *(planned)*: public API for load generators.

## Quick start

```go
import (
    "bytes"
    "context"

    "github.com/lodgvideon/poseidon-http-client/frame"
    "github.com/lodgvideon/poseidon-http-client/hpack"
)

// Encode a HEADERS+DATA pair into a buffer.
func example() []byte {
    var buf bytes.Buffer
    fr := frame.NewFramer(&buf, nil)

    enc := hpack.NewEncoder()
    block := enc.EncodeBlock(nil, []hpack.HeaderField{
        {Name: []byte(":method"), Value: []byte("GET")},
        {Name: []byte(":scheme"), Value: []byte("https")},
        {Name: []byte(":path"), Value: []byte("/")},
        {Name: []byte(":authority"), Value: []byte("example.com")},
    })
    _ = fr.WriteHeaders(frame.WriteHeadersParams{
        StreamID: 1, BlockFragment: block,
        EndHeaders: true, EndStream: true,
    })

    // Decode side
    dec := hpack.NewDecoder()
    fr2 := frame.NewFramer(nil, &buf)
    var h myHandler
    _, _ = fr2.ReadFrame(context.Background(), &h)
    _ = dec
    return buf.Bytes()
}
```

## Limits and contracts

- `frame.Framer` is **NOT goroutine-safe** — instantiate one per HTTP/2 connection.
- `hpack.Encoder` / `hpack.Decoder` are **NOT goroutine-safe** — same scope.
- Decoder `HeaderField.Name`/`Value` slices alias the decoder's scratch arena and
  are valid only during the visitor call. Copy if you must retain.
- Hot path: `0 B/op` and `0 allocs/op` (bench gate enforced in CI).

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

See [docs/BENCH_BASELINE.md](docs/BENCH_BASELINE.md) for reference numbers.

## License

MIT — see [LICENSE](LICENSE).
