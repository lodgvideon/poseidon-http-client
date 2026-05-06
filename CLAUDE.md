# poseidon-http-client — Claude context

Low-level HTTP/2 client in Go. Implements RFC 7540 + RFC 7541 from
scratch (no `net/http`, no `golang.org/x/net/http2`). Target users:
load generators that need zero-alloc codec + fine-grained control over
streams, flow control, and pooling.

## Quick commands

```bash
make tidy        # go mod tidy
make lint        # golangci-lint v1.64
make test-race   # go test -race ./... (default verification)
make bench       # benches with bench-gate (0 B/op, 0 allocs/op enforced on frame + hpack)
```

Single-package iteration:

```bash
go test ./conn/ -count=1 -race -timeout 90s
go test ./conn/ -run TestIntegration_TenConcurrentStreams_Echo -v
```

Pre-commit hook (optional): `git config core.hooksPath .githooks`.

## Architecture

```
conn/                # B-layer: connection, streams, flow control, handshake
  └── depends on: frame, hpack
frame/               # A-layer: HTTP/2 frame codec (parser + writer + Framer)
hpack/               # A-layer: RFC 7541 HPACK encoder/decoder
internal/bytesx/     # Big-endian helpers (Uint24, Uint31)
docs/                # RFC_COVERAGE.md (authoritative test-to-RFC map), BENCH_BASELINE.md, COVERAGE.md
```

Public packages: `frame`, `hpack`, `conn`. `cmd/` does not exist —
this is a library. `Conn` owns one `*frame.Framer` + one
`*hpack.Encoder` + one `*hpack.Decoder` per connection and serializes
writes via `wmu`.

## Phase status

Read [README.md §Phases](README.md) and [conn/doc.go](conn/doc.go) for
the current milestone. As of this commit: **B.2.2 done** (multi-stream
+ recv flow control). Roadmap: B.2.3 outbound flow control →
B.2.4 dynamic SETTINGS+ACK → B.2.5 peer MAX_CONCURRENT_STREAMS →
B.2.6 GOAWAY drain + PING ACK → C client/pool/discovery.

## Code-style gates (golangci-lint v1.64, see `.golangci.yml`)

- `revive` requires doc comments on every exported type, method,
  function, constant.
- `gosec` G115 + G402 are tuned (intentional int conversions; TLS
  config is opt-in by caller).
- `govet`: `fieldalignment` and `shadow` are disabled (excessive churn
  for this codebase).
- `unconvert` is on — strip redundant `uint32(x)` etc.

## RFC trace policy (mandatory)

Every new conformance test MUST add a row to
[docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md) keyed on the RFC section.
The `conformance-gate` CI job greps for `TestConformance_RFC7540_*`
and `TestConformance_RFC7541_*` and fails on regressions. Integration
and negative tests also belong in the matrix when they pin a specific
section's behavior.

## Gotchas

- `frame.NewFramer(w io.Writer, r io.Reader)` — **writer first**, then
  reader. Easy to get backwards.
- `Stream.id == 0` until the first `SendHeaders` writes the HEADERS
  frame under `wmu` (B.2.1 deferred allocation; preserves §5.1.1
  monotonic-id ordering across concurrent `NewStream` callers). Don't
  read `Stream.ID()` before SendHeaders.
- `Stream.events` channel buffer = `opts.StreamEventBuffer` (default
  8). If full, `push` drops + sends RST(REFUSED_STREAM). Caller must
  drain `Recv` promptly or set a larger buffer.
- `recvWindowRefundThreshold = 32 KiB` — WINDOW_UPDATEs batch at this
  granularity (B.2.2). Setting it lower means more control-frame
  chatter; higher means tolerating more in-flight data without
  refund.
- `connRecvWindow = 65535` is RFC-mandated at handshake; only
  per-stream window is governed by `SETTINGS_INITIAL_WINDOW_SIZE`.
- `net.Pipe` in unit tests is **unbuffered + synchronous**. Tests that
  write more than one frame in a row from the peer goroutine while
  the client is also writing will deadlock. Use `httptest`+h2 (real
  TCP buffers) for anything beyond a single round trip.

## Testing patterns

- Integration suite: `conn/integration_test.go` + `conn/multistream_test.go`
  + `conn/flowcontrol_test.go` use `httptest.NewUnstartedServer` with
  `EnableHTTP2 = true` against a real `net/http2.Server` peer.
- Unit suite: `pipeServer` helper in `conn/conn_test.go` drives a
  `net.Pipe` peer for handshake-level checks. Symmetric read/write —
  every server-side write needs a goroutine, every client-side write
  needs a server reader running concurrently.
- Naming: `TestConformance_RFC7540_SecXX_Behavior` (gate-tracked),
  `TestIntegration_*`, `TestConn_*`, `TestStream_*`, `TestFramer_*`,
  `TestHandler_*`.

## Tooling notes

- The `tdd-guard@latest` npm hook (PreToolUse on Edit/Write) currently
  errors out against the `z.ai` Anthropic-compatible endpoint
  (`Failed to ... is not valid JSON`). When that happens, edits via
  the `Edit` / `Write` tools are blocked. **Fallback:** use the
  `mcp__serena__*` tools — `replace_symbol_body`, `insert_after_symbol`,
  `create_text_file`. They go through a different matcher and succeed.
- `commit-commands` plugin enforces a 50-char subject line and rejects
  AI co-author trailers — keep commit subjects short and don't add
  `Co-Authored-By` lines.
