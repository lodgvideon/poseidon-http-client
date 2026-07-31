# poseidon-http-client — Claude context

Low-level HTTP/1.1 + HTTP/2 + HTTP/3 client in Go. Implements RFC 7540
+ 7541 (HTTP/2/HPACK) and RFC 9000/9001/9002/9114/9204 (QUIC/QUIC-TLS/
loss-recovery/HTTP-3/QPACK) from scratch — no `net/http`, no
`golang.org/x/net/http2`, no `quic-go`. Target users: load generators
needing zero-alloc codecs + fine-grained control over streams, flow
control, pooling.

## Quick commands

```bash
make tidy        # go mod tidy
make lint        # golangci-lint v2.5
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
HTTP/1.1 + HTTP/2 stack (A→B→C):
  client/            # C-layer: PUBLIC API — Do/DoStream, pool, managed-pool,
                     #   retry, resolver, selector, rate limit, hooks, metrics.
                     #   Transports: single-conn, pool, managed, H1, ALPN.
  conn/              # B-layer: HTTP/2 connection, streams, flow control, handshake
    └── depends on: frame, hpack
  http1/             # B-layer: minimal HTTP/1.1 wire protocol (reuses hpack.HeaderField)
  grpc/              # C-layer: gRPC-over-HTTP/2 client (NOT a client.Transport —
                     #   separate entry point, absent from client.Do).
                     #   Sits on conn/, NOT on
                     #   client/ — client.DoStream writes the whole request body
                     #   before returning, so client-streaming and bidi are not
                     #   expressible through it. No protobuf dep: messages are
                     #   []byte. See docs/GRPC_GUIDE.md
  frame/             # A-layer: HTTP/2 frame codec (parser + writer + Framer)
  hpack/             # A-layer: RFC 7541 HPACK encoder/decoder

HTTP/3 stack (standalone — see Phase status):
  http3/             # RFC 9114: control stream, SETTINGS, request/response mapping.
                     #   Public entry: http3.Dial(ctx, addr, tls) → *Client, then Client.Do
  quic/              # RFC 9000/9001/9002: full QUIC v1 transport — packets, TLS 1.3
                     #   handshake, AEAD protection, key update, loss recovery, NewReno CC
  qpack/             # RFC 9204: static-table-only QPACK codec

internal/bytesx/     # Shared low-level: QUIC varint codec, buffer pool,
                     #   big-endian uint24/uint31, RFC 7540 padding strip
                     #   (used by frame, quic, http3 — NOT hpack)
docs/                # RFC_COVERAGE.md (authoritative test-to-RFC map),
                     #   HTTP3_DESIGN.md, CLIENT_GUIDE.md, GRPC_GUIDE.md,
                     #   BENCH_BASELINE.md, COVERAGE.md
```

Public packages: `client`, `conn`, `frame`, `grpc`, `hpack`, `http1`, `http3`,
`quic`, `qpack`. `cmd/` does not exist — library only; the one binary is
`examples/loadgen`. Each HTTP/2 `Conn` owns one `*frame.Framer` + one
`*hpack.Encoder` + one `*hpack.Decoder`, serializing writes via `wmu`.

## Phase status

Read [CHANGELOG.md](CHANGELOG.md), [conn/doc.go](conn/doc.go), and
[docs/HTTP3_DESIGN.md](docs/HTTP3_DESIGN.md) for detail. **Phases A, B, C,
and G all shipped** (latest release **v0.9.0**, 2026-07-11):

- **A** (`frame`, `hpack`) + **B** (`conn`, `http1`): HTTP/2 codec +
  connection engine — multi-stream, full bidirectional flow control,
  dynamic SETTINGS + ACK with retroactive `INITIAL_WINDOW_SIZE` resize,
  peer `MAX_CONCURRENT_STREAMS` gate, GOAWAY drain, PING ACK echo.
- **C** (`client`): public client — `Do`/`DoStream`, connection pool +
  managed-pool, retry/backoff, DNS + static service discovery, selectors,
  rate limiting, hooks, metrics. Documented in
  [docs/CLIENT_GUIDE.md](docs/CLIENT_GUIDE.md) (HTTP/1.1 + HTTP/2).
- **G** (`quic`, `http3`, `qpack`): from-scratch HTTP/3 over QUIC.
  **Standalone** — reached via `http3.Dial`, NOT yet wired into
  `client.Do` (no `TransportH3`); `http3.Client` is currently
  one-request-in-flight-per-conn with no pooling. Next H3 work: concurrent
  multiplexing → pooling → retry/metrics parity → `TransportH3` under
  `client.Do` → zero-alloc/bench-gate perf parity.

## Code-style gates (golangci-lint v2.5, see `.golangci.yml`)

- `revive` requires doc comments on every exported type, method,
  function, constant.
- `gosec` G115 + G402 tuned (intentional int conversions; TLS
  config opt-in by caller).
- `govet`: `fieldalignment` and `shadow` disabled (excessive churn).
- `unconvert` on — strip redundant `uint32(x)` etc.

## RFC trace policy (mandatory)

Every new conformance test MUST add row to
[docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md) keyed on RFC section.
`conformance-gate` CI job greps for `TestConformance_RFC7540_*`
and `TestConformance_RFC7541_*`, fails on regressions. Integration
and negative tests also belong in matrix when pinning specific
section behavior.

## Gotchas

- `frame.NewFramer(w io.Writer, r io.Reader)` — **writer first**, then
  reader. Easy to get backwards.
- `Stream.id == 0` until first `SendHeaders` writes HEADERS frame
  under `wmu` (B.2.1 deferred allocation; preserves §5.1.1
  monotonic-id ordering across concurrent `NewStream` callers). Don't
  read `Stream.ID()` before SendHeaders.
- `Stream.events` channel buffer = `opts.StreamEventBuffer` (default
  8). Full → `push` drops + sends RST(REFUSED_STREAM). Caller must
  drain `Recv` promptly or set larger buffer.
- `recvWindowRefundThreshold = 32 KiB` — WINDOW_UPDATEs batch at this
  granularity (B.2.2). Lower = more control-frame chatter; higher =
  tolerates more in-flight data without refund.
- `connRecvWindow = 65535` RFC-mandated at handshake; only per-stream
  window governed by `SETTINGS_INITIAL_WINDOW_SIZE`.
- Outbound flow control (B.2.3): `writeData` chunks at
  `min(peer MAX_FRAME_SIZE, our advertised MAX_FRAME_SIZE)`, blocks
  in `acquireSendCredits` on `fcOutCond` until per-stream + conn send
  windows have credit. Ctx cancel wakes cond via short-lived watchdog
  goroutine. `Conn.Close` broadcasts so in-flight writers bail with
  `ErrConnClosed`.
- `peerSettings` guarded by `psMu sync.RWMutex` (B.2.4). Only reader
  goroutine (`connHandler.OnSettings`) writes; `writeData` /
  `writeHeaders` take RLock. Don't read directly without lock.
- Mid-conn `SETTINGS_INITIAL_WINDOW_SIZE` change applies retroactively
  to all open streams (RFC §6.9.2 delta). Overflow past 2^31-1 →
  typed `ConnError(FLOW_CONTROL_ERROR)`.
- `NewStream` gates inflight on `min(local advertised,
  peer-advertised)` `MAX_CONCURRENT_STREAMS` (B.2.5). Returns
  `ErrTooManyStreams`. `lookupPeerSetting` distinguishes
  "absent" (fall through to local cap) from "explicit zero"
  (refuse all new streams).
- After peer GOAWAY (B.2.6): `NewStream` returns `ErrGoAway`. Streams
  id > `lastStreamID` get `EventReset(REFUSED_STREAM)`, evicted from
  registry; ≤ `lastStreamID` continue normally (RFC §6.8).
  `fcOutCond` broadcast so blocked writers re-check.
- Inbound non-ACK PING auto-echoed with `ACK=1` + same payload
  (RFC §6.7). No active PINGs initiated; ACK frames dropped.
- `net.Pipe` in unit tests **unbuffered + synchronous**. Tests writing
  >1 frame in a row from peer goroutine while client also writes will
  deadlock. Use `httptest`+h2 (real TCP buffers) for anything beyond
  single round trip.

## Testing patterns

- Integration suite: `conn/integration_test.go` + `conn/multistream_test.go`
  + `conn/flowcontrol_test.go` + `conn/sendflow_test.go` use
  `httptest.NewUnstartedServer` with `EnableHTTP2 = true` against real
  `net/http2.Server` peer.
- Unit suite: `pipeServer` helper in `conn/conn_test.go` drives
  `net.Pipe` peer for handshake-level checks. Symmetric read/write —
  every server-side write needs goroutine, every client-side write
  needs server reader running concurrently. For wire-byte assertions
  on single Conn method (e.g. `parseFrameHeaders` /
  `parseDataFrames` / `parseWindowUpdates`), wire `c.fr` to
  `bytes.Buffer` writer and assert produced bytes directly.
- Naming: `TestConformance_RFC7540_SecXX_Behavior` (gate-tracked),
  `TestIntegration_*`, `TestConn_*`, `TestStream_*`, `TestFramer_*`,
  `TestHandler_*`, `TestApplyPeerSettings_*`, `TestOnGoAway_*`,
  `TestOnPing_*`.

## Tooling notes

### Serena — primary code editor (always prefer over Edit/Write)

Serena is LSP-backed semantic MCP. Use for **all Go edits** in
this repo. (1) symbol-aware — understands Go structure, not just text;
(2) bypasses `tdd-guard` PreToolUse hook that fires on `Edit`/`Write`
and currently errors out against `z.ai` endpoint.

**Session start**: call `mcp__serena__initial_instructions` once, then
activate project:
```
mcp__plugin_serena_serena__activate_project  projectPath=/Users/ivanprikhodko/work/source/poseidon-http-client
```
`create_text_file` requires active project.

**Go `name_path` patterns** (pass to `find_symbol`, `replace_symbol_body`, etc.):

| Target | name_path |
|---|---|
| Top-level func/var/const | `FunctionName` |
| Method | `TypeName/MethodName` |
| Nested struct field | `TypeName/FieldName` |
| nth overload | `TypeName/MethodName[1]` |
| Interface method | `InterfaceName/MethodName` |

Pass `relative_path` to restrict search to one file (e.g. `conn/conn.go`).

**Core tools** (prefix `mcp__serena__` or `mcp__plugin_serena_serena__`):

| Tool | When to use |
|---|---|
| `get_symbols_overview` | Orient before editing file |
| `find_symbol` | Locate specific func/type by name_path |
| `replace_symbol_body` | Replace function/method body in place |
| `insert_after_symbol` | Add new func/method after named symbol |
| `insert_before_symbol` | Add before named symbol |
| `rename_symbol` | Rename across whole codebase (LSP refactor) |
| `safe_delete_symbol` | Delete only if no references remain |
| `find_referencing_symbols` | All call-sites / uses of symbol |
| `find_implementations` | Concrete types satisfying interface |
| `get_diagnostics_for_file` | LSP errors/warnings after edit |
| `create_text_file` | Create or fully overwrite file |
| `search_for_pattern` | Regex/literal search across files |
| `find_file` | Locate file by name glob |

**Known caveat**: `replace_symbol_body` on `var (...)` sentinel block
strips `var()` wrapper, produces invalid syntax. Workaround:
rewrite whole file with `create_text_file`.

**Serena memory** — project-scoped notes persisting across sessions.
Stored inside serena project, not Claude's memory system.
Use for codebase facts re-derived each session (lock ordering,
invariants, API decisions).

| Tool | When to use |
|---|---|
| `write_memory` | Save new note (key + body) |
| `read_memory` | Read specific note by key |
| `list_memories` | List all stored keys |
| `edit_memory` | Update existing note body |
| `rename_memory` | Rename key |
| `delete_memory` | Remove note |

Notes survive MCP restarts; **not** git-tracked. Keep complementary
to CLAUDE.md: CLAUDE.md for team-visible conventions, serena memory
for session-derived insights not worth commit.

### Other notes

- `commit-commands` plugin enforces 50-char subject line, rejects
  AI co-author trailers — keep commit subjects short, no `Co-Authored-By`.

## Workflow & reasoning

**Code review / refactoring / complex tasks**: invoke `karpathy-guidelines`
skill first. Enforces simplicity, avoids premature abstraction, keeps changes
minimal — critical for zero-alloc codec where every indirection layer costs.

**Problem analysis (bugs, regressions, unexpected behaviour)**: apply
**5 Whys** — ask "why did this happen?" five times to reach root cause
before proposing fix. Document chain in PR description.

**Deep reasoning (protocol edge-cases, concurrency invariants, API design)**:
use **sequential thinking** — break problem into ordered steps, reason through
each explicitly before writing code. Prevents confident wrong answers on tricky
RFC corner-cases.
