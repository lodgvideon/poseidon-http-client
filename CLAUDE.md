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
the current milestone. **Phase B complete** (B.2.6 merged or in PR
stack #5/#6/#7): multi-stream, full bidirectional flow control,
dynamic SETTINGS + ACK with retroactive `INITIAL_WINDOW_SIZE` resize,
peer `MAX_CONCURRENT_STREAMS` gate, GOAWAY drain, PING ACK echo.
Next: **Phase C** — public client + connection pool + service
discovery.

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
- Outbound flow control (B.2.3): `writeData` chunks at
  `min(peer MAX_FRAME_SIZE, our advertised MAX_FRAME_SIZE)` and
  blocks in `acquireSendCredits` on `fcOutCond` until per-stream +
  conn send windows have credit. Ctx cancel wakes the cond via a
  short-lived watchdog goroutine. `Conn.Close` also broadcasts so
  in-flight writers bail with `ErrConnClosed`.
- `peerSettings` is guarded by `psMu sync.RWMutex` (B.2.4). Only the
  reader goroutine (`connHandler.OnSettings`) writes; `writeData` /
  `writeHeaders` take RLock. Don't read directly without the lock.
- Mid-conn `SETTINGS_INITIAL_WINDOW_SIZE` change applies retroactively
  to all open streams (RFC §6.9.2 delta). Overflow past 2^31-1 →
  typed `ConnError(FLOW_CONTROL_ERROR)`.
- `NewStream` gates inflight on `min(local advertised,
  peer-advertised)` `MAX_CONCURRENT_STREAMS` (B.2.5). Returns
  `ErrTooManyStreams`. `lookupPeerSetting` distinguishes
  "absent" (fall through to local cap) from "explicit zero"
  (refuse all new streams).
- After peer GOAWAY (B.2.6): `NewStream` returns `ErrGoAway`. Streams
  whose id > `lastStreamID` receive `EventReset(REFUSED_STREAM)` and
  are evicted from the registry; ≤ `lastStreamID` continue normally
  (RFC §6.8). `fcOutCond` is broadcast so blocked writers re-check.
- Inbound non-ACK PING is auto-echoed with `ACK=1` and same payload
  (RFC §6.7). We don't initiate active PINGs; ACK frames are dropped.
- `net.Pipe` in unit tests is **unbuffered + synchronous**. Tests that
  write more than one frame in a row from the peer goroutine while
  the client is also writing will deadlock. Use `httptest`+h2 (real
  TCP buffers) for anything beyond a single round trip.

## Testing patterns

- Integration suite: `conn/integration_test.go` + `conn/multistream_test.go`
  + `conn/flowcontrol_test.go` + `conn/sendflow_test.go` use
  `httptest.NewUnstartedServer` with `EnableHTTP2 = true` against a
  real `net/http2.Server` peer.
- Unit suite: `pipeServer` helper in `conn/conn_test.go` drives a
  `net.Pipe` peer for handshake-level checks. Symmetric read/write —
  every server-side write needs a goroutine, every client-side write
  needs a server reader running concurrently. For wire-byte assertions
  on a single Conn method (e.g. `parseFrameHeaders` /
  `parseDataFrames` / `parseWindowUpdates`), wire `c.fr` to a
  `bytes.Buffer` writer and assert the produced bytes directly.
- Naming: `TestConformance_RFC7540_SecXX_Behavior` (gate-tracked),
  `TestIntegration_*`, `TestConn_*`, `TestStream_*`, `TestFramer_*`,
  `TestHandler_*`, `TestApplyPeerSettings_*`, `TestOnGoAway_*`,
  `TestOnPing_*`.

## Tooling notes

### Serena — primary code editor (always prefer over Edit/Write)

Serena is an LSP-backed semantic MCP. Use it for **all Go edits** in
this repo. Two reasons: (1) symbol-aware — understands Go structure,
not just text; (2) bypasses `tdd-guard` PreToolUse hook that fires on
`Edit`/`Write` and currently errors out against the `z.ai` endpoint.

**Session start**: call `mcp__serena__initial_instructions` once, then
activate project:
```
mcp__plugin_serena_serena__activate_project  projectPath=/Users/ivanprikhodko/work/source/poseidon-http-client
```
`create_text_file` requires the project to be active.

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
| `get_symbols_overview` | Orient before editing a file |
| `find_symbol` | Locate a specific func/type by name_path |
| `replace_symbol_body` | Replace a function/method body in place |
| `insert_after_symbol` | Add new func/method after named symbol |
| `insert_before_symbol` | Add before named symbol |
| `rename_symbol` | Rename across whole codebase (LSP refactor) |
| `safe_delete_symbol` | Delete only if no references remain |
| `find_referencing_symbols` | All call-sites / uses of a symbol |
| `find_implementations` | Concrete types satisfying an interface |
| `get_diagnostics_for_file` | LSP errors/warnings after an edit |
| `create_text_file` | Create or fully overwrite a file |
| `search_for_pattern` | Regex/literal search across files |
| `find_file` | Locate file by name glob |

**Known caveat**: `replace_symbol_body` on a `var (...)` sentinel block
strips the `var()` wrapper and produces invalid syntax. Workaround:
rewrite the whole file with `create_text_file`.

**Serena memory** — project-scoped notes that persist across sessions.
Stored inside the serena project, not in Claude's own memory system.
Use for codebase facts that would otherwise be re-derived every session
(e.g. lock ordering, invariants, API decisions).

| Tool | When to use |
|---|---|
| `write_memory` | Save a new note (key + body) |
| `read_memory` | Read a specific note by key |
| `list_memories` | List all stored keys |
| `edit_memory` | Update existing note body |
| `rename_memory` | Rename a key |
| `delete_memory` | Remove a note |

Notes survive MCP restarts; they are **not** git-tracked. Keep them
complementary to CLAUDE.md: CLAUDE.md for team-visible conventions,
serena memory for session-derived insights not worth a commit.

### Other notes

- `commit-commands` plugin enforces a 50-char subject line and rejects
  AI co-author trailers — keep commit subjects short and don't add
  `Co-Authored-By` lines.

## Workflow & reasoning

**Code review / refactoring / complex tasks**: invoke the `karpathy-guidelines`
skill before starting. It enforces simplicity, avoids premature abstraction,
and keeps changes minimal — critical for a zero-alloc codec codebase where
every layer of indirection has a cost.

**Problem analysis (bugs, regressions, unexpected behaviour)**: apply
**5 Whys** — ask "why did this happen?" five times to reach the root cause
before proposing a fix. Document the chain in the PR description.

**Deep reasoning (protocol edge-cases, concurrency invariants, API design)**:
use **sequential thinking** — break the problem into ordered steps, reason
through each one explicitly before writing code. Prevents confident wrong
answers on tricky RFC corner-cases.
