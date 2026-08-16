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
make bench       # benches with bench-gate (see below — it is NOT just frame + hpack)
```

Single-package iteration:

```bash
go test ./conn/ -count=1 -race -timeout 90s
go test ./conn/ -run TestIntegration_TenConcurrentStreams_Echo -v
```

Pre-commit hook (optional): `git config core.hooksPath .githooks`.

## Where to run commands (Windows host)

**Run Go work in WSL, not PowerShell.** The host is Windows, but the Linux
toolchain is what CI runs, and PowerShell quoting mangles anything with
`$(...)`, `$((...))`, or nested quotes. Invoke WSL from the **Bash** tool:

```bash
wsl -e bash -lc 'cd /mnt/c/Users/<user>/work/source/poseidon-http-client/... && go test ./conn/'
```

Two mechanical traps, both verified:

- **Keep every `/mnt/...` path INSIDE the quoted `-lc '…'` string.** Git-Bash
  rewrites a bare `/mnt/c/...` *argument* into `C:/Program Files/Git/mnt/c/...`
  (MSYS path conversion) and the command dies with `No such file or directory`.
  For anything long, write a `.sh` file and run `wsl -e bash -lc 'bash -l /mnt/c/…/x.sh'`.
- **git does NOT work from WSL in a worktree.** A worktree's `.git` file points
  at a *Windows* absolute path (`C:/Users/...`), which WSL's git cannot resolve —
  every command fails with `fatal: not a git repository`. Run git (and `gh`) from
  the Windows side.

WSL's *installed* Go is older than the repo requires; `go.mod` makes it fetch the
matching toolchain, so `go version` reports the repo's version, not the installed
one. `GOTOOLCHAIN=local` exposes the difference. Both `go vet` under each build
tag and `GOOS=linux golangci-lint run` are meaningful only on the Linux side —
`_linux.go` files are never analysed on the Windows host.

### rtk (token-reducing CLI proxy, WSL-only)

`rtk` wraps commands and compresses their output. Measured on this repo:
`go test -v ./frame/` goes from 17.9 KB to 34 B, and real test failures survive
with `file:line` and the assertion message intact.

**Use it for `go test`. Do not use it for `golangci-lint` or `git`:**

| Command | Verdict |
|---|---|
| `rtk go test ./...` | **Use.** Failures kept with detail; exit code correct. On `[build failed]` it drops the compiler line — re-run raw to see *why*. |
| `rtk golangci-lint run` | **Do not use.** Returns **exit 0 while findings exist** (raw returns 1), misattributes the file, and drops the message. Confirmed 2/2 runs. |
| `rtk git …` | Moot — git does not run from WSL here (see above). |

Do **not** run `rtk init` / install its auto-rewrite hook: there is no Windows
`rtk` binary for Claude Code to exec, and the hook would rewrite `golangci-lint`
into the form whose exit code lies.

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
  trace/             # RFC-neutral wire-observation vocabulary (Tracer, FrameInfo,
                     #   TextTracer, POSEIDON_DEBUG). frame emits into it; http1
                     #   and http3/quic seams are follow-ups (#610). Imports
                     #   nothing but stdlib — everything else may import IT.
header/            # RFC-neutral header vocabulary (Field, IndexingMode).
                     #   hpack.HeaderField is an ALIAS of header.Field, so no
                     #   caller broke. http1/http3 import THIS, not hpack — a
                     #   CI step keeps that edge from returning.

HTTP/3 stack (reachable through client.Do — see Phase status):
  http3/             # RFC 9114: control stream, SETTINGS, request/response mapping.
                     #   Public entry: http3.Dial(ctx, addr, tls) → *Client, then Client.Do;
                     #   or client.TransportH3 / H3Pool / H3Managed via client.Do
  quic/              # RFC 9000/9001/9002: full QUIC v1 transport — packets, TLS 1.3
                     #   handshake, AEAD protection, key update, loss recovery, NewReno CC
  qpack/             # RFC 9204: static-table-only QPACK codec

internal/bytesx/     # QUIC varint codec (RFC 9000 §16) — used by quic, http3
internal/bufx/       # HTTP/2 byte helpers: read-buffer pool, big-endian
                     #   uint24/uint31, RFC 7540 padding strip — used by frame
                     #   ONLY. Split from bytesx because the two sets had zero
                     #   overlapping consumers.
docs/                # RFC_COVERAGE.md (authoritative test-to-RFC map),
                     #   HTTP3_DESIGN.md, CLIENT_GUIDE.md, GRPC_GUIDE.md,
                     #   BENCH_BASELINE.md, COVERAGE.md
```

Public packages: `client`, `conn`, `frame`, `grpc`, `hpack`, `http1`, `http3`,
`quic`, `qpack`, `trace`. `cmd/` does not exist — library only; the one binary is
`examples/loadgen`. Each HTTP/2 `Conn` owns one `*frame.Framer` + one
`*hpack.Encoder` + one `*hpack.Decoder`, serializing writes via `wmu`.

## Phase status

Read [CHANGELOG.md](CHANGELOG.md), [conn/doc.go](conn/doc.go), and
[docs/HTTP3_DESIGN.md](docs/HTTP3_DESIGN.md) for detail. **Phases A, B, C,
and G all shipped** (latest release **v0.12.0**, 2026-08-10; a v0.13.0 cycle
is in progress, so `main` is well ahead of the tag):

- **A** (`frame`, `hpack`) + **B** (`conn`, `http1`): HTTP/2 codec +
  connection engine — multi-stream, full bidirectional flow control,
  dynamic SETTINGS + ACK with retroactive `INITIAL_WINDOW_SIZE` resize,
  peer `MAX_CONCURRENT_STREAMS` gate, GOAWAY drain, PING ACK echo.
- **C** (`client`): public client — `Do`/`DoStream`, connection pool +
  managed-pool, retry/backoff, DNS + static service discovery, selectors,
  rate limiting, hooks, metrics. Documented in
  [docs/CLIENT_GUIDE.md](docs/CLIENT_GUIDE.md) (HTTP/1.1 + HTTP/2).
- **G** (`quic`, `http3`, `qpack`): from-scratch HTTP/3 over QUIC, **fully
  wired into `client.Do`**. `http3.Client` multiplexes concurrent `Do` calls
  over one conn and is safe for concurrent use; `client` exposes
  `TransportH3` (single lazy-dialled conn), `TransportH3Pool` and
  `TransportH3Managed` (resolver + selector), so H3→H2 transport parity is
  complete. `http3.Dial` remains the lower-level entry point. Documented in
  [docs/HTTP3_GUIDE.md](docs/HTTP3_GUIDE.md).

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
`conformance-gate` CI job runs `go test -run=Conformance ./...` and then
`scripts/rfc-coverage-gate.sh`, which requires at least one passing
`TestConformance_<TAG>_*` for every tag in its list — currently RFC2616,
7540, 7541, 7838, 8441, 9000, 9001, 9002, 9110, 9112, 9113, 9204, 9114,
not just the two HTTP/2 ones. The same job runs `scripts/rfc-quote-check.py`,
which requires every span a comment presents as an RFC quotation to be
verbatim. Integration and negative tests also belong in matrix when pinning
specific section behavior.

## Gotchas

- **`bench-gate` covers seven packages, not two — and it is two jobs, not one.**
  `.github/workflows/bench-gate.yml` benchmarks
  `./frame ./hpack ./internal/bytesx ./internal/bufx ./qpack ./quic ./http3`
  and `scripts/bench-gate.sh` scans that raw output with **no package
  scoping at all**: any `Benchmark` line reporting non-zero `B/op` or
  non-zero `allocs/op` fails the job. So adding an honest benchmark of an
  allocating path to any of those seven turns CI red. The repo's answer is
  an env guard — see `requireSendBench` in `quic/bench_throughput_test.go`
  and `POSEIDON_BENCH_ENCODE` in `http3/encoder_install_race_test.go`; a
  skipped benchmark prints no `B/op` columns, so the gate ignores it.
  (`conn`, `client`, `grpc` and `http1` are genuinely outside it and use
  their own `//go:build !race` `AllocsPerRun` gates instead.)
  Since the 2026-08-14 split, **pull requests run `alloc-gate`** — the same
  script over the same seven packages at `-benchtime=100ms -count=5`, ~45s —
  and the full `-benchtime=2s` sweep plus the informational benchstat ns/op
  diff runs as **`bench-full`, on `v*` tags and `workflow_dispatch` only**.
  PRs therefore keep the real zero-alloc gate but print no ns/op comparison;
  the benchstat baseline at tag time is the **previous release tag**, not
  `main`, and it is `continue-on-error` — it gates nothing.
- **`quic-interop` gates a partition, not a pass count.**
  `.github/workflows/quic-interop.yml` runs the pinned quic-interop-runner
  against `quic-go` and `ngtcp2` and hands the result to
  `.github/interop/assert_partition.py`, which compares every cell against
  `.github/interop/expected.json` **in both directions**: a cell declared
  `succeeded` that comes back `unsupported` is a capability regression, and a
  cell declared `unsupported` that comes back `succeeded` means the exit-127
  support table in `test/interop/quic/main.go` now lies about us. So
  implementing a declined feature (key update, ECN, chacha20…) turns this red
  until **both** the support table and `expected.json` are edited — that is
  intended, not a flake. Same split as the bench gate: `pull_request` runs a
  short leg (two servers × `handshake`, `transfer`, `retry`, ~2m45s), `v*` tags
  and `workflow_dispatch` run the whole non-measurement matrix (~13 min). There
  is no retry; the only tolerance is `"expect": null`, spent on five
  `(cell, server)` pairs of network fault injection — none of them in the PR leg
  — plus `versionnegotiation`, which the pinned runner never asks for. The three
  `h3-interop*` jobs in `integration.yml` are unrelated — they test the library's
  own HTTP/3 client against real servers, not this endpoint binary.
- `frame.NewFramer(w io.Writer, r io.Reader)` — **writer first**, then
  reader. Easy to get backwards.
- `Framer.writeHeader(h, detail)` is the outbound trace funnel — every write
  path goes through it EXCEPT the `WriteHeaders` fast path, which coalesces
  header+block into one `Write` and calls `traceOut` itself. A new write method
  that hand-rolls its header is invisible to the tracer. `detail` is the frame's
  *logical* payload (padding excluded) or nil.
- `Stream.id == 0` until first `SendHeaders` writes HEADERS frame
  under `wmu` (B.2.1 deferred allocation; preserves §5.1.1
  monotonic-id ordering across concurrent `NewStream` callers). Don't
  read `Stream.ID()` before SendHeaders.
- `Stream.events` channel buffer = `opts.StreamEventBuffer` (default
  8). Full → `push` drops + sends RST(CANCEL). Caller must
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

### Test structure and assertions (mandatory for new and edited tests)

**Arrange–Act–Assert.** Every test is three visible blocks in that order: build the
fixture, perform the one action under test, assert the outcome. Separate them with a
blank line. A test that interleaves acting and asserting is describing a scenario, not
a property — split it into several tests or a table.

**Assert with `testify`, not hand-rolled comparisons.** Do not write
`if got != want { t.Fatalf(...) }`. Use:

- `require.*` where the test cannot meaningfully continue — a constructor that must
  succeed, a non-nil pointer about to be dereferenced. It aborts, like `t.Fatalf`.
- `assert.*` where it can — independent field checks, so one run reports every
  mismatch instead of only the first.

**The mapping is not cosmetic: `t.Fatalf` → `require`, `t.Errorf` → `assert`.**
Swapping a `t.Fatalf` for `assert` lets the test run on with invalid state and turns a
clean failure into a nil-dereference panic several lines later.

**Keep the failure message.** This suite's messages explain *why* the property matters
and what breaks without it — that is deliberate and worth more than the assertion
syntax. `testify`'s defaults print only expected-vs-actual, so carry the explanation
over as the `msgAndArgs` argument rather than dropping it:

```go
require.NoError(t, err, "Establish against the listener")
assert.Truef(t, errors.Is(err, ErrInvalidOptions),
    "NewClient error = %v; a caller classifying this cannot tell it from a transport failure", err)
```

**Two places `testify` must not go:**

- inside an `AllocsPerRun` closure or any body an allocation gate measures — it
  reflects and allocates, and the gate counts the whole process;
- in `//go:build !race` alloc gates and the bench-gate packages, for the same reason.
  Assert outside the measured closure.

`testify` is not in `go.mod` yet, and `make tidy` strips an unused requirement — so it
arrives with the first test that imports it, not before.

**Scope.** The rule binds new tests and any test you touch. The existing 2427 test
functions are being migrated separately and deliberately, not in passing — tracking
issue **#722**, one issue per package (#723-#737), batched by whole files.

**Run the `reviewing-tests` skill after writing or changing any test**, not only when a
review is asked for. It carries the full bar in three passes: can the test fail at all
(mutate it and watch it go red), were its cases designed by a functional-testing
technique — equivalence classes, boundary values, decision tables, state transitions,
negative cases — or hand-picked, and only then the AAA/`testify` structure above.

## Tooling notes

### Serena — primary code editor (always prefer over Edit/Write)

Serena is LSP-backed semantic MCP. Use for **all Go edits** in
this repo. (1) symbol-aware — understands Go structure, not just text;
(2) bypasses `tdd-guard` PreToolUse hook that fires on `Edit`/`Write`
and currently errors out against `z.ai` endpoint.

**Session start**: call `mcp__serena__initial_instructions` once, then activate the
project by passing the **current working directory** — which in a worktree session is the
worktree, not the repo root:

```
mcp__serena__activate_project  project=<absolute path of the cwd>
```

Activate the WORKTREE, not `…/poseidon-http-client`. Worktrees live under
`.claude/worktrees/` INSIDE the repo, so activating the root indexes the main checkout
plus every worktree and yields duplicate symbols for every file.

Serena's onboarding memories for this project (`core`, `tech_stack`,
`suggested_commands`, `conventions`, `task_completion`) live in `.serena/memories/` and
are read with `mcp__serena__read_memory`; start from `core`. Serena line numbers are
**0-based**.

**Context is `claude-code`** (`.mcp.json` passes `--context claude-code`; the default is
`desktop-app`, which is wrong for a CLI agent). That context disables the tools Claude
Code already has, so these Serena tools are **NOT available**: `read_file`,
`create_text_file`, `execute_shell_command`, `list_dir`, `find_file`,
`search_for_pattern`. Use Read / Write / Bash / Glob / Grep for those. It also sets
`structured_tool_output: false` to dodge a Claude Code escaping bug.

Two consequences worth knowing:

- The old workaround for the `replace_symbol_body` `var (...)` bug was "rewrite the whole
  file with `create_text_file`" — that tool is gone under this context. Use `Write`.
- `--project` is deliberately NOT passed in `.mcp.json`, because that file is shared by
  every worktree and would pin them all to one. `activate_project` therefore stays
  available and must be called each session (see above). Do not add `--project`: the
  context carries `single_project: true`, which disables `activate_project` as soon as a
  project is supplied at startup.

Serena is pinned to tag `v1.7.0`. Bump it deliberately; an unpinned MCP can change its
tool surface under you.

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
| `replace_content` | Regex/literal replace when no symbol fits |

For creating a file, searching text, or globbing a filename, use `Write`, `Grep` and
`Glob` — the Serena equivalents are disabled by the `claude-code` context.

**Known caveat**: `replace_symbol_body` on `var (...)` sentinel block
strips `var()` wrapper, produces invalid syntax. Workaround:
rewrite the whole file with `Write`.

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
