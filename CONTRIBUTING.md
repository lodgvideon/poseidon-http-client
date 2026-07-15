# Contributing

poseidon-http-client implements HTTP/1.1, HTTP/2, and HTTP/3 from scratch.
Correctness is enforced by CI gates, not by convention. This document tells
you what those gates are and how to pass them locally before you push.

## Prerequisites

- **Go 1.25+** (`go.mod` declares `go 1.25`; CI runs 1.25).
- **golangci-lint v2.5** — the lint config (`.golangci.yml`) targets this
  version; other versions may report different findings.
- **Docker** with Compose v2 — required only for the integration and
  HTTP/3 interop suites (`make it-test`, `make h3-interop*`). Unit and
  conformance tests need no Docker.
- **benchstat** (optional) — `go install golang.org/x/perf/cmd/benchstat@latest`
  if you want the same before/after bench comparison CI prints.

## Build and test

```bash
make tidy        # go mod tidy
make lint        # go vet + golangci-lint run
make test-race   # go test -race -count=1 ./...   (the default verification)
make test-fast   # unit + integration only, skips stress/E2E (~fast, still -race)
make bench       # all benchmarks, -benchmem, count=10
make bench-gate  # zero-alloc gate (see below); needs bench output file
```

`make test-race` is the bar. Run it before every push.

For single-package iteration:

```bash
go test ./conn/ -count=1 -race -timeout 90s
go test ./conn/ -run TestIntegration_TenConcurrentStreams_Echo -v
```

Use `-count=1` to defeat the test cache; use `-run` to narrow to one test
while iterating, then run the whole package before committing.

## RFC trace policy (mandatory)

Every conformance test maps to a specific RFC section, and that mapping is
tracked in [docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md). The policy:

1. A new conformance test **must** add a row to `docs/RFC_COVERAGE.md`,
   keyed on the RFC section it pins.
2. Conformance tests follow the naming scheme
   `TestConformance_RFC<number>_Sec<XX>_<Behavior>`, e.g.
   `TestConformance_RFC7540_Sec61_DataFrame_PaddedEndStream`. Covered RFCs:
   7540, 7541, 9000, 9001, 9002, 9114, 9204 (plus 2616 and 8441 rows).
3. The `conformance-gate` CI job runs `go test -run=Conformance ./...` and
   fails the PR if any conformance test fails or if any covered RFC has
   zero passing conformance tests (`scripts/rfc-coverage-gate.sh`).

Integration and negative tests that pin behavior of a specific RFC section
also belong in the matrix, in their own rows.

## Peer-input policy (mandatory)

This is a client. Every byte it parses is chosen by someone else, and a server
that is hostile — or merely broken — is the normal case, not the edge case. So:

**New code that consumes peer-controlled bytes must ship with a bound and a
fuzz target that provably reaches it.**

1. **A bound.** Peer input must never drive an unbounded `append`, `make`, map
   insert, loop, goroutine, or timer. Reach for a read primitive that is bounded
   *by construction* — `bufio.ReadSlice`, `io.LimitReader` — over "read it, then
   check the length". By the time you can check, the memory is already spent.
2. **A cap that charges what memory retains.** If the cap counts wire bytes but
   the code retains decoded, decompressed, or re-encoded bytes, the cap
   under-counts by the expansion ratio. This is a real bug we shipped:
   `strings.ToLower` is Unicode-aware and re-encodes each invalid byte as the
   3-byte U+FFFD rune, so an 8 MiB cap admitted 24 MiB.
3. **Reuse the existing limit.** Prefer an established number
   (`conn.defaultMaxHeaderListSize`, a `SETTINGS` value) over a new one. A second
   number for the same concept is a bug waiting to happen.
4. **A fuzz target that reaches the new parser.** Same package is not the same
   thing as reached — trace the call chain from the entry point and say so in the
   PR. Run it (`-fuzz=FuzzX -fuzztime=120s`), and commit any crasher as a seed.
5. **Know what fuzzing cannot do.** Fuzz inputs are finite, so a never-terminated
   line or a never-ending stream just hits EOF. Unbounded-read bugs need a test
   with an *infinite* peer. Fuzzing catches panics, amplification, and
   half-built results; it does not catch "reads forever".

If you add a parser and cannot bound it, say so in the PR rather than leaving it
implied.

### Why this rule exists

Every miss so far has the same shape: **the mechanism existed, and the new code
was outside its reach.** The receive-bounds work enumerated frame readers, so the
QPACK dynamic table — not a frame reader — grew without limit. `maxControlFrameLen`
is a `FrameReader` mechanism, so the unframed QPACK encoder stream was never
covered by it. `http1` was decorative until it became a pooled transport, and its
reads were never revisited. When you add a parser, the question is not "is there
a bound in this package" but "does the bound reach *me*".

## Interop matrix (HTTP/3)

The HTTP/3 client is verified against real, independent server
implementations over real UDP, in Docker. Each target brings its stack up,
runs the suite in a container, and tears down on exit:

```bash
make h3-interop          # baseline: Caddy (quic-go) HTTP/3 server
make h3-interop-loss     # same, through a relay dropping ~10% of datagrams (RFC 9002 loss recovery)
make h3-interop-reorder  # relay reorders ~20%, no loss (ack ranges + reassembly)
make h3-interop-fault    # deliberately misbehaving server; checks typed error surfacing
make h3-interop-chacha   # nginx pinned to TLS_CHACHA20_POLY1305_SHA256
make h3-soak             # sustained concurrent load, leak assertions; not in CI
```

Override knobs where documented in the Makefile, e.g.
`LOSS_PCT=5 make h3-interop-loss`.

The HTTP/2 integration suite has its own Docker stack:

```bash
make it-test        # brings up Docker servers, runs -tags=integration, tears down
make it-test-fast   # in-process Go reference server only, no Docker
```

Note for Windows: Docker Desktop does not forward host UDP, which is why
the interop client runs in-container on a shared network. Use the Make
targets as-is; do not try to dial the interop servers from the host.

## Commit conventions

- **Conventional Commits**: `feat(http3): ...`, `fix(conn): ...`,
  `docs: ...`, `test: ...`, `chore: ...`.
- **Subject line 50 characters or less.** The commit hook rejects longer.
- **No AI co-author trailers** (`Co-Authored-By: Claude ...` and similar
  are rejected).
- Body explains *why*, not *what* — the diff already shows what.

Optional local hook setup: `git config core.hooksPath .githooks`.

## Code-style gates

`make lint` must pass clean. The configuration that matters:

- **golangci-lint v2.5** with the repo's `.golangci.yml`. Run that exact
  version to match CI.
- **revive** requires a doc comment on every exported type, method,
  function, and constant. No exported symbol ships undocumented.
- **unconvert** is on — redundant conversions like `uint32(x)` on an
  already-`uint32` value fail lint.
- `govet` runs with `fieldalignment` and `shadow` disabled; everything
  else applies.

### Zero-alloc bench gate

The codec hot paths (`frame`, `hpack`, `qpack`, plus `quic`/`http3`
covered paths) must benchmark at **0 B/op and 0 allocs/op**. The
`bench-gate` CI job runs the benchmarks on your branch and fails on any
non-zero allocation figure (`scripts/bench-gate.sh`). If your change adds
an allocation to a gated path, CI rejects it — there is no override.
Reproduce locally:

```bash
go test -bench=. -benchmem -benchtime=2s -count=5 -run=^$ ./frame ./hpack ./qpack | tee head.txt
./scripts/bench-gate.sh head.txt
```

## Pull request expectations

Before opening a PR:

1. `make test-race` passes. All tests, `-race`, no skips you added.
2. `make lint` is clean.
3. New conformance tests have their `docs/RFC_COVERAGE.md` rows.
4. Changes touching `frame`, `hpack`, or `qpack` hot paths keep the
   zero-alloc benchmarks at 0 B/op.
5. Bug fixes include a test that fails without the fix. Include the root
   cause in the PR description, not just the symptom.

CI runs the same gates (`ci`, `conformance-gate`, `bench-gate`,
`integration`); a PR merges only when all are green. Keep PRs focused —
one logical change per PR reviews faster and bisects cleaner.
