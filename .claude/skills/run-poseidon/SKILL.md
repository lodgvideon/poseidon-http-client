---
name: run-poseidon
description: Build, run, drive and verify poseidon-http-client. Use when asked to run this client against a live server, load-test it, see HTTP/2 frames on the wire, call one internal function directly, run the tests, lint, check allocations, or clear the gates a pull request has to pass on this machine.
---

`poseidon-http-client` is a library, so "running it" means driving the client
against a live peer. Everything here goes through one script:

```
.claude/skills/run-poseidon/driver.sh   # the harness — start here
.claude/skills/run-poseidon/harness/    # Go program it drives (smoke scenarios + h2 server)
.claude/skills/run-poseidon/scratch/    # disposable main for direct calls (created on demand)
```

## How to call it

**Linux and macOS** — run it straight from the repo (or worktree) root:

```bash
.claude/skills/run-poseidon/driver.sh smoke
```

**Windows** — the driver runs inside WSL, because the Go work here does not
happen on the Windows host. Two mechanical traps: WSL does not inherit the Bash
tool's working directory (it starts in a Docker bind-mount path), and Git-Bash
rewrites a bare `/mnt/c/...` *argument* into `C:/Program Files/Git/mnt/c/...`.
So translate the current directory into a `/mnt/...` path and keep it inside
the quoted `-lc` string:

```bash
wsl -e bash -lc "bash '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')/.claude/skills/run-poseidon/driver.sh' smoke"
```

Either way, swap the last word for any command below, and check what you are
on with `driver.sh env` — it prints `host:` (kernel, arch, GNU or BSD sed)
before anything else. Timings are from the Windows/WSL machine this was built
on (8 cores, warm build cache).

| command | what it does | time |
|---|---|---|
| `env` | toolchain, branch, how many files are CRLF | <1s |
| `smoke` | 7 client scenarios over real sockets | ~1s |
| `loadgen [dur] [workers] [conns]` | `./examples/loadgen` against a local h2 server | ~15s |
| `trace [lines]` | same run with the `POSEIDON_DEBUG=frames` wire log | ~15s |
| `scratch` | call one internal function directly, no client | ~2s |
| `build` | build+vet under every tag, nested modules, harness | ~40s |
| `test [pkgs...]` | `go test -race -count=1`, 13 packages | ~2m |
| `alloc` | `!race` allocation tests + zero-alloc bench gate | ~20s |
| `lint` | golangci-lint on an LF snapshot, root + nested | ~30s |
| `conformance` | `-run=Conformance` + RFC tag gate + quote check | ~25s |
| `coverage` | coverage profile + 80% per-package floor | ~4m |
| `pr` | build, lint, smoke, test, alloc, conformance | ~4m |
| `mutation [base]` | Gremlins over the diff (Docker) | diff-sized |
| `h3` | HTTP/3 interop vs caddy, nginx, aioquic (Docker) | ~45s |

The examples below are written in the Windows form, because that is the host
they were run on. On Linux and macOS drop the `wsl -e bash -lc "..."` wrapper
and run `.claude/skills/run-poseidon/driver.sh <command>` from the repo root —
everything else, including the output, is the same.

## Platforms

- **Linux** — the driver is a plain bash script with no Windows dependency.
  Verified on Ubuntu 24.04 / GNU userland (that is what WSL is), both against a
  `/mnt/c` checkout and a native-filesystem clone. Needs: bash, Go (the
  version `go.mod` pins), golangci-lint v2.5 for `lint`, python3 for the RFC
  quote check, Docker for `mutation` and `h3`.
- **macOS** — same commands; **not run on a macOS host** during this skill's
  construction, so treat it as portable-by-construction rather than verified.
  The two BSD-userland differences that would have broken it are handled in the
  driver: `sed -i` takes an empty suffix argument on BSD (`SED_INPLACE`), and
  `nproc` does not exist (`cpu_count` falls back to `sysctl -n hw.ncpu`).
  Everything else it uses — `file`, `xargs -0`, `tar --exclude`, `mktemp -d`,
  `seq`, process substitution — exists in the BSD userland, and the script uses
  no bash-4 syntax, so Apple's system bash 3.2 is enough. `driver.sh env`
  prints which `sed` it detected; if a command misbehaves there, that line is
  the first thing to read.
- **Windows** — via WSL only, and the driver carries four workarounds that
  exist purely for this host: CR-stripping the repo's gate scripts, linting an
  LF snapshot, reading HEAD without git, and handing Docker a `GIT_DIR` it can
  follow. On Linux and macOS they are inert (an LF tree strips to itself) — no
  branch of the script is Windows-only.

## Run (agent path)

**See the client work.** `smoke` stands up real `httptest` servers — h2 over
TLS with ALPN, and cleartext HTTP/1.1 — and drives the public API against them:
`Do`, `DoStream`, the pool under 16 concurrent workers, the metrics split, and
a `Retryer` replaying two 503s.

```bash
wsl -e bash -lc "bash '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')/.claude/skills/run-poseidon/driver.sh' smoke"
```

```
PASS h2-get               200, 15 bytes, server saw HTTP/2.0                         5ms
PASS h2-post-echo         36864 bytes over multiple DATA frames                      3ms
PASS h2-stream            status 200, 4 DATA events, "chunk-0;chunk-1;chunk-2;"      18ms
PASS h2-pool-concurrency  320 requests, 4 conns, p99 4.194303ms                      6ms
PASS h2-non2xx-metrics    404 counted as non-2xx, not as an error                    2ms
PASS h1-plaintext-get     200 over HTTP/1.1                                          0s
PASS retry-503            2 503s replayed, attempt 3 won                             2ms

smoke: 7/7 scenarios passed
```

Scenarios live in [harness/main.go](harness/main.go); add one when a change
needs a live check the unit tests cannot express. The set is verified to
discriminate: an off-by-one in `http1.parseStatusCode` reddens
`h1-plaintext-get` alone, and `ev.Status >= 200` → `> 200` in
`Client.observeDone` reddens `h2-pool-concurrency` alone.

**Load-test the real binary.** `loadgen` starts the harness server, points
`./examples/loadgen` at it, and tears the server down — all in one invocation,
because a background process started inside `wsl -e bash -lc '...'` dies when
that command returns.

```bash
wsl -e bash -lc "bash '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')/.claude/skills/run-poseidon/driver.sh' loadgen 5s 32 4"
```

```
   server: https://127.0.0.1:36497/ (pid 73556)
   $ go run ./examples/loadgen -url https://127.0.0.1:36497/ -insecure -duration 5s -workers 32 -conns 4
loadgen: hitting / via 127.0.0.1:36497 — 32 workers, 4 conns, 5s, rps=0
==== loadgen summary (5.0s) ====
connections:     4 dialed (0 failed)
requests started: 439668 (87921 req/s)
  2xx:            439638
  errored:        30
latency p50:      524.287µs
latency p99:      2.097151ms
```

**See the frames.** `trace` is the same run with the wire tracer on, 300 ms of
traffic, output truncated to N lines.

```bash
wsl -e bash -lc "bash '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')/.claude/skills/run-poseidon/driver.sh' trace 18"
```

```
11:13:56.964348 h2 -> SETTINGS stream=0 len=36 params=HEADER_TABLE_SIZE=4096,ENABLE_PUSH=0,...
11:13:56.964508 h2 <- SETTINGS stream=0 len=30 params=MAX_FRAME_SIZE=1048576,...
11:13:56.964599 h2 -> HEADERS stream=1 len=45 flags=END_STREAM|END_HEADERS
11:13:56.964622 h2 <- WINDOW_UPDATE stream=0 len=4 incr=983041
11:13:56.965127 h2 <- DATA stream=1 len=2 flags=END_STREAM
11:13:56.965189 h2 -> HEADERS stream=3 len=5 flags=END_STREAM|END_HEADERS
```

The first HEADERS is 45 bytes and the next is 5 — that is the HPACK dynamic
table working, and it is the number a header-encoding change should move.

## Direct invocation

Most changes here touch one function, not the whole client. Go has no REPL, and
a scratch `main.go` cannot live in a normal package — it would join `./...`,
break `go build`, and drag the coverage gate down. Put it under `.claude/`
instead: the go tool skips directories whose name starts with a dot, so `./...`
never sees it, while an explicit path still compiles it against this tree.

```bash
wsl -e bash -lc "bash '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')/.claude/skills/run-poseidon/driver.sh' scratch"
```

The first run writes `scratch/main.go` (an HPACK encode → decode round trip)
and runs it; edit that file to call whatever you are working on, re-run:

```
encoded 15 bytes: 82 40 06 78 2d 64 65 6d 6f 05 68 65 6c 6c 6f
  :method: GET
  x-demo: hello
decode err=<nil>
```

`driver.sh build` vets the scratch package too, so a stale one gets noticed
instead of rotting.

## Gates

`driver.sh pr` runs build, lint, smoke, test, alloc and conformance in that
order — cheapest first, so a break surfaces early. Each is runnable alone. They
exist because **several `make` targets do not work from this host**:

- `make lint` panics instead of reporting (see Troubleshooting) — `driver.sh
  lint` pins the toolchain and lints an LF snapshot.
- `make bench-gate` calls `scripts/bench-gate.sh` with no argument and dies on
  the script's own `${1:?}` guard — `driver.sh alloc` runs the benchmarks and
  hands the raw output in.
- `make mutation` dies in a worktree with `fatal: not a git repository:
  /src/C:/Users/...` — Gremlins shells out to `git diff` and the container
  cannot follow this `.git` file. `driver.sh mutation` bind-mounts the main
  repo's `.git` and sets `GIT_DIR`.

`make test-race`, `make test-fast` and single-package `go test` work fine and
remain the right tools while iterating on one package:

```bash
wsl -e bash -lc "cd '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')' && go test ./conn/ -count=1 -race -timeout 90s"
```

Two Docker gates worth running deliberately rather than on every change:

- `driver.sh h3` — real HTTP/3 over QUIC against three independent servers,
  ending with `TestInterop_ClientH3_BBR_HugeTransfer` pulling 1 MiB from each of
  caddy, nginx and aioquic. Run it for any change under `quic/`, `http3/` or
  `qpack/`; the unit suite has no real peer.
- `driver.sh mutation [base]` — the gate a PR is scored on (>80% efficacy over
  changed files). Runtime tracks the diff, so pass a nearer base when the branch
  is far from `origin/main`.

## Test

`driver.sh test` is `go test -race -count=1 ./...`: 13 packages, ~2 minutes.
Green on this tree apart from one known flake (see Troubleshooting).
`driver.sh coverage` adds the profile and the 80% per-package floor — 91.7%
total here, floor-nearest being conn 89.2% and http3 89.3%.

Both are `-race`, which means the `//go:build !race` allocation gates in `conn`,
`client`, `grpc` and `http1` do **not** run. That is what `driver.sh alloc` is
for: a green `-race` run plus a skipped allocation gate is exactly how a
per-request allocation ships.

## Gotchas

- **A background process does not survive the `wsl -e` call that started it.**
  `nohup sleep 45 &` in one Bash tool call is gone by the next one, `nohup` or
  not. Anything needing a server and a client must run both in a single
  invocation — that is why `loadgen` and `trace` own their server instead of
  telling you to start one.
- **`git` does not run from WSL in a worktree.** `.git` here is a FILE holding
  a Windows path (`gitdir: C:/Users/...`) that WSL's git cannot resolve; every
  command fails with `fatal: not a git repository`. Run git and `gh` from the
  Windows side. The driver reads HEAD by rewriting the drive letter to `/mnt/c`
  itself, and passes the same trick to Docker for Gremlins.
- **Part of this checkout is CRLF, and it breaks tooling two ways.** 71 of 732
  `.go` files plus 78 shell/python/yaml/markdown files carry CRLF against a
  `.gitattributes` that pins the tree to `eol=lf`. Consequences: `gofmt`
  reports every CRLF file as unformatted (71 phantom findings), and
  `./scripts/*.sh` refuse to start from WSL. The driver works around both. The
  real fix is `git add --renormalize .` from the Windows side — a decision for
  the repo owner, not something the driver does behind your back.
- **Files written from the Windows side land as CRLF**, this skill's own files
  included. After editing anything here, normalise it or the driver trips over
  the trap it documents:
  ```bash
  wsl -e bash -lc "cd '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')' && find .claude/skills/run-poseidon -type f -print0 | xargs -0 sed -i 's/\r$//'"
  ```
- **`go build ./...` inside a nested module writes a binary into the tree.**
  `go -C test/interop/quic build ./...` leaves an untracked `quic` executable,
  because go writes the executable when it builds exactly one main package. Use
  `-o /dev/null` there; from the repo root the same command discards it.
- **`loadgen` reporting a few dozen `errored` at the end is not a bug.** Those
  are in-flight requests cancelled when the duration context expires — roughly
  one per worker. A count near the worker count is expected; a count
  proportional to the request total is not.
- **`rtk` compresses `go test` output usefully and lies about lint.**
  `rtk golangci-lint run` exits 0 while findings exist. Never wrap lint in it.
  On `[build failed]`, re-run the test raw — rtk drops the compiler line.

## Troubleshooting

- **`/usr/bin/env: 'bash\r': No such file or directory`** — a CRLF script. Run
  it as `bash <(sed 's/\r$//' scripts/x.sh) ARGS`, which is what the driver
  does. Plain `bash scripts/x.sh` only moves the error to `set: pipefail:
  invalid option name`.
- **`panic: file requires newer Go version go1.26 (application built with
  go1.25)`** from golangci-lint — WSL's go is 1.26.6 and golangci-lint v2.5.0
  is built with go1.25. It panics rather than reporting, so it reads like a
  broken repo. Fix: `GOTOOLCHAIN=go1.25.13 golangci-lint run` (the driver reads
  the pin from `go.mod`'s `toolchain` line). It returns when `go.mod` moves past
  what the installed linter understands — then upgrade the linter, do not drop
  the pin.
- **`File is not properly formatted (gofmt)` on files you never touched** —
  CRLF, not formatting. Lint the LF snapshot: `driver.sh lint`.
- **`line 3: 1: rfc.txt`** from `scripts/rfc-coverage-gate.sh` — the gate takes
  a raw `go test -v -run=Conformance` log as its argument and greps it for
  `--- PASS: TestConformance_<TAG>`. With no argument it dies on its own
  `${1:?rfc.txt}`. `driver.sh conformance` captures the log first (1316 passing
  conformance functions on this tree).
- **`bench file empty or missing`** from `scripts/bench-gate.sh` — same shape:
  it wants raw `go test -bench -benchmem` output as `$1`. `driver.sh alloc`
  produces it, then checks every line is `0 B/op 0 allocs/op`.
- **`fatal: not a git repository: /src/C:/Users/...`** from a container — the
  worktree `.git` file again. Bind-mount the main repo's `.git` and set
  `GIT_DIR` (see `cmd_mutation`), or run from the main checkout.
- **`TestStreamRecvAccessors_NoRaceWithReader` failing with "no reader
  goroutine polled an accessor"** — a flaky fixture, not a bug in `quic`. Failed
  in 2 of 3 whole-module `-race` runs here on 2026-08-23, while
  `go test ./quic/ -run TestStreamRecvAccessors_NoRaceWithReader -race
  -count=10` is 10/10 green. The writer closes its `stop` channel the moment it
  finishes mutating, and nothing guarantees the three reader goroutines were
  scheduled before that — so `polls == 0` and the test's own guard fires,
  correctly reporting that the scenario never happened. Re-run that one test
  before believing a `pr` failure that names it.

## Verify this skill still works

```bash
wsl -e bash -lc "bash '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')/.claude/skills/run-poseidon/driver.sh' env && bash '$(pwd | sed 's|^/\([a-z]\)/|/mnt/\1/|')/.claude/skills/run-poseidon/driver.sh' smoke"
```

`env` prints the toolchain and the CRLF count; `smoke` must end in `7/7
scenarios passed` and exit 0. If `smoke` fails, either the client is broken or
the harness has drifted from an API change — `driver.sh build` vets the harness
and says which.

What this skill deliberately does **not** cover: how to write the change —
test structure (Arrange–Act–Assert, `require`/`assert`), the RFC trace policy,
the peer-input policy, commit conventions. Those live in
[CLAUDE.md](../../../CLAUDE.md), [CONTRIBUTING.md](../../../CONTRIBUTING.md) and
the `reviewing-tests` skill. This one is about making the code actually run and
the gates actually execute on this machine.
