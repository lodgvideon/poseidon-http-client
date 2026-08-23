#!/usr/bin/env bash
# driver.sh — the run-poseidon harness.
#
# On Linux and macOS, run it directly from the repo root:
#
#   .claude/skills/run-poseidon/driver.sh smoke
#
# On Windows it runs INSIDE WSL (the Go work here does not happen on the
# Windows host):
#
#   wsl -e bash -lc 'bash /mnt/c/<repo>/.claude/skills/run-poseidon/driver.sh smoke'
#
# Keep the /mnt/... path inside the quoted -lc string: Git-Bash rewrites a bare
# /mnt/c/... argument into C:/Program Files/Git/mnt/c/... and the command dies.
#
# Every command prints what it ran and exits non-zero when the thing it checks
# is broken. `driver.sh help` lists them.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO" || exit 2

GO="${GO:-go}"
SKILL=".claude/skills/run-poseidon"
HARNESS="./$SKILL/harness"
SCRATCH="./$SKILL/scratch"

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
run()  { printf '   $ %s\n' "$*"; "$@"; }

# Two things differ between the GNU userland (Linux, WSL) and the BSD one
# (macOS), and both appear in commands below that are piped into xargs, where a
# shell function cannot be substituted. So they are resolved once, here.
#
#   -i          GNU takes no argument; BSD requires one (an empty suffix).
#   nproc       GNU only; macOS answers the same question via sysctl.
SED_INPLACE=(-i)
sed --version >/dev/null 2>&1 || SED_INPLACE=(-i '')

cpu_count() { nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo '?'; }
ok()   { printf '\033[32mOK\033[0m   %s\n' "$*"; }
bad()  { printf '\033[31mFAIL\033[0m %s\n' "$*"; }

# sh_gate runs one of the repo's scripts/*.sh gates.
#
# It cannot just exec them. .gitattributes pins the tree to eol=lf, but a
# Windows checkout drifts: in this worktree bench-gate.sh, coverage-gate.sh and
# rfc-coverage-gate.sh are CRLF, and from WSL that is
#   /usr/bin/env: 'bash\r': No such file or directory
# through the shebang, or — if you dodge that with `bash script.sh` —
#   script.sh: line 9: set: pipefail: invalid option name
# from the \r glued to the option. Feeding a CR-stripped copy on a file
# descriptor is the one form that works whatever the checkout did, and it costs
# nothing when the file is already LF.
sh_gate() {
  local s="$1"; shift
  printf '   $ bash <(sed CR-strip %s) %s\n' "$s" "$*"
  bash <(sed 's/\r$//' "$s") "$@"
}

# branch_name reads HEAD without git. In a worktree .git is a FILE holding a
# WINDOWS gitdir path ("gitdir: C:/Users/..."), which WSL's git cannot resolve —
# every git command from this side dies with "fatal: not a git repository".
# Rewriting the drive letter to /mnt/<letter> is how to read HEAD anyway.
# worktree_gitdir prints this worktree's real gitdir as a path THIS shell can
# open, or nothing when .git is an ordinary directory.
#
# On Linux and macOS the file already holds a POSIX path and the first candidate
# is it. On Windows it holds "gitdir: C:/Users/…", which is not a path any shell
# here can open: WSL wants /mnt/c/Users/…, Git-Bash wants /c/Users/…. Try each
# and return the one that exists rather than guessing which shell is running.
worktree_gitdir() {
  [ -f .git ] || return 0
  local raw; raw="$(sed 's/^gitdir: //' .git | tr -d '\r')"
  local drive rest c
  case "$raw" in
    [A-Za-z]:*)
      drive="$(printf '%s' "${raw%%:*}" | tr '[:upper:]' '[:lower:]')"
      rest="${raw#*:}" ;;
    *) printf '%s' "$raw"; return 0 ;;
  esac
  for c in "/mnt/$drive$rest" "/$drive$rest" "$raw"; do
    [ -e "$c" ] && { printf '%s' "$c"; return 0; }
  done
  printf '%s' "/mnt/$drive$rest"
}

branch_name() {
  local head=.git/HEAD gitdir
  gitdir="$(worktree_gitdir)"
  [ -n "$gitdir" ] && head="$gitdir/HEAD"
  sed 's|ref: refs/heads/||' "$head" 2>/dev/null || echo '?'
}

# ---------------------------------------------------------------- env

cmd_env() {
  say "toolchain"
  printf '   host:          %s %s (%s sed)\n' "$(uname -s)" "$(uname -m)" \
    "$(sed --version >/dev/null 2>&1 && echo GNU || echo BSD)"
  printf '   repo:          %s\n' "$REPO"
  printf '   branch:        %s\n' "$(branch_name)"
  printf '   go:            %s\n' "$($GO version)"
  printf '   go.mod says:   %s\n' "$(grep -E '^(go|toolchain) ' go.mod | tr '\n' ' ')"
  printf '   golangci-lint: %s\n' "$(command -v golangci-lint || echo 'MISSING — lint unavailable')"
  printf '   docker:        %s\n' "$(command -v docker || echo 'MISSING — mutation/h3 unavailable')"
  printf '   python3:       %s\n' "$(command -v python3 || echo 'MISSING — RFC quote check unavailable')"
  printf '   cpus:          %s\n' "$(cpu_count)"
  local crlf
  crlf="$(find . -name '*.go' -not -path './.claude/*' -print0 | xargs -0 file | grep -c CRLF)"
  if [ "$crlf" -gt 0 ]; then
    printf '   CRLF .go files: %s — gofmt reports every one as unformatted; `lint` works around it\n' "$crlf"
  else
    printf '   CRLF .go files: 0 — LF tree, nothing for the lint snapshot to fix up\n'
  fi
}

# ---------------------------------------------------------------- smoke

cmd_smoke() {
  say "live client smoke (real sockets, real h2/h1 servers)"
  run "$GO" run "$HARNESS" smoke
}

# ---------------------------------------------------------------- loadgen

# Starts the harness h2 server, aims ./examples/loadgen at it, tears the server
# down. This is the shipped binary against a live peer — as close to "run the
# app" as a client library gets.
#
# Server and load MUST be in one driver invocation: a background process
# started inside `wsl -e bash -lc '...'` is killed when that command returns, so
# there is no "leave the server running and come back to it" between two Bash
# tool calls.
#
#   driver.sh loadgen [duration] [workers] [conns]
cmd_loadgen() {
  local dur="${1:-5s}" workers="${2:-32}" conns="${3:-4}"
  local log; log="$(mktemp)"

  say "loadgen: ./examples/loadgen -> harness serve ($dur, $workers workers, $conns conns)"
  $GO run "$HARNESS" serve >"$log" 2>&1 &
  local srv=$!
  local url="" i
  for i in $(seq 1 100); do
    url="$(grep -m1 '^URL=' "$log" 2>/dev/null | cut -d= -f2-)"
    [ -n "$url" ] && break
    kill -0 "$srv" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$url" ]; then
    bad "harness serve never printed a URL"; cat "$log"
    kill "$srv" 2>/dev/null; rm -f "$log"; return 1
  fi
  printf '   server: %s (pid %s)\n' "$url" "$srv"

  run "$GO" run ./examples/loadgen \
    -url "$url" -insecure -duration "$dur" -workers "$workers" -conns "$conns"
  local rc=$?

  kill "$srv" 2>/dev/null; wait "$srv" 2>/dev/null; rm -f "$log"
  return $rc
}

# ---------------------------------------------------------------- trace

# Same run as loadgen, with the frame tracer on, and only the first frames
# kept. POSEIDON_DEBUG=frames is the repo's wire-level log: it is an env var
# rather than a flag so a run that went wrong can be repeated verbatim with the
# log turned on. Expect tens of thousands of lines a second at full tilt — hence
# the tiny duration and the head.
#
#   driver.sh trace [lines]
cmd_trace() {
  local lines="${1:-40}" log; log="$(mktemp)"

  say "frame trace (POSEIDON_DEBUG=frames, 300ms of traffic, first $lines frames)"
  $GO run "$HARNESS" serve >"$log" 2>&1 &
  local srv=$!
  local url="" i
  for i in $(seq 1 100); do
    url="$(grep -m1 '^URL=' "$log" 2>/dev/null | cut -d= -f2-)"
    [ -n "$url" ] && break
    sleep 0.1
  done
  printf '   server: %s\n' "$url"
  POSEIDON_DEBUG=frames $GO run ./examples/loadgen \
    -url "$url" -insecure -duration 300ms -workers 1 -conns 1 2>&1 | head -n "$lines"

  kill "$srv" 2>/dev/null; wait "$srv" 2>/dev/null; rm -f "$log"
}

# ---------------------------------------------------------------- scratch

# Direct invocation: call one internal function without standing the whole
# client up. Go has no REPL, and a scratch main.go cannot live in a normal
# package — it would join ./..., break `go build`, and drag the coverage gate
# down. Under .claude/ the go tool ignores it (dot-directories are skipped by
# the ... pattern) while an explicit path still compiles it against this tree.
cmd_scratch() {
  if [ ! -f "$SCRATCH/main.go" ]; then
    mkdir -p "$SCRATCH"
    cat >"$SCRATCH/main.go" <<'TEMPLATE'
// Command scratch is a disposable main for poking one function directly.
// Edit freely; nothing imports it and ./... never sees it.
package main

import (
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func main() {
	// Encoder and Decoder carry a dynamic table and must come from their
	// constructors — a zero value has no table and panics on first use.
	enc := hpack.NewEncoder()
	buf := enc.EncodeBlock(nil, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte("x-demo"), Value: []byte("hello")},
	})
	fmt.Printf("encoded %d bytes: % x\n", len(buf), buf)

	// DecodeBlock hands each field to a visitor; the bytes are only valid for
	// the duration of the call.
	dec := hpack.NewDecoder()
	err := dec.DecodeBlock(buf, func(f hpack.HeaderField) error {
		fmt.Printf("  %s: %s\n", f.Name, f.Value)
		return nil
	})
	fmt.Printf("decode err=%v\n", err)
}
TEMPLATE
    printf '   created %s/main.go from the template\n' "$SCRATCH"
  fi
  say "scratch (edit $SCRATCH/main.go, re-run)"
  run "$GO" run "$SCRATCH"
}

# ---------------------------------------------------------------- build

cmd_build() {
  local rc=0
  say "build + vet (default tags)"
  run "$GO" build ./... || rc=1
  run "$GO" vet ./... || rc=1

  # The debug build tag is compile-checked in CI and nowhere else; it rots
  # silently otherwise.
  say "build + vet (-tags poseidondebug)"
  run "$GO" build -tags poseidondebug ./... || rc=1
  run "$GO" vet -tags poseidondebug ./... || rc=1

  # Tagged test files never compile under the default tags, so nothing else
  # here notices when they stop compiling.
  say "vet tagged test files (integration, soak, allocbench)"
  run "$GO" vet -tags integration ./client/... || rc=1
  run "$GO" vet -tags soak ./client/ || rc=1
  run "$GO" vet -tags allocbench ./http3/ || rc=1

  # ./... stops at a nested go.mod: these two are invisible to everything above.
  #
  # -o /dev/null is load-bearing. test/interop/quic is a single main package, so
  # a plain `go build ./...` there WRITES the binary into the module directory
  # and leaves test/interop/quic/quic untracked in the user's tree. (From the
  # repo root the same command discards its output — go only writes the
  # executable when it builds exactly one main package.)
  say "nested modules"
  local m
  for m in contrib/prometheus test/interop/quic; do
    run "$GO" -C "$m" build -o /dev/null ./... || rc=1
    run "$GO" -C "$m" vet ./... || rc=1
  done

  # And the harness itself lives in a dot-directory, so ./... never builds it.
  say "skill harness"
  run "$GO" vet "$HARNESS" || rc=1
  [ -f "$SCRATCH/main.go" ] && { run "$GO" vet "$SCRATCH" || rc=1; }

  [ $rc -eq 0 ] && ok "build" || bad "build"
  return $rc
}

# ---------------------------------------------------------------- test

cmd_test() {
  say "go test -race"
  if [ $# -gt 0 ]; then
    run "$GO" test -race -count=1 -timeout=180s "$@"
  else
    run "$GO" test -race -count=1 -timeout=180s ./...
  fi
}

# ---------------------------------------------------------------- alloc

# Two gates, both easy to miss locally:
#
#  1. The //go:build !race allocation tests. `go test -race ./...` drops the
#     whole file, so a new per-request allocation passes a green -race run.
#     A test escapes this step if EITHER its package is missing from the list
#     OR its name misses the -run pattern; both have happened here.
#  2. scripts/bench-gate.sh: an absolute 0 B/op + 0 allocs/op rule over seven
#     packages. It takes RAW benchmark output as an argument — `make bench-gate`
#     passes none and dies on the script's ${1:?} guard, so this runs the
#     benchmarks itself.
cmd_alloc() {
  local rc=0
  say "allocation gates (NOT under -race — the detector allocates)"
  run "$GO" test -count=1 -run 'Allocs|DoesNotAllocate|IsNotCopied|CallOptions' \
    ./conn/ ./client/ ./grpc/ ./http1/ || rc=1

  say "zero-alloc bench gate (7 packages, -benchtime=100ms -count=1)"
  local out; out="$(mktemp)"
  run "$GO" test -run='^$' -bench=. -benchmem -benchtime=100ms -count=1 \
    ./frame ./hpack ./internal/bytesx ./internal/bufx ./qpack ./quic ./http3 \
    >"$out" 2>&1
  printf '   benchmark lines: %s\n' "$(grep -cE '^Benchmark' "$out")"
  sh_gate scripts/bench-gate.sh "$out" || { rc=1; grep -E '^Benchmark' "$out" | head -20; }
  rm -f "$out"

  [ $rc -eq 0 ] && ok "alloc gates" || bad "alloc gates"
  return $rc
}

# ---------------------------------------------------------------- lint

# Lint runs against an LF SNAPSHOT of the tree, with the toolchain pinned to
# what go.mod names. Two independent traps, both measured here:
#
#  1. golangci-lint v2.5.0 is built with go1.25; WSL's go is 1.26.6. That
#     combination does not report findings — it PANICS with "file requires
#     newer Go version go1.26 (application built with go1.25)" inside go/types.
#     GOTOOLCHAIN pins the loader back to go.mod's toolchain line.
#  2. gofmt emits LF only, so every CRLF file reports as unformatted — 71
#     phantom findings in this worktree. Stripping CR in a copy is the only fix
#     that does not rewrite the user's tree (the real fix, `git add
#     --renormalize .`, is a git operation, and git does not run from WSL here).
cmd_lint() {
  local rc=0
  command -v golangci-lint >/dev/null || { bad "golangci-lint not installed"; return 2; }
  local pin; pin="$(awk '/^toolchain /{print $2}' go.mod)"
  local snap; snap="$(mktemp -d)"

  say "LF snapshot (gofmt cannot read a CRLF tree)"
  tar -c --exclude=./.git --exclude=./.claude . | tar -x -C "$snap"
  local crlf; crlf="$(find "$snap" -name '*.go' -print0 | xargs -0 file | grep -c CRLF)"
  find "$snap" -type f \( -name '*.go' -o -name '*.mod' -o -name '*.sum' -o -name '*.sh' -o -name '*.py' \) \
    -print0 | xargs -0 sed "${SED_INPLACE[@]}" 's/\r$//'
  printf '   %s (%s CRLF .go files normalised, toolchain pinned to %s)\n' "$snap" "$crlf" "$pin"

  say "golangci-lint (root module)"
  printf '   $ GOTOOLCHAIN=%s golangci-lint run ./...\n' "$pin"
  ( cd "$snap" && GOTOOLCHAIN="$pin" golangci-lint run ./... ) || rc=1

  say "golangci-lint (nested modules)"
  local m
  for m in contrib/prometheus test/interop/quic; do
    printf '   $ (cd %s && golangci-lint run)\n' "$m"
    ( cd "$snap/$m" && GOTOOLCHAIN="$pin" golangci-lint run ) || rc=1
  done

  rm -rf "$snap"
  [ $rc -eq 0 ] && ok "lint" || bad "lint"
  return $rc
}

# ---------------------------------------------------------------- conformance

# rfc-coverage-gate.sh greps a RAW `go test -v` log for one passing
# TestConformance_<TAG>_* per RFC tag, so the -v output has to be captured and
# handed over as an argument. Called with no argument it dies on its own
# ${1:?rfc.txt} guard, which reads like a broken gate rather than a misuse.
cmd_conformance() {
  local rc=0 log; log="$(mktemp)"
  say "RFC conformance tests"
  run "$GO" test -count=1 -v -run=Conformance ./... >"$log" 2>&1 || rc=1
  printf '   %s passing TestConformance_* functions\n' "$(grep -c -- '--- PASS: TestConformance_' "$log")"

  say "RFC tag gate (one passing TestConformance_<TAG>_* per tag)"
  sh_gate scripts/rfc-coverage-gate.sh "$log" || rc=1

  # Fails closed without scripts/.rfc-cache — "could not check" is not "fine".
  # The cache is present in this checkout (12 RFC texts); on a fresh one the
  # first run downloads them from rfc-editor.org.
  say "verbatim RFC quotation check"
  run python3 scripts/rfc-quote-check.py || rc=1

  rm -f "$log"
  [ $rc -eq 0 ] && ok "conformance" || bad "conformance"
  return $rc
}

# ---------------------------------------------------------------- coverage

cmd_coverage() {
  say "coverage profile + 80% per-package floor"
  local pkgs; pkgs="$($GO list ./... | grep -v /examples/)"
  # shellcheck disable=SC2086
  run "$GO" test -race -count=1 -timeout=180s -coverprofile=cover.out $pkgs || return 1
  sh_gate scripts/coverage-gate.sh 80
}

# ---------------------------------------------------------------- mutation

# Gremlins over the diff against a base. Docker only, and that is a correctness
# requirement: run natively on this tree and it scores 0% mutator coverage for
# every subdirectory, which looks exactly like a pass.
#
# In a worktree the container also has to be told where git lives. `make
# mutation` fails here with
#   fatal: not a git repository: /src/C:/Users/.../.git/worktrees/<name>
# because .git is a file holding a Windows path that resolves nowhere inside
# the container. Bind-mounting the MAIN repo's .git and pointing GIT_DIR at
# this worktree's entry inside it fixes the `git diff` Gremlins shells out to.
#
# Runtime tracks the diff: coverage alone is ~40s, and a branch far from
# origin/main can run for many minutes. Pass a nearer base to scope it.
cmd_mutation() {
  local base="${1:-origin/main}"
  command -v docker >/dev/null || { bad "docker not available"; return 2; }

  local args=(--rm -v "$REPO:/src" -w /src -v poseidon-gremlins-gopath:/go
              -e GOCACHE=/go/build-cache
              -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory
              -e GIT_CONFIG_VALUE_0=/src)
  local gitdir; gitdir="$(worktree_gitdir)"
  if [ -n "$gitdir" ]; then
    local main_git="${gitdir%/worktrees/*}" name="${gitdir##*/}"
    args+=(-v "$main_git:/gitmain" -e "GIT_DIR=/gitmain/worktrees/$name" -e GIT_WORK_TREE=/src)
    printf '   worktree: GIT_DIR=/gitmain/worktrees/%s\n' "$name"
  fi

  say "gremlins unleash --diff $base (Docker)"
  run docker run "${args[@]}" gogremlins/gremlins:0.6.0 gremlins unleash --diff "$base"
}

# ---------------------------------------------------------------- h3

# Real HTTP/3 against three independent server implementations — Caddy, nginx
# and aioquic — with this client and all three servers in containers on one
# Docker network. The client runs in-container because Docker Desktop on
# Windows does not forward host UDP.
cmd_h3() {
  command -v docker >/dev/null || { bad "docker not available"; return 2; }
  local compose=test/integration/http3/docker-compose.yml
  say "HTTP/3 interop: caddy + nginx + aioquic (Docker)"
  run docker compose -f "$compose" run --rm runner
  local rc=$?
  docker compose -f "$compose" down -v >/dev/null 2>&1
  return $rc
}

# ---------------------------------------------------------------- pr

# The gate stack a pull request has to clear, cheapest first so a break shows up
# early. Coverage, mutation and the Docker suites are deliberately not here —
# they are slow and belong to a deliberate run.
cmd_pr() {
  local rc=0 failed=() step
  for step in build lint smoke test alloc conformance; do
    "cmd_$step" || { rc=1; failed+=("$step"); }
  done
  say "summary"
  if [ $rc -eq 0 ]; then
    ok "build lint smoke test alloc conformance"
  else
    bad "failed: ${failed[*]}"
  fi
  return $rc
}

# ---------------------------------------------------------------- help

cmd_help() {
  cat <<'USAGE'
driver.sh <command> [args]   — run from inside WSL, from any cwd

  env                     toolchain, branch, CRLF state                  <1s
  smoke                   live H1/H2 client scenarios over real sockets   ~1s
  loadgen [dur] [w] [c]   ./examples/loadgen against a local h2 server   ~10s
  trace [lines]           same, with POSEIDON_DEBUG=frames wire log      ~10s
  scratch                 direct-invocation scratch main (creates it)     ~2s
  build                   build+vet: default tags, poseidondebug, tagged
                          test files, nested modules, this harness        ~40s
  test [pkgs...]          go test -race -count=1 (default ./...)           ~2m
  alloc                   !race allocation tests + zero-alloc bench gate   ~20s
  lint                    golangci-lint on an LF snapshot, root + nested   ~30s
  conformance             -run=Conformance + RFC coverage + quote check    ~25s
  coverage                coverage profile + 80% floor                     ~4m
  pr                      build lint smoke test alloc conformance          ~4m
  mutation [base]         Gremlins over the diff vs base (Docker)   diff-sized
  h3                      HTTP/3 interop, 3 servers (Docker)              ~45s
USAGE
}

# ----------------------------------------------------------------

case "${1:-help}" in
  env|smoke|loadgen|trace|scratch|build|test|alloc|lint|conformance|coverage|mutation|h3|pr|help)
    c="$1"; shift; "cmd_$c" "$@" ;;
  *) printf 'unknown command: %s\n\n' "$1"; cmd_help; exit 2 ;;
esac
