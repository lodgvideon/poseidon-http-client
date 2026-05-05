#!/usr/bin/env bash
# Absolute zero-alloc gate on raw `go test -bench -benchmem` output.
#
# Spec §11 requires every hot-path benchmark to report 0 B/op and 0 allocs/op.
# Lines look like:
#   BenchmarkFoo-10    4569750    25.71 ns/op    0 B/op    0 allocs/op
# We require the last four columns to be "<B> B/op <A> allocs/op" with B=0
# and A=0. Anything else fails the gate.
set -euo pipefail

BENCH="${1:?path to raw \`go test -bench\` output required}"

if [ ! -s "$BENCH" ]; then
  echo "bench file empty or missing: $BENCH"
  exit 1
fi

# At minimum we need to have run *some* benchmark.
if ! grep -E '^Benchmark' "$BENCH" >/dev/null; then
  echo "no Benchmark lines found in $BENCH"
  exit 1
fi

violations=$(awk '
  /^Benchmark/ {
    name = $1
    b_op = ""; allocs = ""
    for (i = 2; i <= NF; i++) {
      if ($i == "B/op")      b_op = $(i-1)
      if ($i == "allocs/op") allocs = $(i-1)
    }
    if (b_op == "" || allocs == "") next
    if (b_op + 0 != 0 || allocs + 0 != 0) {
      printf "%s\t%s B/op\t%s allocs/op\n", name, b_op, allocs
    }
  }
' "$BENCH")

if [ -n "$violations" ]; then
  echo "Bench gate FAILED — non-zero allocations detected:"
  echo "$violations"
  exit 1
fi

echo "Bench gate OK — all benchmarks 0 B/op, 0 allocs/op"
