#!/usr/bin/env bash
# Ratio gate for the congestion-control bottleneck (#362).
#
# Nothing may be concluded from the CC matrix until this passes. The rule is
# arithmetic: halve the bottleneck rate and the same transfer must take twice as
# long. A knob that does not bind fails it, and a knob that does not bind is
# exactly how this repo previously produced a confident "BBR is 36% faster in
# bufferbloat" that was an artefact — 1 MiB at a nominal 1 Mbps completing in
# 109 ms against an arithmetic 8.4 s (scripts/cc-matrix.sh:31).
#
# Usage:  scripts/cc-ratio-check.sh [fast-rate] [slow-rate] [bytes]
# Needs docker; on Windows run it from WSL.
set -uo pipefail

FAST="${1:-4mbit}"
SLOW="${2:-2mbit}"
BYTES="${3:-2097152}"
COMPOSE="test/integration/http3/docker-compose.cc.yml"
TOL=25   # percent tolerance on the 2x expectation

# run_at RATE -> echoes the measured seconds, or empty on failure.
run_at() {
  local rate="$1" out shaper
  docker compose -f "$COMPOSE" down -v >/dev/null 2>&1
  out=$(RATE="$rate" H3_CC_BYTES="$BYTES" H3_CC_REPEATS=1 H3_INTEROP_CC=newreno \
        docker compose -f "$COMPOSE" run --rm runner 2>&1)

  # The shaper must prove itself in the log. A run that silently went unshaped
  # would otherwise look like a fast one.
  shaper=$(docker compose -f "$COMPOSE" logs lossproxy 2>/dev/null | grep -o 'SHAPER: .*' | head -1)
  if ! printf '%s' "$shaper" | grep -q 'qdisc tbf'; then
    echo "  rate=$rate: NO SHAPER IN LOG (${shaper:-nothing logged}) — refusing to report a number" >&2
    return 1
  fi
  echo "  rate=$rate: $shaper" >&2

  # The runner logs "CCSUMMARY ... mean_ms=<n> ..."; report it in seconds.
  printf '%s' "$out" | sed -n 's/.*mean_ms=\([0-9.]*\).*/\1/p' | tail -1 |
    awk 'NF{printf "%.3f", $1/1000}'
}

echo "Ratio gate: $BYTES bytes at $FAST vs $SLOW (expect ~2x slower at $SLOW, +/-${TOL}%)"
f=$(run_at "$FAST") || exit 1
s=$(run_at "$SLOW") || exit 1
docker compose -f "$COMPOSE" down -v >/dev/null 2>&1

if [ -z "$f" ] || [ -z "$s" ]; then
  echo "FAIL: could not parse an elapsed time (fast=${f:-?} slow=${s:-?})"
  exit 1
fi

awk -v f="$f" -v s="$s" -v tol="$TOL" '
BEGIN {
  r = s / f
  lo = 2 * (1 - tol/100); hi = 2 * (1 + tol/100)
  printf "fast=%.2fs  slow=%.2fs  ratio=%.2f  want %.2f..%.2f\n", f, s, r, lo, hi
  if (r >= lo && r <= hi) { print "PASS: the bottleneck binds; the matrix may be run"; exit 0 }
  print  "FAIL: the rate knob does not bind arithmetically. Do NOT run the matrix —"
  print  "      a comparison across cells that do not respond to the knob measures nothing."
  exit 1
}'
