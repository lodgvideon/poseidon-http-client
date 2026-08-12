#!/usr/bin/env bash
# Payload gate for the congestion-control benchmark (#564).
#
# The cheaper of the two arithmetic gates and the one that would have caught the
# bug on day one: double the payload, and the elapsed time must double. It needs
# no bandwidth shaper at all.
#
# What it catches: the benchmark used to post its payload to Caddy's "/" route,
# a canned `respond` that answers on the request headers and never reads the
# body. The client returned on that response with the payload still buffered, so
# 1 MiB and 8 MiB both "took" ~0.6 ms and the derived goodput reached 11 GiB/s.
# A timer that is not attached to the transfer cannot be fixed by improving the
# transfer, which is why this runs before any congestion-control comparison.
#
# The companion gate is scripts/cc-ratio-check.sh: halve the bottleneck rate and
# the elapsed time must double.
#
# Usage:  scripts/cc-scale-check.sh [small-bytes] [factor]
# Needs docker; on Windows run it from WSL.
set -uo pipefail

SMALL="${1:-1048576}"
FACTOR="${2:-8}"
BIG=$(( SMALL * FACTOR ))
COMPOSE="test/integration/http3/docker-compose.cc.yml"
TOL=40   # percent; generous, because this gate is about orders of magnitude

run_bytes() {
  local n="$1" out
  docker compose -f "$COMPOSE" down -v >/dev/null 2>&1
  out=$(H3_CC_BYTES="$n" H3_CC_REPEATS=1 H3_INTEROP_CC=newreno \
        docker compose -f "$COMPOSE" run --rm runner 2>&1)

  # The sink must show it consumed the payload. Without this a 502, or a peer
  # that answers without draining, reads as a very fast transfer — which is
  # precisely the bug being gated.
  local got
  got=$(docker compose -f "$COMPOSE" logs sink 2>/dev/null |
        sed -n 's/.*SINK received=\([0-9]*\).*/\1/p' | tail -1)
  if [ "${got:-0}" -lt "$n" ]; then
    echo "  bytes=$n: sink received ${got:-0} of $n — refusing to report a time" >&2
    return 1
  fi
  echo "  bytes=$n: sink confirmed ${got} bytes" >&2

  printf '%s' "$out" | sed -n 's/.*mean_ms=\([0-9.]*\).*/\1/p' | tail -1
}

echo "Payload gate: ${SMALL} vs ${BIG} bytes (expect ~${FACTOR}x, +/-${TOL}%)"
a=$(run_bytes "$SMALL") || exit 1
b=$(run_bytes "$BIG") || exit 1
docker compose -f "$COMPOSE" down -v >/dev/null 2>&1

if [ -z "$a" ] || [ -z "$b" ]; then
  echo "FAIL: could not parse mean_ms (small=${a:-?} big=${b:-?})"
  exit 1
fi

awk -v a="$a" -v b="$b" -v f="$FACTOR" -v tol="$TOL" '
BEGIN {
  r = b / a
  lo = f * (1 - tol/100); hi = f * (1 + tol/100)
  printf "small=%.1fms  big=%.1fms  ratio=%.2f  want %.2f..%.2f\n", a, b, r, lo, hi
  if (r >= lo && r <= hi) { print "PASS: elapsed tracks payload; the benchmark is timing the transfer"; exit 0 }
  print "FAIL: elapsed does not track payload. The timer is not attached to the"
  print "      transfer, so no congestion-control number from this harness means anything."
  exit 1
}'
