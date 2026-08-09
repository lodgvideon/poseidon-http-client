#!/usr/bin/env bash
# Congestion-control A/B matrix (#362): NewReno vs BBR over loss x RTT.
#
# Runs the same upload in both arms of every cell and prints one row per
# (cell, controller). The measured transfer is an UPLOAD because congestion
# control governs the sender — a download would measure the server's controller.
#
# Usage:  scripts/cc-matrix.sh [bytes] [repeats]
# Needs docker; run it from a shell where `docker compose` works (on Windows,
# from WSL — see docs and the h3-interop make targets).
set -uo pipefail

BYTES="${1:-1048576}"
REPEATS="${2:-3}"
COMPOSE="test/integration/http3/docker-compose.cc.yml"

# cells: LOSS_PCT:DELAY_MS (DELAY_MS is one-way, so RTT is twice it)
#
# READ THIS BEFORE DRAWING A CONCLUSION FROM THE OUTPUT.
#
# These cells cannot settle #362. On a link with no bandwidth limit, both
# controllers finish in a single round trip once their window exceeds the
# transfer, so every cell here bottoms out at the RTT floor and the two arms tie
# — measured, not assumed. BBR's actual claim is about a DEEP QUEUE: a loss-based
# controller fills it and pays the standing delay, a model-based one holds near
# the bandwidth-delay product. Nothing in this harness can produce that, so the
# scenario BBR exists for never occurs.
#
# A token-bucket bottleneck was written for exactly this and REMOVED: it did not
# bind. Changing the configured rate 10x moved completion time 2x, and 1 MiB at a
# nominal 1 Mbps completed in 109 ms instead of the arithmetic 8.4 s. It produced
# a confident-looking "BBR is 36% faster in bufferbloat" that was an artefact.
# Whoever finishes this needs a rate limiter validated by the ratio test first:
# halve the rate, and completion time must double.
CELLS=(
  "0:0"     # loopback baseline
  "0:25"    # 50 ms RTT, clean
  "2:25"    # 50 ms RTT, 2% loss
  "5:25"    # 50 ms RTT, 5% loss
  "2:100"   # 200 ms RTT, 2% loss
)

printf '%-8s %-8s %-9s %9s %9s %11s %8s\n' loss rtt_ms cc mean_ms best_ms mean_MiBps drops
echo "-------------------------------------------------------------------------"

for cell in "${CELLS[@]}"; do
  loss="${cell%%:*}"
  delay="${cell##*:}"
  for cc in newreno bbr; do
    # Tear the stack down between every arm. `docker compose run` reuses an
    # already-running dependency, so without this the lossproxy from the
    # previous cell survives with the PREVIOUS LOSS_PCT. That is not a
    # hypothetical: the first version of this script reported 0%, 2% and 5%
    # loss as identical ~51 ms, because every one of them actually ran at 0%.
    # An isolated 5% run of the same cell takes 205 ms.
    docker compose -f "$COMPOSE" down -v >/dev/null 2>&1

    out=$(LOSS_PCT="$loss" DELAY_MS="$delay" \
          H3_INTEROP_CC="$cc" H3_CC_BYTES="$BYTES" H3_CC_REPEATS="$REPEATS" \
          docker compose -f "$COMPOSE" run --rm runner 2>&1)
    # Prove the configured fault was actually injected in THIS run.
    drops=$(docker compose -f "$COMPOSE" logs lossproxy 2>/dev/null \
            | grep -o 'dropped=[0-9]*' | sed 's/dropped=//' | sort -n | tail -1)
    drops="${drops:-0}"
    line=$(printf '%s\n' "$out" | grep -o 'CCSUMMARY .*' | tail -1)
    if [ -z "$line" ]; then
      printf '%-8s %-8s %-9s %9s %9s %11s %8s\n' "$loss%" "$((delay*2))" "$cc" FAILED - - "$drops"
      printf '%s\n' "$out" | tail -5 | sed 's/^/    /'
      continue
    fi
    mean=$(printf '%s\n' "$line" | sed -n 's/.*mean_ms=\([0-9.]*\).*/\1/p')
    best=$(printf '%s\n' "$line" | sed -n 's/.*best_ms=\([0-9.]*\).*/\1/p')
    tput=$(printf '%s\n' "$line" | sed -n 's/.*mean_mibps=\([0-9.]*\).*/\1/p')
    printf '%-8s %-8s %-9s %9s %9s %11s %8s\n' "$loss%" "$((delay*2))" "$cc" "$mean" "$best" "$tput" "$drops"
    if [ "$loss" != "0" ] && [ "$drops" = "0" ]; then
      echo "    WARNING: $loss% loss configured but the proxy dropped nothing — this row is not the cell it claims"
    fi

    # A delay-queue overflow silently adds loss on top of the configured amount,
    # so a run that hit it is not the cell it claims to be.
    if printf '%s\n' "$out" | grep -q 'DELAY QUEUE OVERFLOW'; then
      echo "    WARNING: delay queue overflowed — this row's loss is not $loss%"
    fi
  done
done

docker compose -f "$COMPOSE" down -v >/dev/null 2>&1 || true
