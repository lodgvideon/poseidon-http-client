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
# There is now a bottleneck that binds — kernel `tc tbf` on the relay — and two
# gates that must BOTH pass before any row here is worth reading:
#
#   scripts/cc-scale-check.sh   double the payload, the time doubles
#   scripts/cc-ratio-check.sh   halve the rate, the time doubles
#
# Run them first. A cell whose knob does not respond measures nothing, which is
# the mistake this ticket already made once.
#
# Cells are loss:delay_ms:rate:queue. An empty rate leaves the link unshaped —
# the old behaviour, and still the right baseline. A rate with a deep queue is
# the bufferbloat case BBR exists for and a loss-based controller is supposed to
# lose; the shallow-queue row next to it is that claim's control, because a BBR
# win that shows up at BOTH depths is not a bufferbloat win.
CELLS=(
  "0:0::"             # loopback baseline, unshaped
  "0:25::"            # 50 ms RTT, clean, unshaped
  "2:25::"            # 50 ms RTT, 2% loss
  "5:25::"            # 50 ms RTT, 5% loss
  "2:100::"           # 200 ms RTT, 2% loss
  "0:25:4mbit:500ms"  # bottleneck, DEEP queue — the bufferbloat case
  "0:25:4mbit:50ms"   # same bottleneck, shallow queue — its control
  "2:50:4mbit:500ms"  # bufferbloat plus loss, 100 ms RTT
)

printf '%-6s %-7s %-7s %-6s %-9s %9s %9s %11s %7s\n' \
  loss rtt_ms rate queue cc mean_ms spread_% mean_MiBps drops
echo "--------------------------------------------------------------------------------------"

for cell in "${CELLS[@]}"; do
  IFS=: read -r loss delay rate queue <<<"$cell"
  queue="${queue:-500ms}"
  for cc in newreno bbr; do
    # Tear the stack down between every arm. `docker compose run` reuses an
    # already-running dependency, so without this the lossproxy from the
    # previous cell survives with the PREVIOUS LOSS_PCT. That is not a
    # hypothetical: the first version of this script reported 0%, 2% and 5%
    # loss as identical ~51 ms, because every one of them actually ran at 0%.
    # An isolated 5% run of the same cell takes 205 ms.
    docker compose -f "$COMPOSE" down -v >/dev/null 2>&1

    out=$(LOSS_PCT="$loss" DELAY_MS="$delay" RATE="$rate" QUEUE="$queue" \
          H3_INTEROP_CC="$cc" H3_CC_BYTES="$BYTES" H3_CC_REPEATS="$REPEATS" \
          docker compose -f "$COMPOSE" run --rm runner 2>&1)

    # A configured rate that did not install is the failure mode that produced
    # the artefact this script warns about at the top, so it is checked the same
    # way the loss is: by evidence from the run, not by assuming the env var
    # arrived.
    if [ -n "$rate" ]; then
      shaper=$(docker compose -f "$COMPOSE" logs lossproxy 2>/dev/null |
               grep -o 'SHAPER: .*' | head -1)
      case "$shaper" in
        *"qdisc tbf"*) ;;
        *) echo "    WARNING: rate $rate configured but no tbf qdisc in the relay log (${shaper:-nothing logged}) — this row is unshaped" ;;
      esac
    fi
    # Prove the configured fault was actually injected in THIS run.
    drops=$(docker compose -f "$COMPOSE" logs lossproxy 2>/dev/null \
            | grep -o 'dropped=[0-9]*' | sed 's/dropped=//' | sort -n | tail -1)
    drops="${drops:-0}"
    line=$(printf '%s\n' "$out" | grep -o 'CCSUMMARY .*' | tail -1)
    if [ -z "$line" ]; then
      printf '%-6s %-7s %-7s %-6s %-9s %9s %9s %11s %7s\n' \
        "$loss%" "$((delay*2))" "${rate:-none}" "${rate:+$queue}" "$cc" FAILED - - "$drops"
      printf '%s\n' "$out" | tail -5 | sed 's/^/    /'
      continue
    fi
    mean=$(printf '%s\n' "$line" | sed -n 's/.*mean_ms=\([0-9.]*\).*/\1/p')
    tput=$(printf '%s\n' "$line" | sed -n 's/.*mean_mibps=\([0-9.]*\).*/\1/p')

    # Spread WITHIN this arm, as a percentage of its own mean. Printed next to
    # the mean on purpose: a difference between the two controllers only means
    # something once it is bigger than the noise inside either of them. Reading
    # a between-arm delta without this is how a 9%-noisy cell once produced a
    # confident 12% "regression".
    spread=$(printf '%s\n' "$out" | sed -n 's/.*CCRESULT .* ms=\([0-9.]*\) .*/\1/p' |
      awk 'NR==1{lo=hi=$1} {if($1<lo)lo=$1; if($1>hi)hi=$1; s+=$1; n++}
           END{ if(n<2||s==0) print "n/a"; else printf "%.0f", 100*(hi-lo)/(s/n) }')

    printf '%-6s %-7s %-7s %-6s %-9s %9s %9s %11s %7s\n' \
      "$loss%" "$((delay*2))" "${rate:-none}" "${rate:+$queue}" "$cc" "$mean" "$spread" "$tput" "$drops"
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
