#!/usr/bin/env bash
set -euo pipefail
COV="${1:?coverage.txt}"
THRESH="${2:-90}"
total=$(awk '/^total:/ {gsub("%","",$3); print $3}' "$COV")
echo "coverage: ${total}% (threshold: ${THRESH}%)"
awk -v t="$total" -v th="$THRESH" 'BEGIN { if (t+0 < th+0) exit 1 }'
