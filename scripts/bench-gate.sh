#!/usr/bin/env bash
# Parses benchstat -alpha 0.05 output. Fails on non-zero allocs/B per op.
set -euo pipefail
DIFF="${1:?diff.txt path required}"
if grep -E 'allocs/op' "$DIFF" | awk '{for(i=1;i<=NF;i++) if ($i ~ /^[1-9]/) {print "non-zero alloc"; exit 1}}'; then
  exit 1
fi
echo "Bench gate OK"
