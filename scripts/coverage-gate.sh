#!/usr/bin/env bash
# Per-package coverage gate.
#
# Usage: coverage-gate.sh <test-output> [threshold]
#
# Parses lines like
#   ok  pkg/path  1.5s  coverage: 79.0% of statements
# from `go test -cover ./...` output and fails if any package falls below
# the threshold (default 70 — the current Phase A floor; spec §11 calls
# for ≥ 90 per package, see docs/COVERAGE.md for the ratchet plan).
set -euo pipefail

OUT="${1:?path to go-test output required}"
THRESH="${2:-70}"

if [ ! -s "$OUT" ]; then
  echo "coverage file empty or missing: $OUT"
  exit 1
fi

violations=$(awk -v th="$THRESH" '
  /coverage:/ {
    for (i = 1; i <= NF; i++) {
      if ($i == "coverage:") {
        cov = $(i+1)
        gsub("%", "", cov)
        if (cov + 0 < th + 0) {
          # Find package name (second field for "ok" / "FAIL" lines).
          printf "%s\t%s%% < %s%%\n", $2, cov, th
        }
      }
    }
  }
' "$OUT")

if [ -n "$violations" ]; then
  echo "Coverage gate FAILED — packages below ${THRESH}%:"
  echo "$violations"
  exit 1
fi

echo "Coverage gate OK — all packages ≥ ${THRESH}%"
