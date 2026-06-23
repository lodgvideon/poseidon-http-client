#!/usr/bin/env bash
# coverage-gate.sh — fail CI if any package or the overall total
# statement coverage drops below the supplied threshold.
#
# Usage: ./scripts/coverage-gate.sh [MIN_PERCENT]
#   MIN_PERCENT defaults to 80.
#
# Reads cover.out (produced by `go test -coverprofile=cover.out
# ./...`) and uses `go tool cover -func` for both the total figure
# and the per-package aggregation.

set -euo pipefail

MIN="${1:-80}"
PROFILE="${COVER_PROFILE:-cover.out}"

if [[ ! -f "$PROFILE" ]]; then
  echo "coverage-gate: $PROFILE not found — run 'go test -coverprofile=$PROFILE ./...' first" >&2
  exit 2
fi

# Per-package coverage table from `go test -cover` is the simplest
# authoritative source; we re-run it (cached when nothing changed) so
# we don't need to parse cover.out blocks.
# Exclude examples/ (runnable demos with no tests — doc artifacts, not library
# code) so they don't trip the per-package floor at 0%.
PKG_TABLE=$(go test -cover ./... 2>&1 | grep -E '\bcoverage:' | grep -v '/examples/' || true)

if [[ -z "$PKG_TABLE" ]]; then
  echo "coverage-gate: 'go test -cover ./...' produced no coverage rows" >&2
  exit 2
fi

OVERALL=$(go tool cover -func="$PROFILE" | awk '/^total:/ {print $3}' | tr -d '%')
echo "coverage-gate: total = ${OVERALL}% (threshold ${MIN}%)"

FAIL=0
while IFS= read -r line; do
  # Extract package path and coverage percentage.
  pkg=$(echo "$line" | awk '{print $2}')
  pct=$(echo "$line" | grep -oE 'coverage: [0-9]+\.?[0-9]*%' | grep -oE '[0-9]+\.?[0-9]*')
  if [[ -z "$pct" ]]; then
    continue
  fi
  echo "coverage-gate: $pkg = ${pct}%"
  awk -v p="$pct" -v m="$MIN" 'BEGIN { if (p+0 < m+0) exit 1 }' || {
    echo "coverage-gate: FAIL $pkg coverage ${pct}% < ${MIN}%" >&2
    FAIL=1
  }
done <<< "$PKG_TABLE"

awk -v p="$OVERALL" -v m="$MIN" 'BEGIN { if (p+0 < m+0) exit 1 }' || {
  echo "coverage-gate: FAIL total coverage ${OVERALL}% < ${MIN}%" >&2
  FAIL=1
}

if [[ "$FAIL" -ne 0 ]]; then
  exit 1
fi
echo "coverage-gate: all packages and total ≥ ${MIN}%."
