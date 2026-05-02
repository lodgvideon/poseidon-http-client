#!/usr/bin/env bash
set -euo pipefail
RFC="${1:?rfc.txt}"
if ! grep -E 'PASS:.*TestConformance_RFC7541' "$RFC" >/dev/null; then
  echo "No RFC 7541 conformance tests passed"
  exit 1
fi
echo "RFC coverage gate OK"
