#!/usr/bin/env bash
set -euo pipefail
RFC="${1:?rfc.txt}"

fail=0
for tag in RFC7540 RFC7541; do
  if ! grep -E "^--- PASS: TestConformance_${tag}" "$RFC" >/dev/null; then
    echo "No ${tag} conformance tests passed"
    fail=1
  fi
done

if grep -E '^--- FAIL: TestConformance_' "$RFC" >/dev/null; then
  echo "Conformance test failures present"
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi
echo "RFC coverage gate OK"
