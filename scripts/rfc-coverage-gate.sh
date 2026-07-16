#!/usr/bin/env bash
set -euo pipefail
RFC="${1:?rfc.txt}"

fail=0
# RFC2616 covers HTTP/1.1. It is obsolete (superseded by RFC 9110/9112), and new
# HTTP/1.1 conformance tests are keyed to those; this tag guards the nine tests
# that predate that decision. Without it the whole HTTP/1.1 suite could be
# deleted and this gate would still pass.
for tag in RFC2616 RFC7540 RFC7541 RFC9000 RFC9001 RFC9002 RFC9204 RFC9114; do
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
