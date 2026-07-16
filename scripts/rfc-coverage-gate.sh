#!/usr/bin/env bash
set -euo pipefail
RFC="${1:?rfc.txt}"

fail=0
# RFC2616 covers HTTP/1.1 and is obsolete (superseded by RFC 9110/9112); new
# HTTP/1.1 conformance tests are keyed to those. The tag stays because it guards
# the nine tests that predate that decision -- without it the whole pre-9112
# HTTP/1.1 suite could be deleted and this gate would still pass.
for tag in RFC2616 RFC7540 RFC7541 RFC9000 RFC9001 RFC9002 RFC9110 RFC9112 RFC9204 RFC9114; do
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
