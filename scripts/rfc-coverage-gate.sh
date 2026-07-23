#!/usr/bin/env bash
set -euo pipefail
RFC="${1:?rfc.txt}"

fail=0

# The tags this gate requires. RFC2616 covers HTTP/1.1 and is obsolete
# (superseded by RFC 9110/9112); new HTTP/1.1 conformance tests are keyed to
# those. The tag stays because it guards the nine tests that predate that
# decision -- without it the whole pre-9112 HTTP/1.1 suite could be deleted and
# this gate would still pass.
TAGS="RFC2616 RFC7540 RFC7541 RFC7838 RFC8441 RFC9000 RFC9001 RFC9002 RFC9110 RFC9112 RFC9204 RFC9114"

for tag in $TAGS; do
  if ! grep -E "^--- PASS: TestConformance_${tag}" "$RFC" >/dev/null; then
    echo "No ${tag} conformance tests passed"
    fail=1
  fi
done

if grep -E '^--- FAIL: TestConformance_' "$RFC" >/dev/null; then
  echo "Conformance test failures present"
  fail=1
fi

# Every conformance suite in the tree must be named in TAGS above.
#
# The list is hand-maintained, so it drifts: a new RFC's tests get written, the
# tag is not added, and that entire suite becomes deletable with the gate still
# green. That is not hypothetical -- RFC2616 was ungated until #249, and RFC8441
# was ungated until this ran. Both were found by someone looking, which is the
# thing a gate exists to replace.
#
# So the gate now checks itself against the source: an untagged suite is a
# failure, not something to notice later.
#
# `|| true` on the grep because a tree with no conformance tests at all should
# fall through to the per-tag checks above (which will fail loudly), rather than
# aborting here under `set -e` with a confusing message.
present=$(grep -rhoE "func TestConformance_(RFC[0-9]+)" --include='*_test.go' . 2>/dev/null \
  | sed -E 's/func TestConformance_//' | sort -u || true)
for tag in $present; do
  case " $TAGS " in
    *" $tag "*) ;;
    *)
      echo "TestConformance_${tag}_* tests exist but ${tag} is not in this gate's tag list;"
      echo "  the whole ${tag} suite could be deleted and this gate would still pass."
      echo "  Add ${tag} to TAGS in $0."
      fail=1
      ;;
  esac
done

if [ "$fail" -ne 0 ]; then
  exit 1
fi
echo "RFC coverage gate OK"
