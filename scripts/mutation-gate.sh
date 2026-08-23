#!/usr/bin/env bash
# Runs a Gremlins invocation and separates its two meanings of exit 10.
#
# Usage: mutation-gate.sh <gremlins command...>
#
# Gremlins exits 10 when efficacy — KILLED / (KILLED + LIVED) — is at or below
# the floor in .gremlins.yaml. That is the verdict the diff gate wants, with one
# input class it cannot express: a diff whose mutable Go files contain no mutable
# LINE. Then KILLED and LIVED are both zero, 0/0 scores as 0.00%, and the job
# fails with a message that reads exactly like "mutants in your diff survived".
# Measured on #903, whose only non-test change was four `const` declarations and
# four `case X: return "STRING"` arms — no Gremlins operator applies to either:
#
#   Killed: 0, Lived: 0, Not covered: 0
#   Test efficacy: 0.00%
#   ERROR: below efficacy-threshold
#
# mutation.yml already drops the case where the diff touches no mutable FILE, and
# that step stays: it saves a 30-second coverage run, and it is what keeps the
# shallow-clone hole closed (a diff Gremlins cannot compute is an empty mutant
# set, which exits 0 — the job would pass while testing nothing). This script
# handles the case that filter cannot see, because the file IS mutable and its
# changed lines are not.
#
# Two rules keep this from becoming a gate that cannot fail:
#
#   1. Only exit 10 is ever reinterpreted. Any other non-zero status is passed
#      through untouched — a crashed run, a bad flag, a missing image must stay
#      red.
#   2. The summary line has to be there. If Gremlins exits 10 and this script
#      cannot find `Killed: N, Lived: N`, it refuses to guess and keeps the
#      failure. An output format change must break the build loudly rather than
#      silently turn every run into a pass.
#
# Note KILLED+LIVED == 0 with NOT COVERED > 0 also passes here, and that is
# deliberate: efficacy is still 0/0, and .gremlins.yaml documents why
# threshold.mutant-coverage is left unset. Covering those lines is a review
# comment, not a merge blocker.
set -uo pipefail

if [ "$#" -eq 0 ]; then
  echo "mutation-gate.sh: a Gremlins command is required" >&2
  exit 2
fi

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

"$@" 2>&1 | tee "$log"
rc=${PIPESTATUS[0]}

# Success, or a failure that is not the efficacy floor: nothing to interpret.
[ "$rc" -eq 0 ] && exit 0
[ "$rc" -ne 10 ] && exit "$rc"

summary=$(grep -E '^Killed: [0-9]+, Lived: [0-9]+' "$log" | head -1 || true)
if [ -z "$summary" ]; then
  echo "mutation gate: Gremlins exited 10 but printed no 'Killed: N, Lived: N' summary." >&2
  echo "mutation gate: refusing to read that as an empty mutant set — keeping the failure." >&2
  exit 10
fi

killed=$(printf '%s\n' "$summary" | sed -E 's/^Killed: ([0-9]+),.*/\1/')
lived=$(printf '%s\n' "$summary" | sed -E 's/^.*Lived: ([0-9]+).*/\1/')

if ! [ "$killed" -eq "$killed" ] 2>/dev/null || ! [ "$lived" -eq "$lived" ] 2>/dev/null; then
  echo "mutation gate: could not read counts out of: $summary" >&2
  exit 10
fi

if [ "$killed" -eq 0 ] && [ "$lived" -eq 0 ]; then
  echo
  echo "mutation gate: no mutant in this diff was runnable ($summary)."
  echo "mutation gate: efficacy is 0/0, so there is nothing to gate on — passing."
  exit 0
fi

exit 10
