#!/usr/bin/env python3
"""Fail when the three protocol stacks disagree about maxInterimResponses.

`maxInterimResponses = 100` is declared once per stack — conn (HTTP/2), http1,
http3 — and it is a PEER-INPUT bound, not a tuning knob: every 1xx block is
decoded and handed to the caller, so an unbounded run is peer-driven work with
no natural end. A stack whose bound silently drifts upward is the stack a
hostile peer gets more work out of.

Each package already carries a tripwire test (TestInterimCap_MatchesSiblings)
pinning its own constant to its own literal, `agreedAcrossProtocols`. Those are
not redundant with this script and must not be deleted: they run under a bare
`go test ./...` where no CI script does, and they pin the VALUE 100, whereas a
script comparing the three to each other stays green if all three drift
together.

What they cannot catch is the hole this script closes. Each test pins its own
package's constant to its own package's literal, so a COORDINATED SINGLE-PACKAGE
edit — the constant plus that package's own tripwire — diverges from the other
two with a fully green suite. So this script checks all SIX numbers together.

(#923 says "There is no test" and "Change one and nothing fails". Both are
false: `go test -overlay` with one declaration changed 100 -> 101 turns exactly
that package's test red. The coordinated edit above is the real gap, and it is
the one worth quoting.)
"""

import argparse
import os
import re
import sys

# CRLF-tolerant by construction: \s matches \r, and the files in this repo are
# checked out with CRLF on Windows. A regex anchored on \n silently matches
# nothing there, and a gate that matches nothing passes.
DECL = re.compile(r"\b(maxInterimResponses|agreedAcrossProtocols)\s*=\s*(\d+)")

# Both names must appear exactly this many times. Without the count assertion a
# rename leaving one site makes "all values agree" vacuously true, and the gate
# is green forever while gating nothing — the failure mode #916 was opened for.
WANT = {"maxInterimResponses": 3, "agreedAcrossProtocols": 3}


def discover_go_files(root):
    """Every .go file in the tree.

    Discovered by walking rather than from a hand-kept package list, for the
    reason scripts/rfc-quote-check.py gives for the same choice: a path list a
    human keeps in sync with the package layout is exactly the kind of thing a
    gate like this exists to stop trusting. `.claude` is skipped because it
    holds worktrees — dozens of checkouts of this same repo.
    """
    skip = {".git", "vendor", "node_modules", "website"}
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in skip and not d.startswith(".")]
        for f in filenames:
            if f.endswith(".go"):
                out.append(os.path.join(dirpath, f))
    return sorted(out)


def collect(root):
    sites = {name: [] for name in WANT}
    for path in discover_go_files(root):
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for lineno, line in enumerate(fh, 1):
                m = DECL.search(line)
                if m:
                    rel = os.path.relpath(path, root).replace(os.sep, "/")
                    sites[m.group(1)].append((rel, lineno, int(m.group(2))))
    return sites


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--root",
        default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        help="repo root to scan (a copy, when proving this gate can fail)",
    )
    args = ap.parse_args()

    sites = collect(args.root)
    failed = False

    for name, want in WANT.items():
        found = sites[name]
        for rel, lineno, value in found:
            print(f"{rel}:{lineno} {name} = {value}")
        if len(found) != want:
            print(
                f"\ninterim-cap gate: found {len(found)} declarations of {name}, want {want}. "
                f"A site was renamed, added or removed; with fewer than {want} this gate "
                f"compares numbers that no longer describe all three stacks.",
                file=sys.stderr,
            )
            failed = True

    values = {v for found in sites.values() for (_, _, v) in found}
    if len(values) > 1:
        print(
            "\ninterim-cap gate: the three protocol stacks disagree about the "
            "interim-response bound. Every site above must carry the same number - it is "
            "one peer-input bound for one concept, and a stack that drifts upward is the "
            "stack a hostile peer gets more work out of.",
            file=sys.stderr,
        )
        failed = True

    if failed:
        return 1

    total = sum(len(v) for v in sites.values())
    only = values.pop() if values else "?"
    print(f"\ninterim-cap gate: {total} sites, all agree at {only}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
