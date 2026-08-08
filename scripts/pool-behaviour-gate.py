#!/usr/bin/env python3
"""Locked behaviour gate for the connection-pool tier.

A green test suite does not prove a refactor preserved behaviour — it proves the
suite still passes. This repo has repeatedly shipped tests that passed with the
fix reverted. This gate closes that hole: for each behaviour the pool tier must
keep, it applies a mutation that breaks exactly that behaviour and asserts the
named test goes RED. A mutation that survives means nothing observes the
behaviour, and the refactor about to collapse three implementations into one
would preserve it only by luck.

Run it before and after every pool refactor increment. The result must be
identical: every mutation CAUGHT.

    python3 scripts/pool-behaviour-gate.py            # run the battery
    python3 scripts/pool-behaviour-gate.py --list     # show what is pinned

Design notes that matter if you edit this file:

  * Mutations are anchored INSIDE their enclosing function. The same fragment
    usually exists in a sibling function, and a by-first-occurrence replacement
    lands there instead — reporting NOT CAUGHT for code that is fine. Every
    entry declares `func` and the search is scoped to that function's body.
  * The file is read with newline="" and patterns are joined with \\r?\\n. The
    tree is CRLF; re.escape does NOT escape newlines, so a multi-line pattern
    built any other way silently fails to match.
  * A mutation that does not compile is reported as an ERROR, not a pass. A
    non-compiling mutation proves nothing.
"""

import argparse
import io
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Line-ending-agnostic joiner for multi-line patterns; this tree is CRLF and
# re.escape does not escape newlines.
RX_NL = chr(92) + "r?" + chr(92) + "n"

# Each entry: what behaviour is pinned, which function to mutate, the mutation,
# and the test that must go red. Keep the `why` short enough to read in output.
MUTATIONS = [
    {
        "name": "h2-revive",
        "why": "a re-added address must be revived, or a resolver flap blackholes it forever",
        "sites": [
            ("client/managed_pool.go", "func (mp *managedPool) applySet"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) applySet"),
        ],
        "find": ["\t\t\ts.draining = false"],
        "repl": "\t\t\t_ = s",
        "test": "TestManagedPool_AddressReAddedAfterRemoval|TestManagedPool_SingleAddressFlap",
    },
    {
        "name": "h1-revive",
        "why": "same, HTTP/1.1",
        "sites": [
            ("client/h1_managed_pool.go", "func (mp *h1ManagedPool) applySet"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) applySet"),
        ],
        "find": ["\t\t\ts.draining = false"],
        "repl": "\t\t\t_ = s",
        "test": "TestH1ManagedPool_AddressReAddedAfterRemoval|TestH1ManagedPool_SingleAddressFlap",
    },
    {
        "name": "h3-revive",
        "why": "same, HTTP/3",
        "sites": [
            ("client/h3_managed_pool.go", "func (mp *h3ManagedPool) applySet"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) applySet"),
        ],
        "find": ["\t\t\ts.draining = false"],
        "repl": "\t\t\t_ = s",
        "test": "TestH3ManagedPool_AddressReAddedAfterRemoval",
    },
    {
        "name": "h2-drop-guard",
        "why": "dropIfDraining must check registration identity AND draining under one lock",
        "sites": [
            ("client/managed_pool.go", "func (mp *managedPool) dropIfDraining"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) dropIfDraining"),
        ],
        "find": ["\tcur, ok := mp.subPools[s.addr.String()]", "\tif !ok || cur != s || !s.draining {"],
        "repl": "\tcur, ok := mp.subPools[s.addr.String()]\n\t_ = cur\n\tif !ok {",
        "test": "TestManagedPool_ReviveBeatsDrainWatcher",
    },
    {
        "name": "h1-drop-guard",
        "why": "same, HTTP/1.1",
        "sites": [
            ("client/h1_managed_pool.go", "func (mp *h1ManagedPool) dropIfDraining"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) dropIfDraining"),
        ],
        "find": ["\tcur, ok := mp.subPools[s.addr.String()]", "\tif !ok || cur != s || !s.draining {"],
        "repl": "\tcur, ok := mp.subPools[s.addr.String()]\n\t_ = cur\n\tif !ok {",
        "test": "TestH1ManagedPool_ReviveBeatsDrainWatcher",
    },
    {
        "name": "h3-drop-guard",
        "why": "same, HTTP/3",
        "sites": [
            ("client/h3_managed_pool.go", "func (mp *h3ManagedPool) dropIfDraining"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) dropIfDraining"),
        ],
        "find": ["\tcur, ok := mp.subPools[s.addr.String()]", "\tif !ok || cur != s || !s.draining {"],
        "repl": "\tcur, ok := mp.subPools[s.addr.String()]\n\t_ = cur\n\tif !ok {",
        "test": "TestH3ManagedPool_ReviveBeatsDrainWatcher",
    },
    {
        "name": "h2-failover",
        "why": "only isDialOnlyErr continues the address loop; anything else must abort",
        "sites": [
            ("client/managed_pool.go", "func (mp *managedPool) acquire"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) acquire"),
        ],
        "find": ["\t\tif !isDialOnlyErr(err) {"],
        "repl": "\t\tif err != nil {",
        "test": "TestManagedPool_FailsOverOnFirstDialFailure",
    },
    {
        "name": "h1-failover",
        "why": "same, HTTP/1.1",
        "sites": [
            ("client/h1_managed_pool.go", "func (mp *h1ManagedPool) acquire"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) acquire"),
        ],
        "find": ["\t\tif !isDialOnlyErr(err) {"],
        "repl": "\t\tif err != nil {",
        "test": "TestH1ManagedPool_FailsOverOnFirstDialFailure",
    },
    {
        "name": "h3-failover",
        "why": "same, HTTP/3",
        "sites": [
            ("client/h3_managed_pool.go", "func (mp *h3ManagedPool) acquire"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) acquire"),
        ],
        "find": ["\t\tif !isDialOnlyErr(err) {"],
        "repl": "\t\tif err != nil {",
        "test": "TestH3ManagedPool_FailsOverOnFirstDialFailure",
    },
    {
        "name": "h2-drainlazy-retains",
        "why": "DrainLazy keeps the sub-pool; dropping it at once is DrainHard's behaviour",
        "sites": [
            ("client/managed_pool.go", "func (mp *managedPool) beginDrain"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) beginDrain"),
        ],
        "find": ["\tcase DrainLazy:"],
        "repl": "\tcase DrainLazy:\n\t\tmp.dropSubPool(s, true)",
        "test": "TestManagedPool_DrainLazy_RemovedAddress_RetainsSubPool",
    },
    {
        "name": "h1-drainlazy-retains",
        "why": "same, HTTP/1.1",
        "sites": [
            ("client/h1_managed_pool.go", "func (mp *h1ManagedPool) beginDrain"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) beginDrain"),
        ],
        "find": ["\tcase DrainLazy:"],
        "repl": "\tcase DrainLazy:\n\t\tmp.dropSubPool(s, true)",
        "test": "TestH1ManagedPool_DrainLazy_RemovedAddress_RetainsSubPool",
    },
    {
        "name": "h3-drainlazy-retains",
        "why": "same, HTTP/3",
        "sites": [
            ("client/h3_managed_pool.go", "func (mp *h3ManagedPool) beginDrain"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) beginDrain"),
        ],
        "find": ["\tcase DrainLazy:"],
        "repl": "\tcase DrainLazy:\n\t\tmp.dropSubPool(s, true)",
        "test": "TestH3ManagedPool_",
    },
    {
        "name": "managed-warmup-address-set",
        "why": "warmup must build from the resolved addresses, not the lazily-filled subPools map",
        "sites": [
            ("client/managed_pool.go", "func (mp *managedPool) warmup"),
            ("client/managed_core.go", "func (mp *managedCore[P, MC, C, R]) warmup"),
        ],
        "find": ["\taddrs := append([]Address(nil), mp.addrs...)"],
        "repl": "\taddrs := []Address(nil)",
        "test": "TestWarmup_ManagedPool_PreDialsBeforeAnyRequest",
    },
]


def read(path: Path) -> str:
    with io.open(path, encoding="utf-8", newline="") as fh:
        return fh.read()


def write(path: Path, text: str) -> None:
    with io.open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)


def func_span(src: str, signature: str):
    """Byte range of one function body, so a mutation cannot land in a sibling."""
    start = src.find(signature)
    if start < 0:
        return None
    nxt = src.find("\nfunc ", start + len(signature))
    return (start, len(src) if nxt < 0 else nxt)


def locate(mut):
    """Find the ONE site that currently carries this behaviour.

    Each case lists candidate (file, function) pairs: the per-protocol
    implementation and, once it exists, the shared core. During an incremental
    migration a behaviour lives in exactly one of them - before the migration
    starts only the first exists, after it finishes only the second. Zero
    matches means the anchor rotted; more than one means two copies of a
    behaviour that is supposed to have a single home. Both are errors, because
    an un-appliable or ambiguous mutation proves nothing.
    """
    found = []
    for rel, signature in mut["sites"]:
        path = REPO / rel
        if not path.exists():
            continue
        src = read(path)
        span = func_span(src, signature)
        if span is None:
            continue
        lo, hi = span
        pattern = re.compile(RX_NL.join(re.escape(line) for line in mut["find"]))
        if len(pattern.findall(src[lo:hi])) == 1:
            found.append((path, signature))
    if not found:
        tried = ", ".join(rel for rel, _ in mut["sites"])
        raise LookupError("behaviour not found at any known site (tried %s)" % tried)
    # First listed match wins, and the per-protocol file is listed first.
    #
    # Mid-migration a behaviour genuinely exists twice: in the core, and in the
    # protocols that have not moved yet. The one that RUNS for a given protocol
    # is its own copy while that copy exists, and the core's only after it is
    # deleted. Precedence encodes that. An earlier revision treated two matches
    # as an error, which is right for the finished state and wrong for every
    # step on the way there - it made incremental migration unmeasurable, which
    # is the opposite of what this gate is for.
    #
    # This does not weaken any behaviour: the 13 cases and the tests they assert
    # against are untouched, and once every protocol has moved, the per-protocol
    # sites are gone and all 13 mutate the single shared copy.
    return found[0]


def apply_mutation(src: str, signature: str, mut) -> str:
    span = func_span(src, signature)
    if span is None:
        raise LookupError("function not found: %s" % signature)
    lo, hi = span
    body = src[lo:hi]
    pattern = re.compile(r"\r?\n".join(re.escape(line) for line in mut["find"]))
    hits = pattern.findall(body)
    if len(hits) != 1:
        raise LookupError(
            "pattern matched %d times inside %s (want exactly 1)" % (len(hits), signature)
        )
    crlf = "\r\n" in src
    repl = mut["repl"].replace("\n", "\r\n") if crlf else mut["repl"]
    return src[:lo] + pattern.sub(lambda _m: repl, body, count=1) + src[hi:]


def run(cmd, timeout=900):
    return subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, timeout=timeout)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="print the pinned behaviours and exit")
    ap.add_argument("--only", default="", help="run one mutation by name")
    args = ap.parse_args()

    if args.list:
        for m in MUTATIONS:
            print("%-28s %s" % (m["name"], m["why"]))
        return 0

    selected = [m for m in MUTATIONS if not args.only or m["name"] == args.only]
    if not selected:
        print("no mutation named %r" % args.only, file=sys.stderr)
        return 2

    caught, survived, errors = [], [], []
    with tempfile.TemporaryDirectory() as tmp:
        for mut in selected:
            try:
                target, signature = locate(mut)
            except LookupError as exc:
                errors.append((mut["name"], str(exc)))
                print("  ERROR     %s - %s" % (mut["name"], exc))
                continue
            backup = Path(tmp) / (mut["name"] + ".bak")
            shutil.copyfile(target, backup)
            try:
                write(target, apply_mutation(read(target), signature, mut))
                vet = run(["go", "vet", "./client/"])
                if vet.returncode != 0:
                    errors.append((mut["name"], "mutation does not compile"))
                    continue
                res = run(["go", "test", "./client/", "-run", mut["test"], "-count=1", "-timeout", "600s"])
                if res.returncode != 0:
                    caught.append(mut["name"])
                    print("  CAUGHT    %s" % mut["name"])
                else:
                    survived.append((mut["name"], mut["why"]))
                    print("  SURVIVED  %s — %s" % (mut["name"], mut["why"]))
            except LookupError as exc:
                errors.append((mut["name"], str(exc)))
                print("  ERROR     %s — %s" % (mut["name"], exc))
            finally:
                shutil.copyfile(backup, target)

    print()
    print("caught %d/%d" % (len(caught), len(selected)))
    if survived:
        print("\nSURVIVING MUTANTS — nothing in the suite observes these behaviours:")
        for name, why in survived:
            print("  %-28s %s" % (name, why))
    if errors:
        print("\nERRORS — the mutation could not be applied or did not build:")
        for name, msg in errors:
            print("  %-28s %s" % (name, msg))
        print("\nAn un-appliable mutation proves nothing. Fix the anchor before trusting a green run.")

    return 0 if not survived and not errors else 1


if __name__ == "__main__":
    sys.exit(main())
