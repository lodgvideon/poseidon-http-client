#!/usr/bin/env python3
"""Compare an observed quic-interop-runner matrix against the declared partition.

The gate is not "the matrix reported no failures". It is "the matrix reported
exactly what .github/interop/expected.json says it would", asserted in both
directions:

  * a cell declared `succeeded` that comes back `unsupported` or `failed` is a
    capability regression - and `unsupported` is the one a plain no-failures
    check cannot see, because the runner records exit 127 as a non-failure;
  * a cell declared `unsupported` that comes back `succeeded` means the exit-127
    table in test/interop/quic/main.go now understates the client. Left
    unasserted we would keep telling the world we cannot do something we can;
  * a cell declared `failed` that comes back `succeeded` is the same shape of
    stale claim, one step further along.

There is deliberately no retry and no tolerance: a mismatch is a code change
plus one edit to expected.json, never a re-run.

A cell's expectation is per server, because the partition genuinely is. Whether
a rebind or a migration cell can pass depends on how strict the peer is, and a
single global answer would either be wrong for one server or force us to weaken
the assertion for both. `expect` is therefore either one result for every
server or an object keyed by server name.

A cell may also carry NO `expect`. That declares it deliberately not asserted -
today only where the outcome is probabilistic rather than a property of the
code. Such a cell is still printed with whatever it actually did, so it is
visible on every run; it just does not decide the verdict. This is the only
tolerance in the file, it is per named cell, and it requires a `why`.

Two silent-pass modes are closed explicitly, because both are reachable:

  * run.py returns the FAILED count as its exit status, and returns 0 when the
    compliance check rejected an endpoint and every pairing was skipped. That
    run writes a result file whose every cell is JSON null, so a null is a hard
    error here rather than a missing key.
  * interop.py::_postprocess_results rewrites FAILED to UNSUPPORTED for a client
    that failed a test against *every* server. One server per result file keeps
    that branch unreachable (it needs len(servers) > 1), and the one-pair check
    below is what enforces that, so an aggregated file cannot slip past.
"""

import argparse
import json
import os
import sys

# The three values of result.py::TestResult. Anything else means the runner
# changed under the pinned commit.
VALID_RESULTS = ("succeeded", "failed", "unsupported")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--expected",
        required=True,
        help="path to expected.json (the declared partition)",
    )
    p.add_argument(
        "results",
        nargs="+",
        help="runner --json output files, one per server",
    )
    p.add_argument(
        "--only",
        default="",
        help="comma-separated matrix cells to assert; default is every cell "
        "that declares an expectation",
    )
    return p.parse_args()


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def expectation(case, server):
    """The declared result for this cell against this server, or None.

    None means the cell is deliberately not asserted - see the module docstring.
    A per-server object that is missing this server is an error, not a None:
    silently not asserting a cell because someone added a server is the failure
    mode this whole file exists to avoid.
    """
    want = case.get("expect")
    if want is None:
        return None
    if isinstance(want, str):
        return want
    if server not in want:
        raise KeyError(f"expect has no entry for server {server!r}")
    return want[server]


def observed_pair(result, path):
    """Return (client, server, {cell: result}) for a single-pair result file.

    Rejects anything but exactly one client and one server. _export_results
    orders its rows by iterating list(set(clients)) x list(set(servers)), so
    with more than one of either the row-to-server mapping depends on set
    iteration order; refusing the input is better than guessing it.
    """
    clients = result.get("clients") or []
    servers = result.get("servers") or []
    if len(clients) != 1 or len(servers) != 1:
        raise ValueError(
            f"{path}: expected exactly one client and one server, "
            f"got clients={clients} servers={servers}"
        )
    rows = result.get("results") or []
    if len(rows) != 1:
        raise ValueError(f"{path}: expected exactly one result row, got {len(rows)}")
    cells = {}
    for cell in rows[0]:
        name = cell.get("name")
        if name is None:
            raise ValueError(f"{path}: a result cell has no name: {cell!r}")
        if name in cells:
            raise ValueError(f"{path}: duplicate result for {name}")
        cells[name] = cell.get("result")
    if not cells:
        raise ValueError(f"{path}: the result row is empty - nothing ran")
    return clients[0], servers[0], cells


def check_pair(expected, path, only):
    """Assert one server's observed cells against the declared partition.

    Returns a list of human-readable problems; empty means this server passed.
    """
    result = load_json(path)
    client, server, cells = observed_pair(result, path)

    problems = []
    declared_client = expected["client"]["name"]
    if client != declared_client:
        problems.append(f"client is {client!r}, expected {declared_client!r}")
    if server not in expected["servers"]:
        # Return here rather than carrying on: every per-server expectation
        # below would raise for this server, and the resulting KeyError would
        # bury the one line that says what actually went wrong.
        problems.append(f"server {server!r} is not one of the declared servers")
        return problems

    cases = expected["cases"]

    # Any cell the runner produced that nobody declared at all. Not reachable
    # while the runner is pinned, which is exactly why it is worth catching:
    # it means the pin moved.
    undeclared = sorted(set(cells) - set(cases))
    for name in undeclared:
        problems.append(f"{name}: the runner ran it but expected.json never mentions it")

    if only:
        selected = []
        for name in only:
            if name not in cases:
                problems.append(f"--only names an undeclared cell: {name}")
                continue
            if expectation(cases[name], server) is None:
                problems.append(f"--only names {name}, which declares no expectation")
                continue
            selected.append(name)
    else:
        selected = [
            n for n in cases if expectation(cases[n], server) is not None
        ]

    for name in sorted(set(selected) - set(cells)):
        problems.append(f"{name}: declared but the runner produced no result for it")
    if only:
        # The short leg asks the runner for exactly the --only set, so anything
        # else in the file means the two disagree about what ran.
        for name in sorted(set(cells) - set(selected) - set(undeclared)):
            problems.append(f"{name}: ran but is outside the --only set")

    print(f"\n=== {server} vs {client} ({os.path.basename(path)}) ===")
    print(f"{'cell':<22} {'expected':<12} {'observed':<12} verdict")

    reported = set(selected) | (set() if only else set(cases))
    for name in sorted(reported):
        want = expectation(cases[name], server) if name in cases else None
        got = cells.get(name, "(not run)")
        if want is None:
            # Declared, deliberately not asserted. Printed so it is never
            # invisible; the `why` in expected.json says what it is waiting on.
            print(f"{name:<22} {'-':<12} {str(got):<12} not asserted")
            continue
        if got is None:
            verdict = "NULL - the pairing never ran (compliance check?)"
            problems.append(f"{name}: result is null, the pairing was skipped")
        elif got == "(not run)":
            verdict = "MISSING"
        elif got not in VALID_RESULTS:
            verdict = f"UNKNOWN result {got!r}"
            problems.append(f"{name}: unknown result {got!r}")
        elif got != want:
            verdict = "MISMATCH"
            problems.append(f"{name}: expected {want}, observed {got}")
        else:
            verdict = "ok"
        print(f"{name:<22} {want:<12} {str(got):<12} {verdict}")

    return problems


def main():
    args = parse_args()
    expected = load_json(args.expected)
    only = [t.strip() for t in args.only.split(",") if t.strip()]

    failures = {}
    for path in args.results:
        try:
            problems = check_pair(expected, path, only)
        except (OSError, ValueError, KeyError) as exc:
            problems = [str(exc)]
            print(f"\n=== {os.path.basename(path)} ===\n{exc}")
        if problems:
            failures[path] = problems

    # Both spellings of "not asserted": the whole cell, or one server inside a
    # per-server object. Printed every run so the tolerance is never invisible.
    def has_null(case):
        want = case.get("expect")
        if want is None:
            return True
        return isinstance(want, dict) and any(v is None for v in want.values())

    unasserted = sorted(n for n, c in expected["cases"].items() if has_null(c))
    if unasserted:
        print("\nDeclared but not asserted, with the reason in expected.json:")
        for name in unasserted:
            print(f"  {name}: {expected['cases'][name]['why']}")

    if failures:
        print("\nFAIL: the observed matrix does not match the declared partition.")
        for path, problems in failures.items():
            for problem in problems:
                print(f"  {os.path.basename(path)}: {problem}")
        print(
            "\nDo not re-run. Either the client changed and expected.json has to "
            "follow, or this is the regression the gate exists to catch."
        )
        return 1

    print(
        f"\nPASS: {len(args.results)} server(s) matched the declared partition "
        f"cell for cell."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
