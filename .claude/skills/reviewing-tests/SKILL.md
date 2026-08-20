---
name: reviewing-tests
description: Use immediately after writing or changing any test, and whenever reviewing existing tests — including the #722 sweep. Covers the structural bar (Arrange–Act–Assert, testify require/assert), whether the test can actually fail, and whether its cases were designed by a functional-testing technique rather than picked by hand.
---

# Reviewing tests

A test earns its place by failing when the thing it covers breaks. Everything below
is downstream of that. Run this after writing tests, not only when someone asks for a
review — the cheapest moment to fix a test is before it has been trusted.

Three passes, in this order. Stop at the first that fails; a test that cannot fail is
not worth restructuring, and a test with the wrong cases is not worth reformatting.

## Pass 1 — Can it fail? (correctness of the test itself)

**Gremlins generates the mutants; you do not.** `make mutation` mutates only what
the diff touched and is the bar a change has to clear — a realistic 133-line
non-test diff is 5 mutants and 80 seconds, coverage run included. `make
mutation-full` is the whole module (4318 runnable mutants), `make mutation-dry`
the inventory alone (~40s). Settings and the exclusion list are in
`.gremlins.yaml`; `.github/workflows/mutation.yml` runs the same targets, gated on
pull requests and unthresholded nightly.

Read the run, do not skim it:

- **`LIVED` and `NOT COVERED` are different findings.** The first says a test
  executes the line and does not care what it does; the second says nothing
  reaches it. Different fixes, and the report keeps them apart — so should the
  write-up.
- **`NOT VIABLE` is a VOID verdict, never a catch.** The mutant did not compile, so
  it proved nothing about any test. Gremlins already excludes it from the
  arithmetic; a summary must not quietly fold it in as a kill.
- **`TIMED OUT` is not a kill you can trust.** The suite failed without saying why.
  Under parallel workers that is usually CPU contention rather than the mutation,
  which is why `.gremlins.yaml` pins `workers: 1` with a generous
  `timeout-coefficient`. Tracing one back to its cause is part of the review.
- **A survivor is either a hole or an equivalent mutant — say which.** If the mutant
  is genuinely unobservable through the public surface, the honest move is to delete
  the test, not to strengthen it into a test of something else.
- **A new test whose mutant an existing test already catches should not be written.**
  Say so and drop it — and check before writing it, not after.
- **A run that mutated nothing is not a pass — and it does not exit 0 either.**
  `Killed: 0` beside `Skipped: 4634` measured nothing; it is this tool's version
  of `0 tests ran`. Gremlins reports `KILLED / (KILLED + LIVED)` over 0/0 as
  `Test efficacy: 0.00%`, which is below any floor, so a diff touching only test
  files **exits 10**. Measured on the gate's own first runs; `mutation.yml` has a
  step that skips the job when the diff holds no mutable Go file, because
  otherwise every docs, workflow, test-only and dependabot pull request fails a
  gate that never ran.

Two ways this tool reports a clean sheet it has not earned, both measured:

- **Run natively on Windows and mutator coverage is 0% for every file in a
  subdirectory** — 171638 mutants, 0 runnable. Upstream bug in
  `internal/coverage/coverage.go`; it is why the `make` targets use Docker.
- **The efficacy floor works only as a key in `.gremlins.yaml`.** The
  `--threshold-efficacy` flag and the `GREMLINS_UNLEASH_*` variable are inert in
  v0.6.0 — 66.67% efficacy against a floor of 80 exits 0 through either, because
  Gremlins type-asserts the value and Viper returns a string for a float64 flag.
  Its two thresholds are its only float64 flags. If you find a threshold on a
  command line, the gate it belongs to cannot fail.

### Writing a mutation by hand

Allowed, narrowly, and never as the first move. Gremlins' operators are
arithmetic, conditionals boundary and negation, `++`/`--` and inverted negatives,
plus the bitwise and logical ones `.gremlins.yaml` keeps off for now. None of them
expresses a SEMANTIC substitution, and some of this repo's load-bearing properties
are exactly that: #864 replaced `conn.GetDataBufPool().Get()` with
`make([]byte, 16<<10)` — same bytes, same ownership ledger, no Gremlins operator
that can produce it, and every behavioural test in `client` passed.

So hand-write a mutation only after a Gremlins run, and only where the property
has no operator behind it. When you do, name the missing operator in the PR, and
keep the old discipline: the edit lands on the site you named, it compiles, and it
is reverted afterwards — `git checkout` on a file with uncommitted work destroys
it, so commit first.

**Timing- or concurrency-shaped tests need more.** A control arm that injects nothing,
and a count of injections actually performed — without both, the run where the
injection never happened passes exactly like a real pass. Where the race is too narrow
to reproduce, **widen the window deliberately** (a sleep or a scheduling point in the
path under test) and show the old form failing and the new one passing under the same
injection. That is evidence; "it passed 100 times" is not.

**Synchronisation fixes cannot be validated by mutation.** Removing the wait restores a
test that passes almost always — which is the defect. Say that explicitly and rest the
claim on the ordering argument plus an injection experiment instead.

## Pass 2 — Are the cases the right ones? (functional test design)

Most weak tests here are not badly written; they test one hand-picked happy value.
Name the technique the cases came from. If the answer is "the one that came to mind",
that is the finding.

| Technique | The question it answers | What its absence looks like |
|---|---|---|
| **Equivalence partitioning** | Which inputs are the same to this code? | Three cases from one class, none from the others |
| **Boundary values** | What is at 0, 1, max−1, max, max+1, empty, nil, one-past-end? | Only mid-range values; off-by-one survives |
| **Decision table** | Every combination of the conditions in this branch | One flag varied at a time, so an interaction is never exercised |
| **State transition** | Which transitions are legal, and which are refused? | Only the happy path walked; illegal transitions untested |
| **Pairwise** | With many parameters, all pairs rather than all tuples | A combinatorial matrix "covered" by a diagonal |
| **Error guessing / negative** | What does the caller do wrong, and what does the peer send that is malformed? | Only well-formed input; the error path is unexecuted |

Protocol-specific prompts that repeatedly find real gaps in this repo:

- **Both directions of a decision.** A test that shows X is accepted, and one that
  shows not-X is refused. A one-sided test is satisfied by a function that always says
  yes.
- **The zero value and the absent value are different cases** — an explicit `0`, an
  unset field, and "the peer omitted the setting" often take three different paths.
- **Limits are tested at the limit**, not comfortably inside it: exactly the window,
  exactly `max_frame_size`, exactly the stream cap, then one past.
- **The peer is hostile.** Malformed, truncated, reordered, replayed, oversized.
- **Name the mechanism that must do the rejecting.** If two mechanisms could produce
  the observed rejection, the test does not pin the one you think it does.

Coverage gaps found here are **filed as their own issues, not fixed inline** — mixing
new coverage into a style change makes both unreviewable.

## Pass 3 — Is it written the way this repo writes tests?

**Arrange–Act–Assert**, three visible blocks in that order, blank-line separated. A
test that interleaves acting and asserting is describing a scenario: split it, or make
it a table.

**Assertions use `testify`; no hand-rolled `if got != want { t.Fatalf(...) }`.**

- `require.*` where the test cannot meaningfully continue — a constructor that must
  succeed, a pointer about to be dereferenced. It aborts, like `t.Fatalf`.
- `assert.*` where it can, so one run reports every mismatch rather than the first.

**`t.Fatalf` → `require`, `t.Errorf` → `assert`. The mapping is not cosmetic.**
Swapping a fatal for a non-fatal assert lets the test continue with invalid state, and
the real failure then surfaces as a nil-dereference panic somewhere less obvious.

**The failure message carries the *why*, and survives the rewrite.** This suite's
messages explain what breaks without the property; `testify` prints only
expected-vs-actual, so move the explanation into `msgAndArgs` rather than dropping it.
A message that only restates the comparison is a downgrade.

**`testify` must not appear inside a measured closure** — `AllocsPerRun` bodies,
`//go:build !race` alloc gates, or the seven bench-gate packages. It reflects and
allocates, and those gates count the whole process. Assert outside the closure.

**Fixtures:** a fixture must be capable of expressing the failure. Ask what it has to
*do* for the bug to be observable — a leak test needs a busy peer, a boundary test must
fail on the read the guard is actually on, and a credential-shaped fixture must be
valid for the role it is presented in, or the peer's legitimate rejection is mistaken
for a defect.

## What gets reported

One line per finding, each with the file and line, and each classified: **cannot
fail**, **wrong cases**, **style**. A style finding on a test that cannot fail is
noise — report the failure instead.

If nothing survives all three passes, say the tests are sound and stop. A review that
must produce findings produces bad ones.

## Integration

- `test-driven-development` — writing them in the first place.
- `verification-before-completion` — what must be proven before claiming green.
- `draining-the-backlog` — owns the mutation bar for the backlog loop; this file is
  the standalone form, and where the two disagree that file wins inside the loop.
- Issue **#722** — the sweep applying this to the existing 2427 tests.
- `.gremlins.yaml` and `.github/workflows/mutation.yml` — the tool's configuration
  and where it runs. Read them before arguing with a verdict.
