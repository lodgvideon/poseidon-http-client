# #364 — managed-tier unification: working thread

Append-only. What was tried, what stayed, what was reverted and why.

Evaluator: `scripts/pool-behaviour-gate.py` — 13 mutations, one per pinned
behaviour, each anchored inside its enclosing function. Must read 13/13 before
and after every increment. Locked: editing it is self-certification.

Out of scope: the base pools (`pool.go`, `h1_pool.go`, `h3_pool.go`). Measured
28% mechanically unifiable; the divergences are the load-bearing ones.

---

## Step 0 — baseline

`13/13` on `bdcaf60`. Recorded before touching anything.

Method note carried in from the gate's own history, because it cost a wrong
public claim: an ad-hoc whole-suite mutation reported `h3-drainlazy-retains` as
already caught. It was not. The failure in that run was
`TestIT_H2_StreamedDownload_RetentionStaysBounded` — an unrelated timing test
flaking under load, arriving in the shape the expectation wanted. **A single
whole-suite mutation run is not evidence in a suite containing timing tests.**
The gate's per-case `-run` filter is what makes its verdict trustworthy.

---

## Step 1 — introduce the core, migrate H3 — REVERTED

**What was built.** `client/managed_core.go`: `managedCore[P, MC, C, R]` plus a
three-method `subPoolBackend[MC]` interface (`acquire`, `Stats`, `Close`,
`warmup`) and `coreSubPool[P, MC]`. All 13 methods ported verbatim from the H3
file with types substituted; the three measured differences injected as closures
(`newSub`, `connOf`, `mkRelease`) rather than branched on. H3 became two aliases:

    type h3SubPoolState = coreSubPool[*h3Pool, *h3ManagedConn]
    type h3ManagedPool  = managedCore[*h3Pool, *h3ManagedConn, h3Client, func()]

**Where it got to.** `go build ./...` clean · `go vet` clean ·
`golangci-lint run ./client/` 0 issues · `go test -race ./client/` green (73 s) ·
`git diff --stat` **0 test files changed** · `h3_managed_pool.go` 449 → 89 lines
(−390/+22).

**Why it was reverted.** The gate dropped to **9/13**, with four ERRORs:

    h3-revive             function not found: func (mp *h3ManagedPool) applySet
    h3-drop-guard         function not found: func (mp *h3ManagedPool) dropIfDraining
    h3-failover           function not found: func (mp *h3ManagedPool) acquire
    h3-drainlazy-retains  function not found: func (mp *h3ManagedPool) beginDrain

Not a mutation surviving — a mutation that can no longer be *applied*. The gate
anchors each case inside a per-protocol function, and unification is precisely
the operation that deletes those functions. The gate says it itself: an
un-appliable mutation proves nothing.

**The finding, which is about the refactor and not about the gate.**
Unification collapses three independent observation points into one. After H3
moves to the core there is no h3 `applySet` to mutate — there is one shared
`applySet`, and mutating it is simultaneously the H1, H2 and H3 case. That is
the real cost of this change, and it is the same shape as the `watchDrain`
hazard already flagged: one predicate, three meanings, one place to get it
wrong. It is not visible as a test failure, which is exactly why the gate is
anchored per protocol.

**Not fixed by editing the gate.** The gate is the evaluator. Re-anchoring its
H3 cases onto the shared core so that the refactor passes is self-certification,
and it would also silently reduce 13 behaviours to 10 while still printing a
number that looks like a pass.

**Two ways forward. Both are the human's call, because both change something
the human fixed.**

1. **Wrapper instead of alias.** Make `h3ManagedPool` a struct embedding
   `*managedCore[...]` and keep one-line forwarding methods. Field access still
   works by promotion, so the tests stay untouched, and the gate's per-protocol
   anchors stay valid — 13 observation points survive the migration. Cost: the
   forwarding methods are dead weight, and the design pass showed that a
   *shadowing* method on such a wrapper compiles and is silently never called,
   which is a new failure mode.
2. **Re-scope the gate to per-protocol *tests* rather than per-protocol
   *functions*.** Mutate the shared core once per behaviour and require that
   each protocol's own test file goes red — 13 cases become 4 mutations × 3
   test-filter assertions. Stronger than today (it proves every protocol
   observes the shared code), but it is a change to the locked evaluator and
   must be made deliberately, not as a side effect of wanting a green run.

Reverted to `bdcaf60`. Attempt preserved outside the tree; nothing of it is on
the branch.
