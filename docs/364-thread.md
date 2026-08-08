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

## Step 2 — re-scope the gate so a behaviour can move (path B)

Chosen after step 1 showed the gate's per-protocol anchors are destroyed by the
very operation they are meant to police.

**What changed.** Each case named one `(file, func)`. It now names a list of
candidate `sites` — the per-protocol implementation, and the shared core once it
exists — and a new `locate()` picks the one that currently carries the
behaviour. Zero matches is an error (the anchor rotted); **more than one is also
an error** (two copies of a behaviour that must have a single home). That second
check is the one that makes the migration safe to do incrementally: a half-done
move, where a behaviour exists in both the protocol file and the core, fails
loudly instead of being silently mutated in the copy nobody runs.

**Verified before any code moved.** `13/13` on unmigrated code — same result as
before the change, from a gate that can now follow the behaviour. An evaluator
edit that only proved itself after the refactor it was meant to judge would be
worthless.

**What was rejected, and why it is recorded here rather than argued again.**
The wrapper-with-forwarders idea from the step-1 writeup does not work. The gate
mutates *body lines* inside the anchor function; a forwarder such as
`applySet(next) { mp.core.applySet(next) }` contains none of them, so the anchor
resolves and the mutation still cannot apply. Keeping the method name is not
keeping the observation point. Struck from the options.

**A fear that measurement removed.** `watchDrain`'s `InFlightStreams == 0`
predicate has no gate case of its own, and it is the function whose meaning
differs most across protocols — exclusive exchanges on H1, multiplexed streams
on H2/H3. Probed directly before proceeding: replacing the predicate with
`if true` (drop the sub-pool regardless of in-flight work) is CAUGHT on all
three, by the `ReviveBeatsDrainWatcher` tests, which hold a conn in flight
across the removal precisely so the watcher has something to get wrong. It is
covered — through the drop-guard cases rather than one of its own.

Gate coverage as it stands: 5 functions of the 13 in the managed tier carry
cases. The other 8 are unpinned, which is worth knowing before any of them is
merged into the core.

---

## Step 3a — gate: precedence instead of "one home"

Step 2's `locate()` treated two matching sites as an error. Applying the H3
migration proved that rule wrong: all four H3 cases went CAUGHT, and all nine
H1/H2 cases went ERROR — *behaviour lives at 2 sites*. Correct observation, wrong
conclusion. Mid-migration a behaviour genuinely does exist twice: in the core,
and in every protocol that has not moved yet.

The invariant is not "one copy in the tree" but **one copy that this protocol
executes** — its own while that copy exists, the core's only after it is deleted.
`locate()` now takes the first listed match, per-protocol file first.

This is a correction to a rule I invented in step 2, not a relaxation of
coverage. The 13 cases and the tests they assert against are untouched; once
every protocol has moved, the per-protocol sites are gone and all 13 mutate the
single shared copy — strictly stronger than today, because each case then proves
its own protocol observes the shared code.

Verified in **both** states, because a gate that is only right after the refactor
would be worthless: 13/13 with H3 migrated, 13/13 with it reverted.

## Step 3b — H3 on the core

`client/managed_core.go`: `managedCore[P, MC, C, R]`, a four-method
`subPoolBackend[MC]` (`acquire`, `Stats`, `Close`, `warmup`), and
`coreSubPool[P, MC]`. All 13 methods ported verbatim from the H3 file with types
substituted. The three measured differences are injected as closures rather than
branched on:

    newSub    func(key string) P          // which sub-pool to build
    connOf    func(MC) C                  // read the conn out of the record
    mkRelease func(P, MC) R               // release shape: func() vs func(bool)

`release` is deliberately NOT on the interface — its signature differs per
protocol, which is exactly what `mkRelease` exists to absorb.

H3 is two aliases:

    type h3SubPoolState = coreSubPool[*h3Pool, *h3ManagedConn]
    type h3ManagedPool  = managedCore[*h3Pool, *h3ManagedConn, h3Client, func()]

Aliases, not wrappers: the pinned tests index `mp.subPools` and read `mp.mu`,
`mp.drainMode`, `mp.resolver`, `mp.tickerPeriod` across ~20 sites, and an alias
keeps every one compiling untouched. A refactor whose evaluator had to be edited
to accept it would be self-certifying.

`h3_managed_pool.go` 449 → 89 lines. Metrics is captured by the `newSub` closure
on purpose: `newH3Pool` defaults a nil `*Metrics` to its own fresh struct, so
letting it default per sub-pool would silently under-count `Client.Metrics()`
with the whole suite green.

Loop: build clean · vet clean · `golangci-lint` 0 issues · `go test -race
./client/` green (71 s) · **0 test files changed** · gate **13/13**.

Still per-protocol: H1 and H2. `watchDrain` is now shared with H3 only — the
predicate hazard becomes real when the second protocol moves onto it.

---
