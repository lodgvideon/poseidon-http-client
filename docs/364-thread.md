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

## Step 4 — H2 on the core

Order chosen deliberately: H2 before H1. H2 and H3 both multiplex, so
`InFlightStreams` in `watchDrain` means the same thing for both and sharing the
predicate between them adds no new hazard. H1's exclusive checkout gives that
field a different meaning, so H1 goes last and alone.

**Checked before moving, not after.** Normalised every H2 method body against
the core's (comments stripped, identifiers substituted): 11 of 13 identical,
including `watchDrain` — its only divergence was the back-off comment H1 and H3
never carried. The two that differ are `getOrCreateSubPool` and `acquire`, and
the diff of each is exactly the injection points and nothing else:

    -  p: newPool(key, mp.connOpts, mp.poolOpts, mp.hooksRef, mp.metrics)
    +  p: mp.newSub(key)
    -  release := func() { sub.p.release(mc) }; return mc.c, release, nil
    +  return mp.connOf(mc), mp.mkRelease(sub.p, mc), nil

Had anything else differed, migrating H2 onto the core would have changed H2's
behaviour silently — the gate would likely have caught it, but knowing beforehand
is cheaper than finding out from a red run.

`managed_pool.go` 490 → 115 lines. Two aliases:

    type subPoolState = coreSubPool[*Pool, *managedConn]
    type managedPool  = managedCore[*Pool, *managedConn, *conn.Conn, func()]

Loop: build · vet · lint 0 issues · `-race` green (74 s) · **0 test files
changed** · gate **13/13**.

**The gate is now stronger than it was.** Eight cases — every H2 and H3 one —
mutate the single shared copy, and each still requires its own protocol's tests
to go red. That is the property the re-scope in step 3a was for: it proves both
protocols observe the shared code, which per-protocol anchors never could.

Remaining: H1. Its `watchDrain` predicate counts exclusive exchanges where the
core counts multiplexed streams. That is the one step where sharing the function
changes what the number means, and it gets its own increment.

---

## Step 5 — H1 on the core; the tier is unified

Same pre-check as H2, and it is the one that mattered: **11 of 13 bodies
identical to the core, `watchDrain` among them.** The two that differ are
`getOrCreateSubPool` and `acquire`, differing only at the injection points —
here `mkRelease` carries `keepAlive`, because H1 is the protocol whose checkout
is exclusive.

**The `watchDrain` hazard, stated precisely now that it is measurable.** The
fear was that H1's `InFlightStreams == 0` means exclusive exchanges while H2/H3
mean multiplexed streams, so one shared predicate would conflate them. It does
not: `s.p.Stats()` dispatches through the type parameter to `h1Pool.Stats()`,
which sums `h1SumActive`. The predicate asks "nothing in flight" in each
protocol's own terms, which is correct for all three, and the code was already
byte-identical before the move.

What *did* change is real and worth writing down: **the drain poll schedule now
has one home for three protocols.** `drainPollInit` 20 ms, `drainPollMax` 5 s,
and the doubling between them used to sit in three places. Retuning them for one
protocol now retimes sub-pool teardown for all three, and no test asserts
timing — all three `DrainGraceful` tests assert eventual convergence. That is a
review-time constraint, not something the gate can catch.

`h1_managed_pool.go` 452 → 89 lines. Tier total **1391 → 733**, of which 448 is
the shared core: three implementations became one.

Loop: build · vet · lint 0 issues · `-race` green (72 s) · **0 test files
changed** · gate **13/13**.

**The gate is at its strongest here.** All 13 cases now mutate the single shared
copy, and each still requires its own protocol's tests to go red. Before the
refactor, 13 cases proved 13 separate implementations were observed. Now they
prove that every protocol observes the one implementation — which is the property
that makes the unification safe to keep, rather than merely safe to have done.

Managed tier complete. Base tier deliberately untouched: 28% mechanically
unifiable, and its divergences are the load-bearing ones.

---
