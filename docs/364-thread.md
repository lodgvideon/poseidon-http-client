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
