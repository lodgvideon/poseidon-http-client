---
name: verification-before-completion
description: Use when about to claim work is complete, fixed, or passing, before committing or creating PRs - requires running verification commands and confirming output before making any success claims; evidence before assertions always
---

# Verification Before Completion

## Overview

Claiming work is complete without verification is dishonesty, not efficiency.

**Core principle:** Evidence before claims, always.

**Violating the letter of this rule is violating the spirit of this rule.**

## The Iron Law

```
NO COMPLETION CLAIMS WITHOUT FRESH VERIFICATION EVIDENCE
```

If you haven't run the verification command in this message, you cannot claim it passes.

## Scope on Claude Opus 5 — what to drop from this file, and what to keep

Anthropic's Opus 5 prompting guide is explicit that a class of instruction in this
skill is now counterproductive:

> If your prompt contains explicit verification instructions ("include a final
> verification step for any non-trivial task," "use a subagent to verify"), remove
> them: instructions like these cause over-verification on Claude Opus 5, and
> removing them reduces wasted tokens with no loss in quality.

and, on self-correction:

> Avoid instructing re-checks it already performs ("double-check your answer,"
> "re-verify before responding"); like verification instructions, these compound
> with the model's own behavior and add cost without improving results.

That does not retire this skill, because it does not all say the same thing. Two
different rules live here and only one is affected:

| Kind of instruction | Example | On Opus 5 |
|---|---|---|
| **Ritual re-check** — do the work again for its own sake | "add a verification step", "double-check before answering", "spawn a verifier over your finished work" | **Drop it.** The model already does this; instructing it compounds and costs tokens for nothing |
| **Claim discipline** — what you may assert, and on what evidence | "do not say it passes unless you ran it", "evidence from a check you edited is not evidence", "count what ran, not just what failed" | **Keep it.** This governs the honesty of a statement, not the frequency of a check |

The distinction is worth stating plainly, because it is easy to read the guide as
retiring the whole idea. Self-verification is about *whether the work was done*
and Opus 5 handles it unprompted. This file is about *what you are entitled to
claim* — and a model that verifies diligently and then reports a green it
manufactured has satisfied the first and failed the second. Nothing in the guide
suggests calibration improved; it says re-checking did.

So: the Gate Function below is a habit of assertion, not a mandated extra pass.
Do not schedule verification rounds. Do not delegate verification of your own
work to a subagent — the guide names that one specifically. Do run the command
whose result you are about to state, and do not state one you did not run.

One structural note, from the same guide: *"Positive examples of the
communication style you want tend to be more effective than instructions about
what not to do."* Most of this file is prohibition — Red Flags, Rationalization
Prevention, ❌ pairs. The Key Patterns section is the part shaped the way the
guide recommends, and it is the part to extend when this file next grows.

## The Gate Function

```
BEFORE claiming any status or expressing satisfaction:

1. IDENTIFY: What command proves this claim?
2. RUN: Execute the FULL command (fresh, complete)
3. READ: Full output, check exit code, count failures
4. VERIFY: Does output confirm the claim?
   - If NO: State actual status with evidence
   - If YES: State claim WITH evidence
5. ONLY THEN: Make the claim

Skip any step = lying, not verifying
```

## The Second Iron Law: the check must be out of your reach

```
EVIDENCE FROM A CHECK YOU CAN EDIT IS NOT EVIDENCE
```

The gate above assumes the verification command means something. It stops meaning
anything the moment the thing being verified and the thing doing the verifying are
both yours to change. An agent optimizing for green does not have to lie — it can
simply move the bar and report honestly that the bar was cleared.

Before running a check, know which side of the line it sits on:

| Surface | Examples | Rule |
|---|---|---|
| **Locked** | Test assertions, CI filter, coverage floor, allocation gate, rubric | May read, may propose changes — may **not** change them and then cite the result |
| **Editable** | The implementation under test, the fix, the config being tuned | Change freely; this is the work |
| **Append-only** | Results log, findings list, rejected attempts | Add to it; never rewrite history |
| **Human-controlled** | Merge, release, deploy, weakening a gate | Propose only; requires explicit approval |

Changing a locked surface is not forbidden — it is sometimes correct. What is
forbidden is changing it and then reporting the resulting green as verification.
If a test is genuinely wrong, say so, change it as its own reviewable act, and
state plainly that the evidence now rests on a check you rewrote.

### How green gets manufactured

Every entry below produces a truthful "it passes" and proves nothing. They are
ordered by how easy they are to do without noticing:

| Move | What it looks like | Why it reads as green |
|---|---|---|
| Narrowing the run | `-run 'TestFoo'` instead of CI's filter | Everything selected passed; most of it never ran |
| Piping the command | `cmd \| tail`, `cmd \| grep` | A pipeline's exit code is its **last** command's — this is always green |
| Guarding the check | Env var that skips the benchmark on CI | Job green, gate never executed |
| Loosening the assertion | `>=` for `==`, dropping a field from the compare | Same test name, weaker claim |
| Marking it skipped | `t.Skip`, `xfail`, `.only` left in | Zero failures, zero coverage |
| Widening the tolerance | Bumping a timeout or an epsilon until it stops flaking | The flake was the finding |
| Citing the last run | Evidence from before the most recent edit | True yesterday, unknown now |

Two of these deserve their own habits:

- **Never read an exit code through a pipe.** Redirect to a file, then inspect the
  file: `cmd > out.txt 2>&1; echo $?` then grep `out.txt`. Reading `$?` after a
  pipe reports the formatter's success, not the test's.
- **Count what ran, not just what failed.** Zero failures and zero tests executed
  are the same output. Print the number of tests that actually ran beside the
  result, every time.

### Aggregate green hides a failed dimension

A single pass/fail rolls several independent obligations into one bit, and the one
that failed is exactly the one the roll-up hides. Report per dimension — tests,
lint, types, build, the specific gate this change was supposed to satisfy — and
treat a missing dimension as a failure, not as an absence. "The suite is green"
answers a narrower question than "the change is correct", and only one of those is
usually the claim being made.

### Evidence has a timestamp

Verification evidence is only valid for the tree that produced it. Any edit after
the run — including a "trivial" one, including a rebase, including an
auto-formatter — invalidates it. If you changed anything since the command ran,
you have not verified the current state; you have verified a state that no longer
exists. Re-run, or say which run the claim rests on and what has changed since.

## Common Failures

| Claim | Requires | Not Sufficient |
|-------|----------|----------------|
| Tests pass | Test command output: 0 failures | Previous run, "should pass" |
| Linter clean | Linter output: 0 errors | Partial check, extrapolation |
| Build succeeds | Build command: exit 0 | Linter passing, logs look good |
| Bug fixed | Test original symptom: passes | Code changed, assumed fixed |
| Regression test works | Red-green cycle verified | Test passes once |
| Agent completed | VCS diff shows changes | Agent reports "success" |
| Requirements met | Line-by-line checklist | Tests passing |

## Red Flags - STOP

- Using "should", "probably", "seems to"
- Expressing satisfaction before verification ("Great!", "Perfect!", "Done!", etc.)
- About to commit/push/PR without verification
- Trusting agent success reports
- Relying on partial verification
- Thinking "just this once"
- Tired and wanting work over
- **ANY wording implying success without having run verification**

## Rationalization Prevention

| Excuse | Reality |
|--------|---------|
| "Should work now" | RUN the verification |
| "I'm confident" | Confidence ≠ evidence |
| "Just this once" | No exceptions |
| "Linter passed" | Linter ≠ compiler |
| "Agent said success" | Verify independently |
| "I'm tired" | Exhaustion ≠ excuse |
| "Partial check is enough" | Partial proves nothing |
| "Different words so rule doesn't apply" | Spirit over letter |
| "The test was wrong anyway" | Maybe — but then the green is not evidence. Say so |
| "I only widened the tolerance a little" | The flake was the finding you just deleted |
| "It's skipped on this platform for a reason" | Then the gate did not run; do not report it as passing |
| "I ran a narrower filter, it's the same thing" | It is not. Run CI's filter, not one you composed |
| "Exit code was 0" (after a pipe) | That was the pager's exit code |
| "Nothing changed since the run" | Something always did. Re-run or name the delta |

## Key Patterns

**Tests:**
```
✅ [Run test command] [See: 34/34 pass] "All tests pass"
❌ "Should pass now" / "Looks correct"
```

**Regression tests (TDD Red-Green):**
```
✅ Write → Run (pass) → Revert fix → Run (MUST FAIL) → Restore → Run (pass)
❌ "I've written a regression test" (without red-green verification)
```

**Build:**
```
✅ [Run build] [See: exit 0] "Build passes"
❌ "Linter passed" (linter doesn't check compilation)
```

**Requirements:**
```
✅ Re-read plan → Create checklist → Verify each → Report gaps or completion
❌ "Tests pass, phase complete"
```

**Agent delegation:**
```
✅ Agent reports success → Check VCS diff → Verify changes → Report actual state
❌ Trust agent report
```

## Why This Matters

From 24 failure memories:
- your human partner said "I don't believe you" - trust broken
- Undefined functions shipped - would crash
- Missing requirements shipped - incomplete features
- Time wasted on false completion → redirect → rework
- Violates: "Honesty is a core value. If you lie, you'll be replaced."

## When To Apply

**ALWAYS before:**
- ANY variation of success/completion claims
- ANY expression of satisfaction
- ANY positive statement about work state
- Committing, PR creation, task completion
- Moving to next task
- Delegating to agents

**Rule applies to:**
- Exact phrases
- Paraphrases and synonyms
- Implications of success
- ANY communication suggesting completion/correctness

## The Bottom Line

**No shortcuts for verification.**

Run the command. Read the output. THEN claim the result.

This is non-negotiable.

<!--
Copied from poseidon-http-client @ main on 2026-08-16, then extended the same day.

DIVERGED FROM THE CLIENT REPO. Additions, not upstream text:
  - "The Second Iron Law: the check must be out of your reach", with the surface
    table, the manufactured-green catalogue, the per-dimension rule and the
    evidence-timestamp rule
  - the last six rows of Rationalization Prevention
Source: `context-engineering:harness-engineering` (metric-gaming resistance and
locked/editable surfaces). Upstream covered honesty about evidence but never the
case where the evidence is real and the check was moved.
-->

<!--
Second pass, 2026-08-16: reconciled against Anthropic's "Prompting Claude Opus 5"
guide (platform.claude.com/docs/en/build-with-claude/prompt-engineering/prompting-claude-opus-5).
The section naming Opus 5 explicitly is from that pass.
-->
