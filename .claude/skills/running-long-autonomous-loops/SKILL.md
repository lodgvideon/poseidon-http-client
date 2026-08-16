---
name: running-long-autonomous-loops
description: Use when running a multi-hour /loop, an autonomous backlog drain, or any session that fans out subagents and must survive a usage-limit cutoff. Covers the token-budget policy, the workflow return contract, and the resumability checkpoint. Also use when a loop died mid-flight and you are deciding how to resume.
---

# Running Long Autonomous Loops

## Overview

A loop's lifespan is set by what it puts in the main context, not by how much
work remains. Tool outputs dominate agent trajectories, so a loop that pipes raw
output through context dies hours before one that does not — with the same work
left in both cases.

Measured on the HTTP/2 reconciliation loop (28 commits, 62 conformance tests):
two adversarial rounds cost **1.13M subagent tokens** but needed only ~40 fields
of that in the main context. The earlier reconciliation fan-out was **259 agents
/ 23.1M tokens**. Subagent tokens are cheap and isolated; main-context tokens are
the scarce resource. Every rule below is about keeping the second small while the
first does the work.

## Iron rules

1. **Subagents return verdicts, not evidence.** Full reasoning goes to a file;
   the main context gets the decision. See the return contract below.
2. **Never pipe raw test output through context to count something.** Redirect to
   a file, then grep. `go test ./... -v > /tmp/t.txt 2>&1; grep -c '^--- FAIL' /tmp/t.txt`
   — never `go test -v | grep -c FAIL`.
3. **Green output gets masked, red output stays raw.** A failing test's full
   output is the debugging material; masking it costs a whole diagnostic round.
   Suspend masking for anything failing in the last 3 turns.
4. **Checkpoint to the memory file after every batch**, not at the end. A
   usage-limit cutoff is not a failure if the next session resumes from a written
   state; it is a total loss if the state was only in context.
5. **One batch = one commit.** Batch boundaries are the resume points. A
   half-applied batch is the expensive failure mode.
6. **Stop on an external blocker, do not loop against it.** Usage limits,
   billing, a tag the user reserves, a sandbox denial: write the blocker and the
   exact next action, then stop. Retrying burns the budget that would have
   finished the work.

## Workflow return contract

The default failure is a workflow that returns everything the agents produced,
and the orchestrator then re-reads the full output file to extract two fields.
That happened here — it cost a hand-written Python extraction pass per round.

Force the shape at the schema:

```js
const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['verdict', 'severity', 'finding'],
  properties: {
    verdict:  { type: 'string', enum: ['REFUTED', 'UPHELD'] },
    severity: { type: 'string', enum: ['blocker', 'minor', 'none'] },
    finding:  { type: 'string', description: 'ONE sentence. No evidence, no reasoning.' },
    // evidence stays OUT of the return; the agent writes it to a file and
    // returns the path when the finding is non-trivial.
    evidence_path: { type: 'string' },
  },
}

// Return the decision surface only. journal.jsonl keeps the rest.
return { blockers: results.filter(r => r?.severity === 'blocker'), counts }
```

Rules of thumb:

- A verdict field the orchestrator will not branch on does not belong in the return.
- Prose fields need an explicit length cap in the schema description, or they
  arrive at ~700 chars each and multiply by the agent count.
- `journal.jsonl` in the transcript dir is the durable record — parse it when a
  finding actually needs its evidence, not by default.
- A task notification **truncates**; never diagnose from it. The journal is the
  source of truth.

## Budget policy

| Category | Share | Trigger |
|---|---|---|
| Tool outputs | ≤35% | over budget → redirect to file, keep greps |
| Batch/work state | ≤30% | over budget → checkpoint + compact |
| Subagent returns | ≤20% | over budget → tighten the return schema |
| Reserve | 15% | never spend; it is the resume margin |

Trigger on the signal, not on a timer. Total over ~70% → checkpoint the current
batch and compact. Under active debugging → mask nothing; finish the diagnosis
first, then compact.

**Mask first, then compact — the order is not interchangeable.** Masking removes
low-value bulk losslessly and leaves the content retrievable; compaction is lossy
and cannot be undone. Compacting before masking spends the lossy operation on
material the lossless one would have removed for free, and the summary then
carries a distillation of noise.

**Compact at 70–80%, never at 90%+.** The reason is not tidiness: the model
writing the summary is itself under context pressure at that point, and its
summarisation degrades exactly where it costs most — it drops task goals, loses
user constraints, and flattens the state it was called to preserve. A late
compaction produces a worse checkpoint than an early one, so the trigger is a
ceiling, not a target. If you find yourself past 85% with no checkpoint, summarise
in a separate call with a clean context containing only the material to fold, not
in the pressured one.

**A fresh summary is not a fresh fact.** After compaction the checkpoint reads
authoritatively while silently carrying whatever was true before it. If the task
moved — a requirement changed, an assumption was corrected, a ticket was refuted —
the summary keeps asserting the old version, and nothing in its tone says so.
Re-validate the checkpoint against the current goal in the first turn after every
compaction. This is the same failure as a stale ticket premise, arriving through
your own notes instead of through someone else's.

## Effort is the cost dial, not verbosity

Everything above rations context. The larger lever on Opus 5 is the effort
setting, and Anthropic's guide is unambiguous about how to use it:

> use `low` and `medium` liberally as your primary control for token cost and
> response time wherever quality holds, and step up to `xhigh` for demanding
> coding and agentic work.

For a loop that means: pick effort per stage, not per session. A mechanical
sweep, a file inventory, an extraction pass — `low` or `medium`. The adversarial
round, the hard diagnosis, the design fork — `xhigh`. Running a whole night at one
setting overpays on most of it and underpowers the rest.

**If you inherited effort defaults from an earlier model, re-run the sweep.** The
guide says so directly, and defaults carried over from Opus 4.x are the most
likely reason a loop feels expensive for what it produces.

Two corrections this rules out:

- **Effort does not shorten what the model says.** It controls how much the model
  *thinks*, and lowering it "can reduce thinking volume without reliably
  shortening the visible response". So you cannot buy a shorter subagent return by
  dropping effort — cap the length in the return schema, which is where this file
  already sends you.
- **Terser prose is not the token lever.** Compressing an agent's writing register
  acts on the smallest line of the bill. A subagent's spend is dominated by what it
  *reads* and by how hard it thinks; its final report is a low single-digit
  percentage of the total. Effort and scope move the 90%+; style moves the rest.

**Files written to disk run long on Opus 5, and evidence files are files.** The
guide notes Claude-authored documents are longer than on prior models. A loop that
writes evidence, checkpoints, and reports every iteration accumulates that
silently. Say what length you want when you ask for a written artifact: cover the
substance, no filler sections, no restated summaries.

## Prefix stability — the cheapest optimization, and the one loops skip

Everything above spends effort to shrink the context. Prefix caching spends none
and pays on every single turn, so it comes first — but only if the prefix actually
stays byte-identical, and a long loop is the single easiest place to break it.

- **Nothing dynamic in the stable prefix.** A timestamp, an iteration counter, a
  run id, or a "currently on batch 7 of 20" line in the system prompt or in a
  re-loaded skill invalidates the cache on every turn that changes it. Dynamic
  state goes at the end, in the turn, never in the prefix.
- **Whitespace counts.** A single changed newline invalidates the entire cached
  block downstream of it. If a loop regenerates its own prompt or re-emits a
  preamble, pin it as an immutable string rather than rebuilding it per iteration.
- **Order for stability:** system prompt, tool definitions, reusable templates,
  history, then the current turn. Least-stable content last, always.
- **Re-loading a skill mid-loop is safe; rewriting one is not.** Appending a fresh
  copy of the rules extends the prefix and keeps the cache. Editing the file the
  loop re-reads each iteration invalidates it from that point on, so batch skill
  edits between runs rather than during one.

Target 70%+ hit rate on a stable workload. At loop length this is the difference
between one budget and several.

## Measure the optimization, not just the work

Every mechanism on this page costs tokens to run: masking needs a store and a
reference, compaction needs a summarisation call, partitioning needs a
coordinator. A technique that does not measurably move the metric it was added
for is not neutral — it is overhead with a good story. Every fifth iteration,
check where context actually went and drop the machinery that did not earn its
place, rather than adding another layer on top of it.

## Traps

| Trap | Fix |
|---|---|
| Piping `-v` test output to `grep -c` | Redirect to a file first; grep the file |
| Re-reading a full workflow output for 2 fields | Constrain the return schema instead |
| Masking a failing test's output | Exempt red output from masking entirely |
| Resuming from context instead of the memory file | Checkpoint per batch; treat context as volatile |
| Looping against a usage limit / reserved tag | Classify as external, write the action, stop |
| Verifier prose with no length cap | Cap it in the schema description |
| `-race` flake mistaken for a regression | Re-run before bisecting; see the flake notes in memory |
| Compacting before masking | Mask first — spend the lossless operation before the lossy one |
| Compacting at 90%+ because it "still fits" | The summariser is degraded too; 70–80% is the ceiling |
| Trusting a checkpoint written before the task changed | Re-validate the summary against the goal after every compaction |
| An iteration counter or timestamp in the re-loaded prefix | Dynamic state goes in the turn, never in the prefix |
| Optimization machinery nobody measured | Drop what did not move the metric instead of layering more |

## Red flags — stop and restructure

- A workflow return you have to write a parser for.
- Raw command output in context that nothing will read twice.
- Batch N in progress with no written record of batches 1..N-1.
- The same file read more than twice in a batch.
- A loop iteration that ends without a commit or a checkpoint.

## Integration

- `auditing-rfc-conformance` — the fan-out this budget policy was measured on;
  its journal-parsing and resume guidance applies directly.
- `dispatching-parallel-agents` — when to fan out at all.
- `verification-before-completion` — what must be proven before a batch commits.
- `using-git-worktrees` — isolation when loops may run concurrently.

<!--
Copied verbatim from poseidon-http-client @ main
(.claude/skills/running-long-autonomous-loops/SKILL.md), 2026-08-16.
Verified identical to the docs/loop-context-budget-skill branch, so this is the
current version, not a stale worktree copy.

Copied from poseidon-http-client @ main on 2026-08-16, then extended on the same
day. All skills named under Integration resolve here, plus `draining-the-backlog`.

DIVERGED FROM THE CLIENT REPO. These sections are additions, not upstream text:
  - "Mask first, then compact", the 70-80% compaction ceiling and the stale-summary
    rule (appended to Budget policy)
  - "Prefix stability"
  - "Measure the optimization, not just the work"
  - the last five rows of the Traps table
Source: the `context-engineering:context-optimization` skill. The prefix-caching
material has no upstream counterpart at all — a multi-hour loop is the single
easiest place to break a stable prefix and the most expensive place to do it.

Merging upstream changes now needs a real diff, not a copy. The other four
skills carry the same marker where they diverged.

One environment note: iron rule 2's `/tmp/t.txt` is illustrative. Here, redirect
to the session scratchpad directory instead of /tmp.
-->

<!--
Second pass, 2026-08-16: reconciled against Anthropic's "Prompting Claude Opus 5"
guide (platform.claude.com/docs/en/build-with-claude/prompt-engineering/prompting-claude-opus-5).
The section naming Opus 5 explicitly is from that pass.
-->
