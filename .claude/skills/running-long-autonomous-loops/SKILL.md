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
