---
name: draining-the-backlog
description: Use when draining the poseidon-http-client backlog in a long unattended /loop, or resuming one after a cutoff — hours of work with nobody awake to answer a question, where a stale ticket premise or a test that cannot go red costs the whole iteration.
---

# Draining the backlog

**REQUIRED BACKGROUND:** `running-long-autonomous-loops` owns the context
budget, the subagent return contract, output masking, and the resume checkpoint.
Where this file touches those, it is to say what differs here, and its numbers
win: a threshold that disagrees with one over there is a bug in this file.

## The work, and what it is for

Drain the poseidon-http-client backlog, one ticket per iteration, each iteration
closed. The client is a general-purpose HTTP/1.1+2+3 implementation written from
scratch for speed: maximum throughput, minimum CPU, minimum noise of its own.
Its first consumer is load testing, where the client's own overhead mixes
straight into the measurement, but the same thrift pays in production at scale.

Two things follow. An unnecessary allocation on a hot path is a defect even when
the ticket says nothing about it. And a false green costs more than a visible
break: a silently truncated body, or a benchmark that measures nothing, does
more damage than a crash, because nobody notices either one.

## Working alone

No one is watching. The user does not answer mid-run, so "should I do X?" stops
the work until morning. For anything reversible, act on your own. You have
enough context — do not wind down, and do not offer to start a fresh session.

Number the iterations in your reply. The number survives compaction and keeps
the schedule.

Load this skill again at the start of every iteration. The rules live here, not
in your memory of them: by the tenth iteration the first copy sits in the middle
of the context, where attention reaches worst, and compaction retells it in its
own words and drops precisely that middle. A fresh copy costs a couple of
thousand tokens and puts the rules back at the end, where they are visible. If
you and this file disagree, the file is right.

## What you may move, and what moves you

Everything below assumes the things that judge the work sit outside the work.
Alone at 4am with a ticket that will not go green, the cheapest exit is always to
adjust the thing doing the judging, and it does not feel like cheating — it feels
like fixing a bad test. Know which side of the line you are on before you touch
anything:

| Surface | Here that means | Rule |
|---|---|---|
| **Locked** | Test assertions, CI's own filters, `bench-gate`, the allocation gates behind `!race`, the mutation bar, this file | Read freely, propose changes in the open — never change one and then cite the green it produced |
| **Editable** | The ticket's code, the tests you are adding, the batch's diff | This is the work; move it |
| **Append-only** | The memory checkpoint, the issues you file, the night's disposition list, lesson files | Add; do not rewrite what an earlier iteration recorded |
| **Human-controlled** | Merging your own PR, releases, weakening any gate above, anything irreversible | Prepare it, describe it, stop |

Changing a locked surface is sometimes right — a test really can be wrong. What
is never right is changing it and reporting the result as verification. Do it as
its own visible act, in its own commit, and say in the PR body that the evidence
now rests on a check you rewrote. An agent that quietly moves the bar and then
truthfully reports clearing it has produced a night of work that means nothing,
and nothing in the log will say so.

The env guard that skips a benchmark is the local shape of this, already living
in the repo. It is a legitimate answer to a gate that cannot express an honest
allocating benchmark. It is not an answer to a benchmark you did not want to fix.

## The iteration

Merge the PRs that are ready, take ONE ticket, close the batch with a commit, a
PR, and a memory entry. An unclosed batch is the most expensive shape a cut-off
can take.

A ticket you refute closes the same way. The verdict, the evidence under it, and
the issue closed are the deliverable; what ends is the batch, not necessarily
the code. "Nothing to commit" is a report, not an exit. When the ticket was
loop-internal and there is no issue to close, the verdict goes to the memory
checkpoint — not into a PR body, which nobody triages and nothing searches.

**Re-derive the ticket's premise from the code before you write a line.** Line
references, numbers, and "X is broken" have gone stale here more than once, and
checking the premise pays for itself faster than any implementation built from
the description. It leaves a trace or it did not happen: the line you actually
read, or the command and its output, goes into the ticket verdict. A premise
check with no artifact looks identical to one that was skipped.

Before reporting a result, match every claim against tool output you saw this
session. Whatever you did not check, call unchecked.

## Bugs: the causal chain

No bug is fixed or closed without one: five whys, every link backed by a command
whose output you saw. A link without a command is a flagged assumption, and it
is flagged in writing — the chain belongs in the PR description, which is where
CLAUDE.md already requires it, so an unbacked link is visible to a reader rather
than only to you.

Two mistakes this chain tends to make on its own:
- a quantifier you never measured. "nothing", "never", "all three" are separate
  claims, and the evidence for each is a command whose output lands next to the
  word. Let the word itself be the trigger: writing it is the moment to go run
  the count or the grep that would refute it. This is the rule that survives
  every careful reading of this file and gets broken anyway;
- a root cause that is really the nearest visible barrier. A neighbouring check
  often trips before the real one, and then you fix the wrong thing.

Open an issue with a reproduction for everything you turn up — report every
finding, the small ones and the ones you are unsure of included; filtering
happens separately. Fixing it on the spot does not excuse you from filing it.

Close the iteration with a one-line disposition for everything you noticed, and
every line carries a number: filed as #N, or filed as #N and closed by this
batch. The single exception is a thing you checked and found not to be a defect,
which needs the command that shows it — "probably nothing" is not that command.
A finding you neither filed nor listed leaves no trace anywhere, and no trace is
the same signal as a test run that executed nothing.

## Features go through /ork:implement

Anything that adds behaviour. Tests and edits to existing code you write
yourself; split a ticket that mixes the two. Nothing beyond what the task asks
for: no refactoring of the surroundings, no abstractions for a hypothetical
future, no handling of cases that cannot occur.

## The other commands

This skill loads every iteration and it is the only one that does. The rest are
exceptions, and two of them may not be reachable from inside a loop at all.

`/simplify` — after the batch's change is written and green, over the diff you
just produced and nothing else. It rewrites for reuse and simplicity and does
not hunt for bugs, so the bar above still has to pass after it: re-run the
tests, and re-run the mutation if it touched the test you were proving.

`/ork:assess` — only when the iteration hits a real fork: two viable approaches
and no cheap experiment that separates them. It returns a rated report, and that
report is context you keep paying for every iteration afterwards. Rating work
already decided on is the most expensive way this loop procrastinates.

`/mattpocock-skills:improve-codebase-architecture` — not from the loop: it is
slash-only and ends in a browser report a human picks from. File architectural
friction as an issue naming the module; running it is the user's move.

`/ork:implement` carries the same slash-only flag, which is worth knowing before
a feature ticket stalls the night. Try the call once; if it comes back refused,
that blocker is scoped to the ticket, not to the session: label the ticket for
the user's morning, leave it open, take a different one — and do not spend a
later iteration rediscovering the same refusal, since nothing between one
iteration and the next changes that flag. The end-of-turn rule stops the whole run
for a blocker that would stop every ticket — a usage limit, a repository you
cannot write to — and this is not one of those. Building the feature by hand
instead defeats the routing rule, which exists so features get the heavier
treatment.

## The quality bar

A test that does not go red when you break the thing it covers is not a test.
Mutate and confirm — twice, printing how many tests actually ran, because zero
tests run looks exactly like "caught". Commit before mutating, assert the edit
landed on the site you named, and restore afterwards: an unanchored replacement
takes the file's first match instead, and a mutation that never applied reads
exactly like a caught one.

Back a race or narrow-window test with a control run that injects nothing and a
count of injections performed: without both, the run where the injection never
happened passes just as well.

Claim a difference between variants only after you have the spread within one
variant. Smaller than your own noise is not a difference.

A pipeline's exit code is its last command's, so `cmd | tail` is always green.

Performance is guarded unevenly, and you need to know where. `bench-gate` scans
its seven packages unscoped: any `Benchmark` line reporting non-zero `B/op` or
`allocs/op` fails the job, so an honest benchmark of an allocating path turns CI
red and the repo's answer is an env guard that skips the benchmark entirely.
`conn`, `client`, `grpc` and `http1` sit outside bench-gate and carry their own
allocation gates behind `//go:build !race`, because the race detector allocates
on its own and drowns a one-allocation difference. Run CI's filter rather than a
shorter one you compose — `go test -count=1 -run
'Allocs|DoesNotAllocate|IsNotCopied|CallOptions' ./conn/ ./client/ ./grpc/
./http1/` — since a narrower pattern and a shorter package list have each already
passed here while the gate they stood for never ran. The main run under `-race`
executes none of it.

## Against degradation

Every 3rd iteration is a compaction: fold what is new into the checkpoint's
existing sections instead of regenerating them, and carry identifiers (paths,
SHAs, symbol names) over word for word, because prose mangles them. Every 5th,
measure where the context is going and optimise what you measured, not what you
assumed.

`running-long-autonomous-loops` says to trigger on the signal rather than on a
timer, and that is not a contradiction: the schedule is the floor. Compact ahead
of it once the window is past the sibling's ~70%, or as soon as a lost-middle symptom
shows — you contradict your own numbers, you break a rule from this file, you
redo work already done. Degradation arrives as a cliff, not a slope, so
compacting once symptoms show is already late.

Compaction is what makes you re-read code, not the loop: iterations share one
context, so what you opened two tickets ago is still there until a compaction
takes it. Write derivations into the checkpoint, not file contents — "recycle
resets every field, guarded by TestRecycleStream_ResetsReqAuthority" survives
and saves the next read; a pasted function body does not survive and would not
have helped. Orient from the architecture map in CLAUDE.md and from the code
graph before opening whole files, and re-derive only the lines the current
ticket actually names.

The graph is indexed per directory and per commit. Before trusting an answer,
confirm an index exists for the worktree you are actually in AND that the
`head_sha` it is stamped with is your current HEAD — existence alone proves
nothing, and the listing exposes both. Re-index the same root after a batch
lands. An index built against another commit replies with line
numbers that have since moved — the same stale premise this loop exists to
catch, arriving through a tool instead of a ticket. Read its degrees precisely:
in-degree counts distinct calling functions, not call sites, so a lower number
than grep gives is agreement, not a contradiction, and neither one licenses
"nothing calls this" without the command that shows it.

A wrong fact that reached the context is not cured by a correction on top: the
first version keeps its weight and resurfaces a few turns later. Go back to the
source — the file, the command output, the ticket — and take the value from
there again.

Keep lesson files: one lesson per file, the substance in the first line, and why
it matters. Do not restate what the repository or its history already holds, and
delete a lesson that turns out wrong.

**Record what you tried and abandoned, not only what you shipped.** A dead end
leaves no trace anywhere today: the issue log holds findings, the checkpoint holds
derivations, and the approach you spent forty minutes on before discarding it is
in neither. So iteration eleven tries it again, at full price, and discards it
again for the same reason. One line per abandonment — what you tried, what killed
it, the command that showed it — is enough to stop that, and it is the cheapest
line in the checkpoint. It is also the one an agent skips, because abandoning
something feels like nothing happened.

**Prune across batches, not only within one.** `/simplify` runs over the diff you
just produced, which catches accretion inside a batch and is blind to accretion
across ten of them: a guard added in batch three, a second guard in batch six that
subsumes it, a helper from batch eight with one caller left. Nothing here removes
any of that, because every individual addition was justified when it landed. Every
fifth iteration, take one stack you have added to more than twice and try removing
its oldest member — if the suite stays green and the mutation still dies, it was
scaffolding. Equal quality with less code is an improvement, and it is the only
kind this loop will never propose on its own.

## Subagents

**Under Opus 5:** delegate only large, independent tracks. Do not delegate what
you would finish yourself in a handful of calls, and do not hand your own
finished work to a subagent for a second pass over it.

**Under Fable 5:** delegate independent subtasks and keep working while they
run. Every few iterations, examine what a subagent produced with fresh context —
against the specification, not against your memory of it.

Either branch: a verifier's assignment is to refute one named claim you are about
to publish, with fresh context and the claim alone — that is not the same thing
as handing over your finished work for a general pass, which is what the Opus
rule forbids and what returns opinions instead of counter-evidence. Five workers
is the ceiling for a single fan-out, and the refuter does not count against it. Past that the orchestrator burns more context reading summaries than
the workers spend working, and each extra agent adds coordination faster than it
adds coverage. Before any fan-out, price the single pass you are replacing — a
fan-out that never beat one careful pass is not a win, it is a bill. The shape of
the return itself belongs to `running-long-autonomous-loops`.

## Against Anthropic's Opus 5 guidance — one confirmation, two open tensions

Checked against Anthropic's Opus 5 prompting guide on 2026-08-16. Recorded here
rather than silently resolved, because two of these are judgement calls the next
run should make deliberately.

**Confirmed.** "Report every finding, the small ones and the ones you are unsure
of included; filtering happens separately" is exactly what the guide prescribes:
a review prompt saying *"only report high-severity issues"* or *"be conservative"*
gets followed literally and returns less, so the guide's own advice is to report
everything and filter in a separate pass. Keep that rule as written; it is not a
stylistic preference, it is a countermeasure.

**Tension 1 — the refuter.** The Subagents section keeps one narrow delegation:
a verifier assigned to refute one named claim, with fresh context and the claim
alone. The guide rules out delegating any review of one's own output to a
subagent, flatly and without a carve-out, and says separately that instructed
re-checks *"compound with the model's own behavior and add cost without improving
results"*. (Its exact phrasing is not reproduced here: this file ships a gate that
bans those substrings outright, and the gate is a substring match that cannot tell
a quotation from an instruction. It failed this section once, correctly.) The distinction
this file draws — targeted refutation of a single published claim, versus handing
over finished work for a general pass — is real, but it is this file's
distinction, not the guide's. Treat the refuter as on probation: use it only for a
claim that will be published and is expensive to retract, and if a run's refuters
mostly return agreement, drop the construct rather than defending it.

**Tension 2 — the lost middle.** "Working alone" justifies re-loading this skill
each iteration on the grounds that the first copy ends up mid-context "where
attention reaches worst". The guide states that on Opus 5, across a 1M-token
window, *"instruction following, tool calling, and reasoning stay consistent
throughout the window"*. If that holds here, the stated reason for the re-load is
weaker than written. The re-load is cheap insurance and worth keeping, but the
*justification* should be the honest one — compaction retells these rules in its
own words and drops detail, and a fresh copy is immune to that — rather than an
attention claim the model's own documentation contradicts.

**Also worth acting on.** The guide reports that `low` and `medium` effort hold
quality at a fraction of the tokens, and that review accuracy in particular holds
at lower effort. Nothing in this file sets effort per stage. Premise re-derivation,
mutation runs, and issue drafting are candidates for `low`/`medium`; the causal
chain and the design forks are not.

## Environment

Go runs only through `wsl -e bash -lc '…'`, POSIX paths inside the quotes. git
and gh run from the Bash tool on the Windows side — not WSL, where a worktree's
`.git` points at a Windows path its git cannot resolve and every command dies
with `fatal: not a git repository`. PowerShell runs them, but git writes progress
to stderr and PowerShell can render that as an error record, so the Bash tool is
the one that reports git's own exit code and nothing else. `rtk go test`
is fine, `rtk golangci-lint` is not — it returns 0 while findings are live. Run
vet under each build tag, and the linter as `GOOS=linux golangci-lint run`: the
`_linux.go` files are never analysed on the Windows host, so a local green says
nothing about them.

## End of turn

The closing message is the first thing the user has seen in many hours: lead
with the outcome, details after it, in full sentences and without working
shorthand.

Stop at an external blocker, naming the blocker and the next step. If your last
paragraph turned out to be a plan, a question, or a promise, do the work now
instead.

## Where this loop actually slips

Three agents planned an iteration of this loop with the repository's CLAUDE.md
loaded and none of these rules. They re-derived every ticket premise, held
scope, and got the environment right unprompted. These are the four things they
talked themselves out of, in their own words:

| The argument | What holds |
|---|---|
| "Filing here would be backlog noise" | Filtering is a separate pass with the whole list in front of it. At 4am you are the worst-placed reader of your own finding. |
| "Worth a line in the PR body at most" | A PR body is not searchable as work and nobody triages it. An issue is and they do. |
| "Nothing to commit, nothing to push, no PR" | See the iteration: a refuted ticket still closes a batch. |
| "Mutating committed code to re-derive a known result is busywork" | The mutation is the whole difference between a test and a decoration, and a verdict can flake, which is why it runs twice. |

One refusal in that baseline was correct, and the bar has to leave room for it: a
new test whose mutant an existing test already catches should not be written at
all. Say so, drop the test, move on — do not strengthen it into a test of
something else just to have written one.

## What gets lost first

The end of a text reads better than its middle, so what follows anchors what is
most expensive to forget — not a substitute for the sections above, a reminder
that they are there.

A fresh load of this skill at the start of the iteration. The iteration number
in the reply. The ticket's premise checked against the code before the first
line of implementation. Five whys, each link with its own command. The mutation
run twice, with the number of tests that ran printed beside it. A control arm and
an injection count behind anything timing-shaped. The spread inside one variant
before any claim of a difference between two. CI's own allocation filter rather
than a shorter one. The batch closed with a commit, a PR, and a memory entry, and
every finding of the night carrying an issue number.

And the one that hides best, because clearing it feels like progress: no green
cited from a check you edited this session. If you loosened an assertion, widened
a tolerance, narrowed a filter or guarded a gate, that was a change to a locked
surface — it belongs in the PR body in those words, and the run after it verifies
nothing.

<!--
Copied from poseidon-http-client @ main on 2026-08-16 (SKILL.md + check-skill.py),
then extended the same day. check-skill.py is unmodified.

DIVERGED FROM THE CLIENT REPO. Additions, not upstream text:
  - "What you may move, and what moves you" (surface table)
  - the abandonment log and the cross-batch pruning rule (in Against degradation)
  - the closing paragraph of "What gets lost first"
Source: `context-engineering:harness-engineering`. Upstream already resisted
metric gaming in practice (mutation testing, control arms, CI's own filter) but
never named the principle, and kept no record of discarded approaches.
-->

<!--
Second pass, 2026-08-16: reconciled against Anthropic's "Prompting Claude Opus 5"
guide (platform.claude.com/docs/en/build-with-claude/prompt-engineering/prompting-claude-opus-5).
The section naming Opus 5 explicitly is from that pass.
-->
