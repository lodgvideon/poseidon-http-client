"""Deterministic gate for the loop skill, run BEFORE any judgement about quality.

Each check encodes a rule the model docs state prescriptively, so a failure here
is a documented defect rather than an opinion.

The rules used to live in .claude/prompts/backlog-loop.md and were re-read by
hand each iteration. They are a skill now, so the gate validates the frontmatter
too — and every prose check runs against the BODY only. Matching against the
description as well would let a rule pass because the frontmatter mentions it,
which is precisely the vacuous pass this file exists to catch.

WHAT THIS GATE CANNOT DO. It catches deletion and hollowing-out: a rule dropped
during an edit, a section reduced to its heading. It does NOT read meaning. Every
check is a substring or a count, and "read this file never" contains "read this
file". A 124-word document inverting every rule in the skill passed all twelve
checks before the structural ones below existed; it now fails on length and on
the missing quality-bar keywords, but a full-length inverted rewrite would still
pass clean. The threat model here is an edit that loses something, not an
adversary — do not read a green gate as "the rules are right".
"""
import io, re, sys

import os

HERE = os.path.dirname(os.path.abspath(__file__))
P = os.path.join(HERE, 'SKILL.md')
raw = io.open(P, encoding="utf-8").read()

fails, warns = [], []

# 0. Frontmatter: the skill is unloadable without it, so this runs first and
#    everything after it reads the body alone.
fm, s = "", raw
m = re.match(r"^---\r?\n(.*?)\r?\n---\r?\n(.*)$", raw, re.S)
if not m:
    fails.append("frontmatter: missing or malformed YAML block")
else:
    fm, s = m.group(1), m.group(2)
    if len(fm) > 1024:
        fails.append(f"frontmatter: {len(fm)} chars, over the 1024 limit")
    name = re.search(r"^name:\s*(\S+)\s*$", fm, re.M)
    desc = re.search(r"^description:\s*(.+)$", fm, re.M)
    if not name:
        fails.append("frontmatter: no name field")
    elif not re.fullmatch(r"[a-z0-9-]+", name.group(1)):
        fails.append(f"frontmatter: name '{name.group(1)}' is not lowercase-kebab")
    elif name.group(1) != os.path.basename(HERE):
        fails.append(f"frontmatter: name '{name.group(1)}' != directory '{os.path.basename(HERE)}'")
    if not desc:
        fails.append("frontmatter: no description field")
    elif not desc.group(1).startswith("Use when"):
        fails.append("frontmatter: description does not start with 'Use when'")

lines = [l for l in s.split("\n")]
body = [l for l in lines if l.strip() and not l.startswith("#")]

# 1. Opus 5: generic self-verification instructions must not appear.
banned = ["double-check", "double check", "re-verify", "reverify",
          "check your work again", "final verification step",
          "verification pass over your own"]
hits = [b for b in banned if b.lower() in s.lower()]
if hits:
    fails.append(f"opus/generic-verification: found {hits}")

# 2. Fable 5: no instruction to reproduce internal reasoning as response text.
reasoning = ["show your reasoning", "transcribe your reasoning",
             "describe your thought process", "spell out your reasoning",
             "write out your thinking"]
hits = [b for b in reasoning if b.lower() in s.lower()]
if hits:
    fails.append(f"fable/reasoning-extraction: found {hits}")

# 3. Both: an explicit scope constraint must exist (documented need).
if not re.search(r"beyond what the task|nothing beyond the task", s, re.I):
    fails.append("scope: no explicit scope constraint")

# 4. Fable 5: autonomous pipelines need the no-questions guard.
if not re.search(r"no one is watching|nobody is watching|does not answer mid-run", s, re.I):
    fails.append("fable/autonomy: no 'user is not watching' guard")

# 5. Fable 5: long sessions need the context-budget reassurance.
if not re.search(r"enough context|context .{0,30}(will|does) not run out", s, re.I | re.S):
    warns.append("fable/context-budget: no reassurance against self-truncation")

# 6. Fable 5: 'give the reason, not only the request' — intent should be stated.
if not re.search(r"because|so that|matters|consumer|the point", s.split("## The iteration")[0], re.I):
    warns.append("fable/intent: opening states the task but not why it matters")

# 7. Opus 5 + Sonnet 5 code review: finding stage must ask for coverage,
#    not self-filtering by severity.
if re.search(r"only the (important|serious|major|high[- ]severity)", s, re.I):
    fails.append("review/self-filter: skill tells the model to filter by severity")
if not re.search(r"report every|including the small|filtering happens separately", s, re.I):
    warns.append("review/coverage: no explicit 'report everything' at the finding stage")

# 8. Prescriptiveness budget (Fable 5: too prescriptive degrades output).
imperative = sum(1 for l in body if re.match(
    r"^\s*[-–]?\s*(Do not|Don't|Never|Always|Merge|Take|Run|Keep|Start|File|Check|Stop|Number|Delegate|Write|Prefer|Read|Split|Report|Compact|Measure|Mutate|Verify|Load|Open|Claim|Back)\b", l))
warns.append(f"size: {len(body)} content lines, ~{len(s.split())} words, {imperative} imperative openers")

# 9. Contradiction: the two subagent branches must be mutually exclusive and labelled.
if len(re.findall(r"Under Opus 5", s)) != 1 or len(re.findall(r"Under Fable 5", s)) != 1:
    fails.append("branching: model-conditional subagent guidance is not clearly labelled once each")

# 10. Every quality rule should carry its reason (why it exists), not just the rule.
qs = s.split("## The quality bar")[1].split("##")[0] if "## The quality bar" in s else ""
if not qs:
    fails.append("structure: no '## The quality bar' section")
rules = [b for b in qs.strip().split("\n\n") if b.strip()]
noreason = [r.split("\n")[0][:48] for r in rules
            if not re.search(r"—|:|because|which is why|looks exactly like|is not a difference|always", r)]
if noreason:
    warns.append(f"traceability: quality rules without a stated reason: {noreason}")

# 11. Lost-in-middle: recall degrades in the middle of a long context, so the
#     rules have to be re-anchored at both edges — an instruction to load the
#     skill again (which puts them back at the end of the window) near the top,
#     and the anchor block at the bottom. A rule that lives only in the middle
#     is one the model stops applying somewhere around the tenth iteration.
#     A third, not a fifth: the head has to hold the framing before the rule.
head, tail = s[:len(s) // 3], s[-(len(s) // 5):]
if not re.search(r"load this skill again|read this file", head, re.I):
    fails.append("placement/reload: opening third does not tell the model to load the skill again")
if "## What gets lost first" not in tail:
    fails.append("placement/anchors: closing anchor section is not in the final fifth")

# 12. Composition: the generic loop rules (return contract, masking, budget,
#     checkpoint) belong to the sibling skill. Two copies of a rule drift, and
#     the drift is invisible until they disagree mid-loop.
if "running-long-autonomous-loops" not in s:
    fails.append("composition: no cross-reference to running-long-autonomous-loops")

# 13. Hollowing-out: a section reduced to its heading keeps every substring the
#     checks above look for. These are the rules whose absence has actually cost
#     an iteration, so each names the keyword family that must survive an edit.
SECTION_KEYWORDS = {
    "## The quality bar": ["mutat", "injection", "spread", "exit code", "allocs/op"],
    "## Bugs: the causal chain": ["five whys", "assumption", "quantifier"],
    "## What gets lost first": ["premise", "mutation", "iteration number", "batch"],
}
for heading, needed in SECTION_KEYWORDS.items():
    if heading not in s:
        fails.append(f"structure: no '{heading}' section")
        continue
    sec = s.split(heading)[1].split("\n## ")[0].lower()
    missing = [k for k in needed if k not in sec]
    if missing:
        fails.append(f"hollowed/{heading.strip('# ')}: section lost {missing}")

# 14. A body this far under size is not the standing order any more, whatever
#     substrings it kept. The real file runs ~1800 words.
if len(s.split()) < 600:
    fails.append(f"degenerate: body is {len(s.split())} words, under the 600 floor")

print("DETERMINISTIC GATE")
print("  FAIL:", len(fails))
for f in fails:
    print("   FAIL", f)
print("  WARN:", len(warns))
for w in warns:
    print("   warn", w)
sys.exit(1 if fails else 0)
