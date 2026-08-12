"""Deterministic gate for the loop prompt, run BEFORE any judgement about quality.

Each check encodes a rule the model docs state prescriptively, so a failure here
is a documented defect rather than an opinion.
"""
import io, re, sys

import os

# The prompt sits next to this script, so the gate travels with the repo rather
# than pointing at whoever last ran it.
P = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backlog-loop.md')
s = io.open(P, encoding="utf-8").read()
lines = [l for l in s.split("\n")]
body = [l for l in lines if l.strip() and not l.startswith("#")]

fails, warns = [], []

def check(ok, name, detail):
    (fails if not ok else []).append(f"{name}: {detail}") if not ok else None

# 1. Opus 5: generic self-verification instructions must not appear.
banned = ["перепроверь", "double-check", "re-verify", "ещё раз проверь свою",
          "финальный шаг проверки", "верификац" + "ионный шаг"]
hits = [b for b in banned if b.lower() in s.lower()]
if hits:
    fails.append(f"opus/generic-verification: found {hits}")

# 2. Fable 5: no instruction to reproduce internal reasoning as response text.
reasoning = ["изложи ход мысли", "покажи рассуждени", "опиши свои размышлени",
             "show your reasoning", "transcribe your reasoning"]
hits = [b for b in reasoning if b.lower() in s.lower()]
if hits:
    fails.append(f"fable/reasoning-extraction: found {hits}")

# 3. Both: an explicit scope constraint must exist (documented need).
if not re.search(r"сверх задачи|beyond what the task", s, re.I):
    fails.append("scope: no explicit scope constraint")

# 4. Fable 5: autonomous pipelines need the no-questions guard.
if not re.search(r"без наблюдател|не отвечает по ходу", s):
    fails.append("fable/autonomy: no 'user is not watching' guard")

# 5. Fable 5: long sessions need the context-budget reassurance.
if not re.search(r"контекст.{0,40}(хватает|достаточно|не заканчивается)", s, re.I | re.S):
    warns.append("fable/context-budget: no reassurance against self-truncation")

# 6. Fable 5: 'give the reason, not only the request' — intent should be stated.
if not re.search(r"чтобы|ради|нужен для|цель|потребител", s.split("## Итерация")[0], re.I):
    warns.append("fable/intent: opening states the task but not why it matters")

# 7. Opus 5 + Sonnet 5 code review: finding stage must ask for coverage,
#    not self-filtering by severity.
if re.search(r"только (важные|серьёзные|высок)", s, re.I):
    fails.append("review/self-filter: prompt tells the model to filter by severity")
if not re.search(r"включая мелкое|фильтровать будешь отдельно|report every", s, re.I):
    warns.append("review/coverage: no explicit 'report everything' at the finding stage")

# 8. Prescriptiveness budget (Fable 5: too prescriptive degrades output).
imperative = sum(1 for l in body if re.match(r"^\s*[-–]?\s*(Не |Делегируй|Прогоняй|Веди|Начни|Проверяй|Смержи|Возьми|Останавливайся|Нумеруй|Закончи|Сверь)", l))
warns.append(f"size: {len(body)} content lines, ~{len(s.split())} words, {imperative} imperative openers")

# 9. Contradiction: the two subagent branches must be mutually exclusive and labelled.
if len(re.findall(r"Под Opus 5", s)) != 1 or len(re.findall(r"Под Fable 5", s)) != 1:
    fails.append("branching: model-conditional subagent guidance is not clearly labelled once each")

# 10. Every quality rule should carry its reason (why it exists), not just the rule.
qs = s.split("## Порог качества")[1].split("##")[0]
rules = [b for b in qs.strip().split("\n\n") if b.strip()]
noreason = [r.split("\n")[0][:48] for r in rules
            if not re.search(r"—|:|потому|выглядит как|не разница|всегда", r)]
if noreason:
    warns.append(f"traceability: quality rules without a stated reason: {noreason}")

print("DETERMINISTIC GATE")
print("  FAIL:", len(fails))
for f in fails:
    print("   ✗", f)
print("  WARN:", len(warns))
for w in warns:
    print("   !", w)
sys.exit(1 if fails else 0)
