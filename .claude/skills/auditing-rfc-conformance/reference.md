# RFC Conformance Audit — Reference

Operational details for the pipeline in [SKILL.md](SKILL.md). Everything here
was validated in the July 2026 HTTP/1.1 audit (RFC 2616 + 9110/9112 →
`docs/rfc-analysis/`): 2530 facts extracted, 7 disputes (all cosmetic),
196 deltas, 256-item checklist, 256 code verdicts.

## 1. Fetch + section map

```bash
curl -s -L -o rfcNNNN.txt https://www.rfc-editor.org/rfc/rfcNNNN.txt
```

Section map = Grep `^[0-9]+(\.[0-9]+)*\.?\s+[A-Z]` with line numbers. Also grep
the tail (`^Appendix|^Acknowledge|^Index`) to bound the last unit.

## 2. Unit sizing

~200–600 source lines per extraction unit. Split big sections (RFC 9110 §15 →
15.1-15.3 / 15.4 / 15.5-15.6); merge tiny ones (§4-§5). One unit = one
extract agent + two verify agents. 23 units covered 9110+9112; 25 covered 2616.

## 3. Extraction schema (schema-forced agent output)

```json
{ "section": "...", "title": "...",
  "facts": [{
    "id": "SEC-N",
    "requirement": "one-sentence paraphrase",
    "quote": "verbatim operative clause, <=30 words, COPIED from the read lines",
    "level": "MUST|MUST NOT|SHOULD|SHOULD NOT|MAY|REQUIRED|RECOMMENDED|OPTIONAL|informative",
    "applies_to": ["client","user-agent","server","origin","proxy","cache","gateway","tunnel","all"],
    "client_relevant": true,
    "reconcile": "what the client code must do / exactly what to check"
  }]
}
```

Prompt essentials: give exact `Read file_path/offset/limit` for the unit; "if
you cannot see it in the lines you read, do NOT invent it"; capture ABNF
productions as `informative` facts; do not merge distinct requirements; for
paginated RFCs name the page-header artifacts to ignore.

## 4. Verification schema

```json
{ "section": "...",
  "verdicts": [{ "id": "SEC-N",
    "quote_verbatim": true, "level_ok": true, "applies_ok": true,
    "verdict": "CONFIRMED|REFUTED", "correction": "exact problem + fix" }] }
```

Prompt essentials: same Read window as the extractor; "DEFAULT TO REFUTED when
unsure or the quote cannot be located"; allow only line-wrap/whitespace/
page-break differences; CONFIRMED requires all three checks true.

Merge rule: fact is confirmed only when **both** verifiers CONFIRM. Anything
else → `VERIFY` flag with corrections inline. `<2` verifier results = not
verified (see resume trap), never "confirmed by default".

## 5. Delta between spec versions

Per topic (framing, fields, conn, methods, status, …) one agent reads BOTH
sources (old + current ranges) and emits:

```json
{ "id": "topic-N",
  "change_type": "added|removed|tightened|relaxed|moved|reworded_same|clarified|security_hardening|split_across_docs|default_changed",
  "summary": "...",
  "rfc_old": { "ref": "...", "quote": "verbatim or 'absent'" },
  "current": { "ref": "...", "quote": "verbatim or 'absent'" },
  "client_impact": "silent bug / security hole / no-op ...",
  "reconcile": "what to check in the code" }
```

Verify ×2: both quotes verbatim + change_type correct ("a rule that vanished is
removed, not relaxed"; SHOULD→MUST is tightened; brand-new rule is added).
Observed dispute class: mislabelled change_type, quotes fine.

## 6. Checklist distillation

Filter facts: `client_relevant && level ∈ {MUST, MUST NOT, REQUIRED, SHOULD,
SHOULD NOT, RECOMMENDED}`. Split hard (MUST-family) vs soft (SHOULD-family).
Emit checkbox markdown + JSON buckets for the reconciliation agents.
HTTP/1.1 2026 slice yielded 256 items (163 hard / 93 soft) from 1208 facts.

## 7. Code reconciliation

Bucket checklist by subsystem (~10–40 items). Per bucket:

- **Judge** (high effort): reads bucket JSON + named source files + greps
  tests + coverage doc. Verdict per item:
  `PASS` (implemented AND tested) | `UNTESTED` | `PARTIAL` | `FAIL` |
  `NOT_APPLICABLE` (out of scope / caller's job — must justify). Evidence =
  file:line or symbol + test name. Tell the judge which behaviours are
  deliberately out of scope (caching, redirects…) so N/A is principled.
- **2 adversarial verifiers**: receive only non-PASS findings; instructed
  "DEFAULT real_gap=false — assume the code complies unless re-reading proves
  the gap". For N/A findings, real_gap=true means the dismissal was wrong.
- **Merge**: both false → PASS (or justified N/A); both true → confirmed gap;
  split → REVIEW (human). Never auto-resolve splits.

## 8. Workflow mechanics

- `pipeline(units, extract, verify, render)` — no barrier; verification of
  unit A runs while unit B still extracts.
- Extract on `fable`, verify/judge on default model with `effort:'high'`.
- Results: parse `<transcriptDir>/journal.jsonl` (`"type":"result"` lines),
  NOT the truncated task notification. Journal may contain duplicate result
  lines after a resume — dedup by `agentId`.
- **Doc-collision trap**: when two RFCs share section numbers, the verify
  results' `bucket`/`section` strings are agent-paraphrased. Bind verify →
  extract by **id-set overlap** or static sec→doc map + fact-count, never by
  string equality, never by grepping agent transcripts for file paths.
- **Resume**: agents with `started` but no `result` line died (session limit,
  crash). `Workflow({scriptPath, resumeFromRunId})` replays cached results and
  re-runs only the dead ones. Recount disputes after — missing verifiers
  masquerade as disputes (observed 220 → real 2).
- Windows PowerShell 5.1 parsers: ASCII-only `.ps1` (ANSI read mangles
  em-dashes → parse errors), build literal backticks via `[char]96`.

## 9. Calibration numbers (July 2026 run)

| Metric | Value |
|--------|-------|
| Facts per unit (extract) | 20–75 |
| Verifier refutation rate on good extraction | 0.3–0.6%, all cosmetic |
| Refutation rate of a broken/unread verifier | 0% — treat as failure |
| Under-extraction of a single freeform pass vs schema pass | ~4× fewer facts |
| Memory-recall citation accuracy (measured) | 0/5 correct |
| Judge false-positive gaps overturned by adversarial pass | multiple per run — the layer pays for itself |
