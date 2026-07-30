---
name: auditing-rfc-conformance
description: Use when validating an implementation against an RFC or other spec, quoting normative requirement text (MUST/SHOULD/MAY), building a conformance checklist, diffing spec versions (e.g. RFC 2616 vs 9110/9112), or answering "what does the spec say" / "is this conformant".
---

# Auditing RFC Conformance

## Overview

Facts come from fetched spec bytes, never recall, and every extracted fact and
every code verdict must survive two independent default-refute verifiers.

Measured baselines without this method: citing RFC text from memory was wrong
**5/5** times; a single-pass extraction found **~25%** of the normative facts a
systematic pass found; WebFetch HTML summarization silently dropped passages.

## Iron rules

1. **Never cite spec text from recall.** Download the raw `.txt` from
   rfc-editor.org to a local file and `Read` it with line offsets. Quotes are
   copied bytes. WebFetch/HTML summarization is lossy — treat it as recall.
2. **Every fact is structured**: verbatim quote (≤30 words, operative clause) +
   normative level (MUST/MUST NOT/SHOULD/…/informative for ABNF) + audience
   (client/server/proxy/…) + client_relevant flag + one-line reconcile note
   ("what to check in the code").
3. **Two independent verifiers per fact**, both re-reading the same source
   lines, instructed to REFUTE by default when unsure. They check three things:
   quote-verbatim, level correct, audience correct.
4. **Disputes are flagged `VERIFY` with the correction inline — never dropped.**
5. **A verifier that refutes 0 of N is broken, not thorough.** Healthy rate on
   good extraction ≈ 0.3–0.6%, all cosmetic (dropped inline `(Section X.Y)`
   refs, collapsed multi-line ABNF). Zero refutations means the verifier
   didn't read the source.
6. **Code reconciliation is adversarial**: a judge reads checklist + code +
   tests and rules PASS/FAIL/PARTIAL/NOT_APPLICABLE/UNTESTED with file:line
   evidence; then two verifiers re-read the code trying to *overturn* every
   non-PASS (default `real_gap=false`). Both overturn → PASS. Both confirm →
   real gap. Split → REVIEW for a human. This killed false-positive FAILs in
   production and validated 142 deliberate N/A calls.

## Pipeline

```
fetch .txt → grep section map (line numbers) → chunk into units (~200–600 lines)
  → extract per unit (schema-forced, model: fable)
  → verify ×2 per unit (default-refute, model: opus high-effort)
  → parse journal → facts catalog (VERIFY-flagged disputes)
  → [optional] delta vs other spec version (both quotes verbatim, change_type)
  → distill checklist (client_relevant × MUST/SHOULD family)
  → reconcile vs code (judge + 2 adversarial verifiers)
```

Orchestrate with the Workflow tool, `pipeline()` (no barrier between units).
Schemas, prompt skeletons, unit sizing, and journal parsing:
see [reference.md](reference.md).

If a verified catalog already exists for the spec (this repo:
`docs/rfc-analysis/`), cross-check against it instead of re-extracting;
divergence between your extraction and the catalog is a red flag either way.

## Traps (each cost real time in production)

| Trap | Fix |
|------|-----|
| Two RFCs share section numbers (9110 §6 vs 9112 §6) → id collision in results | Tag results by static sec→doc map + per-section fact-count; **never** grep agent transcripts for the file path (stale/injected paths mistag) |
| Session limit kills some verify agents mid-run | Journal shows `started` without `result`; resume with `resumeFromRunId` — cached agents replay, only dead ones re-run |
| "Disputed" count inflated after partial run | Missing verifiers ≠ refutations; recount after resume (220 → 2 in production) |
| Task notification truncates the result | `journal.jsonl` in the workflow transcript dir is the source of truth; parse it, not the notification |
| Old RFCs (2616) are paginated | Tell extractors to ignore page headers/footers and treat wrapped text as continuous; 9110/9112 are unpaginated |
| Quote "not found" by verifier but rule is right | Expected cosmetic class: extractor dropped inline `(Section X.Y)` or ABNF `;` comments — flag VERIFY, keep the fact |
| Requirement prose is stronger than its own `level` | Highest-value dispute class (RFC 9002 run): a `SHOULD NOT` written up as "MUST NOT", a lowercase "recommends" in an *informative* appendix tagged `RECOMMENDED`. Left in, the checklist manufactures false hard-MUST failures. Tell verifiers to compare the paraphrase against the quote, not only the quote against the source |
| A verify pass covers only the first N ids of its unit | Not a refutation — the remaining facts have **one** verifier, which the merge rule must treat as unverified. Re-verify exactly those ids with a fresh independent pair, then patch the rows |
| API `529 Overloaded` kills verify agents in bursts | Same recovery as a session limit (`resumeFromRunId`), but do not retry immediately — a resume launched while another big workflow is still running just re-earns the 529 |
| Resuming an already-**completed** run to retry a handful of dead agents | Measured: a resume of a killed run replayed 319 cached agents fine, but a resume of a *finished* run re-ran 292 of 322 from scratch and burned the whole session limit. Resume only a run that died mid-flight; for a few stragglers in a finished run, launch a tiny standalone workflow scoped to exactly those ids |
| Verifier agents write scratch probe files into the repo | They compile throwaway `_test.go` files to *prove* a gap. A `git add -A` while a reconciliation run is live commits them (caught `quic/zzverify_scid_test.go` in a fix commit). Commit fixes by explicit path, or `git status` right before staging |

## Red flags — stop and restart the step

- A quote you didn't copy from a `Read` of the downloaded file
- "The RFC says..." without a file+line you can point to
- One verification pass, or verifier that confirms everything
- Dropping a disputed fact instead of flagging it
- Judging code conformance without file:line evidence
- Reporting gap counts from a run that had agent errors

## Common mistakes

| Mistake | Consequence |
|---------|-------------|
| WebFetch the HTML instead of Read the .txt | Summarizer paraphrases/drops text (lost 4 passages in baseline test) |
| Extract without a schema | Prose lists, ~4× under-extraction, nothing machine-checkable |
| Verify with "check this is right" prompts | Confirmation bias; must be "REFUTE by default" |
| One combined judge+verify agent for code audit | No one kills false positives; N/A calls go unchallenged |
| Trust obsolete spec (2616) | Framing/security rules changed (TE+CL, invalid CL, obs-fold); always audit against the current documents and diff versions |
