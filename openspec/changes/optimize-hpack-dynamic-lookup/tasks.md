# Tasks

## 1. Establish the baseline

- [x] 1.1 Pin the reviewed checkout and verify the existing HPACK and connection race tests; record the commit and commands in the review log.
- [x] 1.2 Add lookup state-transition characterization and scaling/churn benchmarks; run them on unchanged production code and retain the baseline output.

## 2. Index dynamic fields

- [x] 2.1 Add the optional encoder index and lifecycle updates; verify byte-identical lookup choices through insertion, eviction, resizing, compaction, and reset.
- [x] 2.2 Verify hash collisions, caller-buffer reuse, and warmed allocation behavior with targeted tests; review tests using the repository's reviewing-tests workflow.
- [x] 2.3 Repeat baseline benchmarks, including small-table/newest-hit cases and churn; record improvements and regressions without extrapolating to HTTP throughput.

## 3. Integration verification

- [x] 3.1 Run the relevant race tests, broader repository checks, lint, and quality/mutation tools; record unavailable or failing checks explicitly. See `docs/reviews/hpack-2026-11-05.md` for the existing full-lint failures, blocked Gremlins run, and two surviving local mutations.
- [x] 3.2 Independently review the diff, validate the OpenSpec change, and verify no Go version/dependency or unrelated changes were made.

## Workflow follow-up

- Address the independently reproduced protocol defects as separate changes, starting with decoder callback ownership in the connection handler.
- Measure and redesign arena compaction separately if churn remains costly; preserve the required input-buffer ownership copy.
