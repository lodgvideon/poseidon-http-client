# Tasks

## 1. Reproduce and fix field ownership

- [x] 1.1 Add response and PUSH_PROMISE regressions and observe old fields being overwritten on the merged baseline; retain the failing test output.
- [x] 1.2 Copy callbacks directly into pooled HeaderBlock storage, preserve growth/clamping/metadata, and pass the focused regression tests.
- [x] 1.3 Preserve validation, refusal, and error cleanup; pass related connection tests and add the RFC coverage entry and work log.

## 2. Integration verification

- [x] 2.1 Pass connection/HPACK race tests and existing connection/gRPC allocation gates, then run the root-module race suite; record exact toolchains and results.
- [x] 2.2 Run lint, complexity/duplication analysis, and generated mutation checks; resolve attributable failures and document existing or unavailable checks.
- [x] 2.3 Independently review implementation and tests, validate OpenSpec, inspect the public diff for sensitive material, and prepare the next PR.
