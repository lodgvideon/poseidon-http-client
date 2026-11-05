# Verification and work log

Baseline: `16f6ff0f0823c33a474676236272acafddfdbf50` (`main`, merged PR #969).
Scope: HTTP/2 response and PUSH_PROMISE field ownership in `conn`.

## Root cause: five whys

1. Why could a caller receive a different header? A later field insertion overwrote an earlier decoded field's bytes.
2. Why could insertion modify an emitted field? Its slices still referenced the HPACK decoder's reusable dynamic-table arena.
3. Why did the handler retain those slices? It appended field descriptors to `scratch` during callbacks and copied bytes only after `DecodeBlock` returned.
4. Why was that copy too late? Eviction and arena reuse can happen between two callbacks within the same block.
5. Why did the event's ownership guarantee not protect it? Ownership began after the borrowed bytes' lifetime had ended; validation also read the overwritten bytes.

The fix copies each field directly into the final pooled `HeaderBlock` during its callback. Validation and delivery use that owned block. This removes the intermediate borrowed-field list and retains one copy of each field.

## Reproduction and regression coverage

The fixture sets a 64-byte dynamic-table limit, seeds `x-old: a`, then references that entry and inserts `x-new: b`. Each entry occupies 38 table bytes, forcing eviction and arena reuse. On the baseline, the earlier field changes before delivery. With `X-old` as the seed, the overwrite can also hide an invalid uppercase name from validation.

Ten subtests failed on the baseline and passed after the fix:

- Response, informational response and trailers, each in HEADERS and HEADERS+CONTINUATION form.
- PUSH_PROMISE with and without CONTINUATION.
- Invalid uppercase field followed by eviction, for response and promise.

The delivery tests preserve order, duplicate names and indexing modes, clear the input buffers, and reuse the decoder before reading the queued event. They include a 600-byte field and check capacity clamping. Two further cases reject an HPACK index-zero error after valid fields without delivering a partial event.

Reproduce the RED control by copying `conn/hpack_ownership_test.go` into a checkout of the baseline and running:

```sh
GOTOOLCHAIN=go1.25.13 go test ./conn \
  -run 'TestHandler_(HeaderFieldsSurviveHPACKReuse|PushPromiseFieldsSurviveHPACKReuse|HPACKReuseCannotHideInvalidField)$' \
  -count=1 -timeout=60s
```

The focused regression run also passed with `-race` on Go 1.25.13. Raw local logs retain the baseline failures; the fixture and command above reproduce them without publishing machine-specific paths or large field dumps.

`TestHeaderBlock_AppendFieldGrowthKeepsOneBackingBuffer` starts with a local 8-byte buffer and forces growth with 17 bytes of fields. It checks that both fields refer to the new backing array, retain their content/indexing modes, and have clamped capacities. This closes the pooled-buffer test's limitation: a 600-byte field alone does not guarantee growth if the pool returns a previously enlarged block.

## Ownership review

An independent reviewer checked every decode, validation, refusal and delivery exit. A partial decode is released on error. Each delivery function defers release through its local pointer and clears that pointer only after successful enqueue. No field is accessed after transfer; the consumer may release immediately. Survivable PUSH_PROMISE refusals still decode the entire block, preserving compression state.

The review found no blocking defects. Existing behavior for events abandoned during stream reset is unchanged.

## Validation results

All commands use Go 1.25.13 on darwin/arm64 unless stated otherwise.

| Check | Result |
| --- | --- |
| Focused connection regressions with `-race` | Passed |
| Root-module `go test -race -count=1 -timeout=180s ./...` | Passed, exit 0; all 15 tested packages |
| `TestConn_Roundtrip_AllocsPerRequest` | Passed: 0.0 allocations/request |
| `TestInvokeInto_AllocsPerCall` | Passed: Invoke 2.0, InvokeInto 1.0 allocations/call |
| `make lint GOLANGCI_LINT=<golangci-lint-2.5.0>` | Passed: vet and lint for root, contrib/prometheus and test/interop/quic; 0 issues |
| `golangci-lint fmt --no-config --enable goimports --diff` on four changed Go files | Passed: no formatting diff |
| `go test ./conn -count=1 -timeout=180s -coverprofile=coverage.out` | Passed: 89.5% statement coverage |
| Strict OpenSpec validation and `git diff --check` | Passed |

The installed golangci-lint 2.14 initially reported two attributable findings, now fixed, and the same 13 unrelated findings as the prior baseline run. The repository pins 2.5.0 in CI; that version was installed into a temporary tools directory and the complete `make lint` target passed. No module or dependency files changed.

### Complexity and duplication

The new `acquireHeaderBlock`, `appendField`, `rebindFields` and `decodeHeaderBlock` helpers each have 100% statement coverage and CRAP scores 1, 2, 2 and 2. `emitHeaderBlock` has 100% coverage / CRAP 8. Dryer at 0.8 over the two changed production files reports no duplicate candidates.

Crapper at threshold 10 exits 2 because the existing validators remain above the threshold: `validateResponseFields` 14.0212 and `validatePushedRequest` 10.0579. Their complexity did not change. `handlePushPromiseBlock` also retains baseline complexity and CRAP 10, outside ub-mode's strict under-10 target. This patch does not claim to bring the entire handler below that target; broader validation refactoring is outside the ownership fix.

### Generated mutation checks

The native ub-mode `mutator` ran 19 selected mutations in the new HeaderBlock helpers and decode/response/promise delivery functions: **19 killed, 0 survived, 0 selected uncovered**. Two unmodified controls passed. Captured output confirmed all kills were test failures, with no build failures or timeouts. Source hashes matched the final four-file checkout.

The growth-condition mutation (`!=` to `==`) initially survived because fields retained their content through detached old arrays. The deterministic growth test now detects that ownership violation. The full two-file inventory contains 116 sites; 97 were outside this focused run. Four uncovered sites belong to unchanged fallback branches for absent/invalid status or absent promised authority.

The repository-standard `make mutation` was attempted once and failed before executing any mutants: Docker did not expose the checkout, reporting `not in a Go module: open /go.mod: no such file or directory`. The native run is supplemental evidence, **not a passing Gremlins CI gate**. Only the image newly downloaded for this failed attempt was removed; existing Docker resources were preserved.

Commands in the isolated snapshot (the actual mutation run wrapped the shown test command to retain output, exit status, duration and source hashes):

```sh
crapper --root "$SNAPSHOT" \
  --coverage-command 'go test ./conn -count=1 -timeout=180s -coverprofile=coverage.out' \
  --threshold 10 conn/headerblock.go conn/handler.go
dryer --root "$SNAPSHOT" --threshold 0.8 conn/headerblock.go conn/handler.go
mutator --root "$SNAPSHOT" --use-existing-coverage --mutate-all \
  --lines 54,97,98,114,117,118 \
  --test-command 'go test ./conn -run "TestHandler_|TestHeaderBlock" -count=1 -timeout=90s' \
  --timeout-factor 30 --max-workers 1 --verbose conn/headerblock.go
mutator --root "$SNAPSHOT" --use-existing-coverage --mutate-all \
  --lines 433,442,451,455,461,614,624,637,644,674,686,705 \
  --test-command 'go test ./conn -run "TestHandler_|TestHeaderBlock" -count=1 -timeout=90s' \
  --timeout-factor 30 --max-workers 1 --verbose conn/handler.go
```

The selected lines cover every generated mutation in `acquireHeaderBlock`, `appendField`, `rebindFields`, `decodeHeaderBlock`, `emitHeaderBlock` and `handlePushPromiseBlock`; line 54 has two sites. Before importing coverage into the native tool, the module prefix in a copy of `coverage.out` was replaced with the absolute snapshot path. The original profile was retained; coverage was not bypassed.

## Work log

1. Verified PR #969 was merged and branched from its fresh `main` commit.
2. Traced decoder callback lifetimes through response and promise handlers; selected this P1 corruption defect as the next isolated repair.
3. Wrote the OpenSpec proposal/design and failing regressions before production edits; reproduced both corruption and validation bypass.
4. Moved copying into the callback, removed borrowed scratch storage, and reviewed all cleanup/transfer paths.
5. Ran focused race tests and allocation gates; requested independent implementation and test criticism.
6. Added RFC matrix entries and ran complexity, duplication and generated mutation checks. A surviving growth mutation prompted a deterministic backing-buffer test.
7. Repeated independent review of the final tests, implementation and specification; no blocking findings remained. Passed the complete root-module race suite and all three modules' pinned lint checks.
8. Audited complete staged files and the prepared PR description for sensitive material and evidence accuracy; corrected an unsupported claim about unreachable status fallback branches.
