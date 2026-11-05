# Proposal

## Why

At `f61188010b90445be8206635573c20d34f482162`, HPACK encoding scans the dynamic table for every non-static match. Old entries and misses therefore become slower as the table grows, while the existing warm-request benchmark exercises only a small table.

## What Changes

- Add an encoder-only hash index for exact and name-only dynamic-table matches.
- Preserve the existing wire representation, FIFO eviction, indexing modes, and ownership of caller buffers.
- Measure oldest/newest hits, misses, realistic warm requests, and insertion churn before and after the change.
- Record the separately reproduced protocol defects and arena-compaction costs for subsequent, independently verified changes.

## Capabilities

No wire or public API behavior changes. This is a performance refactor, so `.openspec.yaml` sets `skip_specs: true`; acceptance criteria live in the design and verification tasks.

## Impact

Changes are confined to `hpack` encoder lookup and dynamic-table lifecycle hooks, associated tests, benchmarks, and local review records. The decoder does not build an index. No Go version, module dependencies, Huffman policy, or connection locking changes are required.
