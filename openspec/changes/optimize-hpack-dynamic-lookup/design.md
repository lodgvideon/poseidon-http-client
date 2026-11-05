# Design

## Context

See `proposal.md` for motivation. The encoder and decoder share an arena-backed FIFO table, but only the encoder searches by bytes. The encoder owns copied input bytes; callers may reuse their buffers immediately. Arena compaction moves offsets, and HPACK indexes shift whenever an entry is inserted.

## Goals / Non-Goals

**Goals:** expected constant-time lookup with respect to the number of live entries; identical wire bytes and indexing decisions; no additional per-field key-copy allocation; correct reset, resizing, eviction, and hash-collision behavior.

Hashing still takes time proportional to the input byte lengths. This is not a worst-case O(1) guarantee. Collision handling must prioritize correctness.

**Non-goals:** removing the required input copy, replacing arena storage, changing Huffman policy, or combining protocol fixes with this refactor.

## Decisions

1. Attach an optional index to the existing table, enabled only by the encoder. Allocate its maps on the first successful dynamic insertion. The table's existing insertion, eviction, and clear operations remain the single owners of lifecycle updates. The decoder does not hash fields or allocate maps.
2. Use randomized `hash/maphash` fingerprints for name and full-field lookup. Check the newest matching name first; hash the value only when that candidate has another value. Map values are monotonically increasing insertion IDs. Convert an ID to the current HPACK index by subtracting it from the newest ID and adding one. Ring growth and arena compaction therefore need no index rebuild.
3. Compare actual bytes before accepting a map candidate. On a hash collision, fall back to the existing scan. A collision may affect performance but cannot change encoded headers. Eviction deletes a map entry only when its stored ID belongs to the evicted entry; newer matches survive.
4. Retain arena ownership and copying in this unit. String-key maps are simpler but safe insertion requires new string copies; unsafe strings would couple map keys to mutable storage. A separate immutable-string encoder table also changes insertion allocation behavior. Neither is needed to remove the current linear search.
5. Keep benchmarks alongside package tests. Allocation-producing churn benchmarks must be opt-in because the repository's benchmark gate treats every printed allocation as a failure. Capture both baseline and changed results with the same command and host.

## Risks / Trade-offs

- Hashing and map access can cost more than scanning a tiny table or its newest entry. Measure these separately from old-entry hits and misses, and report any regression.
- Map maintenance adds work to insertion and eviction. Measure churn, not only warmed repeated headers.
- Two indexes increase per-encoder memory. Keep them bounded by the live table and clear them with the table.
- A stale ID could reference the wrong header after eviction or reset. Characterization tests cover those transitions and buffer ownership; collision tests verify byte comparison.
- Arena compaction still copies live bytes. Record its cost and handle storage changes as a later unit with dedicated memory and lifetime tests.

## Verification and rollback

Run baseline characterization tests and benchmarks before editing production code. After implementation, run HPACK tests with `-race`, relevant connection tests, the repository lint target, focused quality tools, and a broader test suite when practical. Use protocol reference implementations only in tests. Any unavailable check or existing failure is recorded explicitly.

The change preserves the public API and data on the wire. Reverting its code and test diff restores the previous lookup without migration.
