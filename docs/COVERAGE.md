# Coverage Policy

## Current floor (Phase A)

Per-package statement coverage gate is **70%**, enforced by
`scripts/coverage-gate.sh` from the `coverage` CI job.

| Package                 | Current | Floor |
|-------------------------|--------:|------:|
| `internal/bytesx`       |   96.9% |   70% |
| `frame`                 |   79.0% |   70% |
| `hpack`                 |   72.3% |   70% |

The floor exists to **prevent regression** of what is already covered. It is
intentionally below the spec target so this gate can be wired into CI today
without rewriting the test suite.

## Spec target (acceptance criterion)

[Phase A spec §11](superpowers/specs/2026-05-02-poseidon-frame-layer-design.md)
calls for **≥ 90% per package** as one of the conditions for tagging `v0.1.0`.
The floor will be ratcheted up toward 90% before that tag.

## Known gaps to close before 90%

Lowest-covered surfaces (`go tool cover -func`):

- `hpack.Decoder.SetMaxDynamicTableSize`, `SetMaxHeaderListSize`, `Reset`
- `hpack.Encoder.SetMaxDynamicTableSize`, `SetMaxDynamicTableSizeLimit`,
  `Reset`
- `hpack.HeaderField.Size`
- `hpack.dynamicTable.compactArena` (size-update eviction path)
- `hpack.Decoder.decodeOne` streaming branches not exercised by current
  fragmenting tests (size update, padding-style edge cases)
- `frame.Framer.SetMaxHeaderListSize`, `SetReadBuffer`
- `frame.Framer.WriteDataPadded`, `WritePushPromise` padding/length-too-large
  branches

These are mostly small public-API setters and a few error branches; no
algorithmic rework is needed to reach 90%.

## Ratchet protocol

When raising the floor:

1. Add tests that close the gap.
2. Bump the threshold in `.github/workflows/ci.yml` (`coverage-gate.sh ... N`).
3. Update the table above with the new numbers.
4. Never lower the floor.
