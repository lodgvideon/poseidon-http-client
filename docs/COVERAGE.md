# Coverage Policy

## Current floor (E.1 — Phase D.5)

Per-package statement coverage gate is **90%**, enforced by
`scripts/coverage-gate.sh` from the `coverage` CI job.

| Package                 | Current | Floor |
|-------------------------|--------:|------:|
| `internal/bytesx`       |   96.9% |   70% |
| `frame`                 |   93.3% |   90% |
| `hpack`                 |   95.7% |   90% |
| `conn`                  |   91.4% |   90% |
| `client`                |   90.1% |   90% |

All packages at or above the ≥90% `v0.1.0` acceptance criterion.

## Spec target (acceptance criterion)

[Phase A spec §11](superpowers/specs/2026-05-02-poseidon-frame-layer-design.md)
calls for **≥ 90% per package** as one of the conditions for tagging `v0.1.0`.
Target reached in E.1 (2026-05-21).

## Ratchet protocol

When raising the floor:

1. Add tests that close the gap.
2. Bump the threshold in `.github/workflows/ci.yml` (`coverage-gate.sh ... N`).
3. Update the table above with the new numbers.
4. Never lower the floor.
