# Coverage Policy

## Current floor (F.1 — HTTP/1.1 fallback)

Per-package statement coverage gate is **80%**, enforced by
`scripts/coverage-gate.sh` from the `coverage` CI job.

| Package                 | Current | Floor |
|-------------------------|--------:|------:|
| `internal/bytesx`       |   96.9% |   70% |
| `frame`                 |   92.8% |   80% |
| `hpack`                 |   95.9% |   80% |
| `conn`                  |   90.2% |   80% |
| `client`                |   90.1% |   80% |
| `http1`                 |   89.6% |   80% |

All packages at or above the ≥80% gate. The `conn` and `client`
packages were restored to ≥90% after the F.1 HTTP/1.1 fallback
additions (targeted tests for the new transport code paths:
H1.1 concurrent-dial cancellation, singleConn/managedPool warmup
guards, pool GOAWAY close, rate-limiter no-Done slow path, Shutdown
timer drain). `http1` sits just under 90% (89.6%); the remaining gap
is hard-to-reach partial-read error paths.

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
