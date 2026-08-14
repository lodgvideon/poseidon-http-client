# Coverage Policy

## Current floor (F.1 — HTTP/1.1 fallback)

Per-package statement coverage gate is **80%**, enforced by
`scripts/coverage-gate.sh` from the `coverage` CI job.

The table below is every package the gate sees, measured 2026-08-14. It used to
list six of them — `http3`, `quic`, `qpack`, `grpc`, `header` and
`internal/bufx` had shipped without ever being added, so a reader checking
whether a package was covered got no answer for four of the largest. The gate
itself was never fooled: `coverage-gate.sh` enumerates packages from `go list`
and fails any non-`examples` one with no test files, so the omission was in the
documentation only.

| Package                 | Current | Floor |
|-------------------------|--------:|------:|
| `header`                |  100.0% |   80% |
| `internal/bytesx`       |  100.0% |   70% |
| `hpack`                 |   97.3% |   80% |
| `http1`                 |   97.1% |   80% |
| `internal/bufx`         |   96.9% |   80% |
| `frame`                 |   94.7% |   80% |
| `grpc`                  |   93.5% |   80% |
| `quic`                  |   90.8% |   80% |
| `client`                |   90.1% |   80% |
| `trace`                 |   89.9% |   80% |
| `qpack`                 |   87.7% |   80% |
| `http3`                 |   87.6% |   80% |
| `conn`                  |   85.5% |   80% |

Every package clears the ≥80% CI gate. Four sit below the ≥90% spec acceptance
bar — `trace`, `qpack`, `http3` and `conn` — which the bar's own scope explains
for three of them: it was written for the Phase A/B/C packages, and the HTTP/3
stack and the tracer arrived after it. `conn` is the one that genuinely drifted,
from 90.2%.

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
