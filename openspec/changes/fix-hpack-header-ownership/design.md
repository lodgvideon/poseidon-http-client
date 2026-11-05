# Design

## Context

See proposal.md for the failure. The handler currently collects decoder-owned field slices and copies them after DecodeBlock returns. HeaderBlock already pools bytes and field descriptors together, clamps field capacities, and transfers ownership through StreamEvent.Release.

## Goals / Non-Goals

**Goals:** own bytes during the callback; preserve the existing event lifetime, validation order, and warmed allocation ceiling; keep one backing byte array per block.

**Non-goals:** change decoder storage, add per-field allocations, or fix other HPACK protocol defects in this patch.

## Decisions

1. Decode directly into a pooled HeaderBlock using a shared field-copy method. The standalone NewHeaderBlock path keeps its exact preallocation and uses the same copying logic. Cloning fields into a scratch list before the final copy would add allocations and copy the block twice.
2. Grow the byte buffer once for each incoming name/value pair. On growth, rebind existing fields from their lengths into the new array; preserve capacities and indexing metadata. This keeps one owned array while avoiding a new descriptor representation or unconditional rebinding on every block.
3. Remove handler scratch and pass owned fields explicitly to validation/classification. These private methods have no other callers. No handler reference survives transfer to the event consumer.
4. Release a partially decoded block on decoder error. Each delivery function keeps a local block pointer and defers releasing it; successful enqueue clears that pointer, transferring ownership. Failed validation, refused push, vanished parent, and overflow retain handler ownership and release exactly once. Complete decoding still precedes every survivable push refusal.

## Risks / Trade-offs

- Early allocation of a pooled block also affects refused headers; verify existing refusal/error tests and preserve error mapping.
- Buffer growth must not leave fields attached to superseded storage; test small and growing blocks with capacity-clamped views and metadata.
- A consumer can release immediately after enqueue; perform no field access after successful delivery.
- Pool return errors can be invisible to ordinary tests; run existing warm allocation gates in addition to race/regression checks and review every ownership exit.

## Migration Plan

No API or data migration. Revert this change to restore the previous implementation if needed; the original corruption would return.
