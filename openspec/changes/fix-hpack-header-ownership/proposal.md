# Proposal

## Why

At `16f6ff0f0823c33a474676236272acafddfdbf50`, response and PUSH_PROMISE handlers retain borrowed HPACK fields until the whole block has decoded. Eviction and reuse of the decoder arena can overwrite an earlier field before validation or delivery, changing the response received by the caller.

## What Changes

- Copy each decoded field into the event's pooled HeaderBlock during its decoder callback.
- Validate and deliver that same owned block; return it exactly once when decoding, validation, or delivery fails.
- Preserve field order, duplicates, indexing metadata, capacity clamping, and ownership until StreamEvent.Release.
- Cover complete and fragmented responses and promises with regression tests, including later decoder reuse.

## Capabilities

### New Capabilities

- `http2-header-ownership`: decoded HTTP/2 event fields remain stable until their consumer releases them.

### Modified Capabilities

None.

## Impact

Private connection handler and HeaderBlock internals, regression tests, and the RFC coverage matrix. Public APIs, dependencies, Go version, HPACK wire format, and connection error classifications remain unchanged. Table-size signaling, integer ceilings, and streaming-input limits remain separate follow-ups.
