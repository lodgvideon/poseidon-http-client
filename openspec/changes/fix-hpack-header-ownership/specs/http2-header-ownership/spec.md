# Spec Delta

## Purpose

Keep decoded HTTP/2 header events stable while compression state and incoming frame buffers are reused by the connection.

## ADDED Requirements

### Requirement: Preserve decoded fields through delivery
The client SHALL deliver header names, values, ordering, duplicates, and indexing metadata as decoded, even when subsequent fields in the same block evict or overwrite earlier compression-table entries.

#### Scenario: Response fields are evicted within a block
- **WHEN** a response, informational response, or trailer block references a dynamic entry and then replaces its decoder storage
- **THEN** the earlier field retains its original name and value during validation and delivery

#### Scenario: Promised request fields are evicted within a block
- **WHEN** a PUSH_PROMISE references a dynamic entry and then replaces its decoder storage
- **THEN** the promised request retains all original fields during validation and delivery

#### Scenario: Block spans multiple frames
- **WHEN** a header block is split across HEADERS or PUSH_PROMISE and CONTINUATION frames
- **THEN** delivered fields have the same content and order as the equivalent complete block

### Requirement: Preserve event ownership until release
Delivered header fields SHALL remain valid until their event is released. Reusing decoder storage or appending to one field SHALL NOT change another live field or another event's fields.

#### Scenario: Event is queued while another block is decoded
- **WHEN** an event remains queued or held while later blocks reuse compression storage
- **THEN** the earlier event's fields remain unchanged until it is released

#### Scenario: Consumer appends to a field
- **WHEN** the consumer appends bytes to a delivered name or value
- **THEN** neighboring field content remains unchanged

### Requirement: Preserve refusal and error behavior
The client SHALL retain its existing connection and stream error classifications and SHALL NOT deliver a partially decoded or rejected header block. A stream-level refusal SHALL still consume the full HPACK block to preserve connection compression state.

#### Scenario: Decode fails after emitting a valid field
- **WHEN** a valid field is followed by invalid HPACK input
- **THEN** the client returns the existing connection error and delivers no header event

#### Scenario: Promised stream is refused
- **WHEN** a decoded promise is refused while the connection remains usable
- **THEN** subsequent blocks referencing entries introduced by the refused promise still decode correctly
