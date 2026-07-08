// Package quic implements the QUIC v1 transport (RFC 9000) for the HTTP/3
// client. It is being built in phases (see docs/HTTP3_DESIGN.md); this file set
// currently provides the A-layer frame codec: parsing and serialization of the
// QUIC frames a client sends and receives (RFC 9000 §19), decoupled from packet
// protection (RFC 9001) and the connection engine.
//
// Frames live inside a decrypted QUIC packet payload — a byte slice, not a
// stream — so the parser operates over a []byte and dispatches each frame to a
// FrameHandler (mirroring the frame package's Handler-visitor read model). All
// multi-byte fields are QUIC variable-length integers (internal/bytesx), which
// is a different encoding from the HPACK/QPACK prefixed integer.
//
// Like frame, hpack, and qpack, the codec does no networking and is NOT safe
// for concurrent use; the owning connection serializes access.
package quic
