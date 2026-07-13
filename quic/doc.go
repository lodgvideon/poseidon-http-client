// Package quic implements a complete QUIC v1 client transport from scratch for
// the HTTP/3 client (design in docs/HTTP3_DESIGN.md). It is the full engine, not
// just a codec: connection establishment and lifecycle (RFC 9000), stream
// multiplexing and flow control, the TLS 1.3 handshake over CRYPTO streams and
// AEAD packet + header protection with key update (RFC 9001), and ACK-based loss
// detection, PTO, RTT estimation, and NewReno congestion control with pacing
// (RFC 9002).
//
// The frame codec is one layer within it: frames live inside a decrypted QUIC
// packet payload — a byte slice, not a stream — so the parser operates over a
// []byte and dispatches each frame to a FrameHandler (mirroring the frame
// package's Handler-visitor read model). All multi-byte fields are QUIC
// variable-length integers (internal/bytesx), a different encoding from the
// HPACK/QPACK prefixed integer.
//
// The package does no application-level HTTP; http3 layers on top. It is NOT
// safe for concurrent use except where documented; the owning connection
// serializes access.
package quic
