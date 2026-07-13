// Package quic implements the QUIC v1 client transport engine (RFC 9000) from
// scratch, with no quic-go dependency, for the HTTP/3 client. It is the full
// client connection engine, not just a codec: the QUIC frame codec (parsing and
// serialization of the frames a client sends and receives, RFC 9000 §19),
// packet protection and the TLS 1.3 handshake over CRYPTO streams (RFC 9001,
// including 1-RTT key derivation and key update), and ACK-based loss recovery
// with NewReno congestion control and PTO probing (RFC 9002). On top of that it
// exposes streams, bidirectional flow control, connection-ID issuance and
// rotation, and CONNECTION_CLOSE / Retry to the http3 package above it.
//
// Frames live inside a decrypted QUIC packet payload — a byte slice, not a
// stream — so the parser operates over a []byte and dispatches each frame to a
// FrameHandler (mirroring the frame package's Handler-visitor read model). All
// multi-byte fields are QUIC variable-length integers (internal/bytesx), which
// is a different encoding from the HPACK/QPACK prefixed integer.
//
// The engine drives a caller-supplied datagram transport (the PacketConn
// interface) rather than owning a socket, and like frame, hpack, and qpack it
// is NOT safe for concurrent use; the owning connection serializes access.
package quic
