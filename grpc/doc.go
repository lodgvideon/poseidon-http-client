// Package grpc implements the gRPC-over-HTTP/2 wire protocol on top of the
// poseidon conn layer.
//
// # Dependency order
//
// grpc → conn → frame, hpack. grpc also imports frame directly, for the
// RST_STREAM error codes it maps to gRPC status codes. No import of client,
// http1, http3, or any third-party gRPC library.
//
// This package is HTTP/2 only. That is not a choice made here: http3.Request
// carries a []byte body with no incremental send path, so the streaming call
// shapes cannot be expressed on that stack at all yet.
//
// Note the import-path collision with google.golang.org/grpc — a caller
// holding both must alias one of them.
//
// # Scope
//
// This package speaks the gRPC HTTP/2 transport: request/response header
// mapping, length-prefixed message framing, grpc-status trailers, timeouts,
// and binary metadata. It deliberately carries no protobuf dependency —
// messages cross the API as []byte, and the caller owns marshaling. That
// keeps the package usable as a load-generator transport, where the payload
// is often a pre-marshaled fixture replayed millions of times.
//
// All four call shapes are supported, because conn.Stream is genuinely
// full-duplex: Send may run concurrently with Recv on the same Stream.
//
//   - unary:            Invoke, or Send + CloseSend + one Recv
//   - server-streaming: Send + CloseSend, then Recv until io.EOF
//   - client-streaming: repeated Send, CloseSend, one Recv
//   - bidi-streaming:   Send from one goroutine, Recv from another
//
// # Compression
//
// The client advertises grpc-accept-encoding: identity and never sets the
// compressed flag. A server that returns a compressed message despite that is
// violating the protocol; Recv reports it as ErrCompressed.
//
// # Not covered
//
// Name resolution, load balancing, and retry policy live above this package —
// a ClientConn is one HTTP/2 connection, multiplexing as many concurrent
// streams as the peer's SETTINGS_MAX_CONCURRENT_STREAMS allows, which is the
// same shape gRPC itself uses.
package grpc
