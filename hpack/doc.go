// Package hpack implements HPACK (RFC 7541) header compression for HTTP/2.
//
// The package is built from scratch (no dependencies on net/http or
// golang.org/x/net/http2) for tight zero-allocation control. Encoder and
// Decoder hold pooled state and are NOT safe for concurrent use; callers
// instantiate one of each per HTTP/2 connection.
//
// Decoded HeaderField values reference internal buffers that are only valid
// for the duration of the FieldVisitor invocation — copy if you must
// retain.
package hpack
