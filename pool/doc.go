// Package pool is the protocol-neutral vocabulary of connection management:
// how a client discovers backends, picks one, sizes a per-backend pool, and
// observes the connections it opens and closes.
//
// Nothing here is HTTP-specific. A pool trades in connection liveness and the
// peer's concurrent-stream limit, which HTTP/2, HTTP/3 and gRPC-over-HTTP/2 all
// have and none of them define differently. The types lived in package client
// until gRPC needed the same pooling; putting them here lets grpc reuse the
// machinery without linking the HTTP client, and keeps the dependency order
// grpc/doc.go states.
//
// Every name here is aliased back into package client, so client.Address and
// pool.Address are the same type and no caller had to change.
//
// # What is NOT here
//
// Hooks and Metrics stay with the packages that own a request. A pool touches
// only the connection third of those — dial, connection close, resolver update
// — and consumes it through the Observer and Recorder interfaces rather than
// owning a struct with Method, Path and StatusCode fields in it.
package pool
