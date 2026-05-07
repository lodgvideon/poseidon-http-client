// Package client provides a high-level HTTP/2 client API on top of
// the conn package. Phase C.1 ships a single-connection-per-Client
// transport with conn-level auto-redial; Phase C.2 will add a
// connection pool against the same internal transport interface.
//
// Two entry points:
//
//   - Client.Do is synchronous: it issues a request and returns a
//     fully-buffered Response. Response body and trailers are
//     opt-in via Request.WantBody and Request.WantTrailers; when
//     either is false the corresponding frames are still consumed
//     (so flow control refunds run) but the bytes are dropped.
//
//   - Client.DoStream returns a StreamResponse once the initial
//     HEADERS frame has arrived. The caller pumps StreamResponse.Recv
//     for DATA, trailers, and reset events. The caller MUST call
//     StreamResponse.Close if it does not drain to EndStream.
//
// All API contracts are described in
// docs/superpowers/specs/2026-05-07-poseidon-client-c1-design.md.
package client
