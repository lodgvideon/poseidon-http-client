// Package client provides the high-level HTTP client API over this repo's
// from-scratch HTTP/1.1, HTTP/2 and HTTP/3 stacks — the conn, http1 and http3
// packages respectively. It is the only package most callers need.
//
// Two entry points:
//
//   - Client.Do is synchronous: it issues a request and returns a
//     fully-buffered Response. Response body and trailers are
//     opt-in via Request.BodyMode (BodyBuffer) and Request.WantTrailers;
//     when the body mode is BodyDiscard or WantTrailers is false the
//     corresponding frames are still consumed (so flow control refunds
//     run) but the bytes are dropped.
//
//   - Client.DoStream returns a StreamResponse once the initial
//     HEADERS frame has arrived. The caller pumps StreamResponse.Recv
//     for DATA, trailers, and reset events. The caller MUST call
//     StreamResponse.Close if it does not drain to EndStream.
//
// gRPC is NOT reached through this package. It has its own entry point in
// grpc/, on top of conn/ rather than client/, because Client.DoStream writes
// the whole request body before returning and so cannot express
// client-streaming or bidirectional calls. See docs/GRPC_GUIDE.md.
//
// # The transport grid
//
// Half of this package is one grid: three protocols by three connection
// topologies. TransportKind names the cells:
//
//	topology            HTTP/2                HTTP/1.1                HTTP/3
//	------------------  --------------------  ----------------------  --------------------
//	one conn per Client TransportSingleConn   TransportH1SingleConn   TransportH3
//	per-host pool       TransportPool         TransportH1Pool         TransportH3Pool
//	resolver + selector TransportManaged      TransportH1Managed      TransportH3Managed
//
// TransportALPN sits outside the grid: it offers "h2" and "http/1.1" at dial
// time and pins the negotiated protocol for the Client's life.
//
// The implementation files follow the same grid, one row per concern. The
// HTTP/2 column carries no prefix purely because it shipped first, so read
// pool.go as "the H2 pool" rather than as the pool:
//
//	concern                     HTTP/2               HTTP/1.1                  HTTP/3
//	--------------------------  -------------------  ------------------------  -----------------------
//	single-conn transport       single_conn.go       h1_transport.go           h3_transport.go
//	per-host pool actor         pool.go              h1_pool.go                h3_pool.go
//	pool to transport adapter   pool_transport.go    h1_pool_transport.go      h3_pool_transport.go
//	managed pool                managed_pool.go      h1_managed_pool.go        h3_managed_pool.go
//	managed to transport        managed_transport.go h1_managed_transport.go   h3_managed_transport.go
//
// This layout is written down because the recurring defect in this repo is a
// fix landing in one column and not in its siblings. When changing anything in
// a grid cell, look across its row before assuming the change is local — and
// note that identical-looking bodies can still differ, because some divergence
// lives in the per-protocol helpers they call rather than in the cell itself.
// client/pool_shared.go records which parts are genuinely shared, and the
// reasoning for each part that deliberately is not.
//
// Guides: docs/CLIENT_GUIDE.md (HTTP/1.1 + HTTP/2), docs/HTTP3_DESIGN.md.
package client
