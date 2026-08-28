// Package poolcore is the connection-pool machinery: the per-address pool
// actor, the resolver-driven fan-out across addresses, and the dial helpers
// both are built from.
//
// It is internal because nothing outside this module should build a pool
// directly — the public entry points are client and grpc. Being internal is
// also why the names here are exported freely: an exported identifier in an
// internal package is module-private, so the seam between client and this
// package costs no public API.
//
// The vocabulary — addresses, resolvers, selectors, options, stats and the
// connection-lifecycle events — is in package pool, which this one builds on
// and which callers can name. Observability arrives through pool.Observer and
// pool.Recorder rather than through a hooks struct, because a pool raises only
// the connection third of a caller's callbacks and knows nothing about
// requests.
//
// See docs/adr/0001-connection-pooling-lives-outside-client.md.
package poolcore
