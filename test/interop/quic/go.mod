// The quic-interop-runner client endpoint.
//
// A nested module, like test/integration/http3/{faultserver,sink}, and for the
// reason the root module cannot host it: this is a `main` package with no tests,
// and CI enforces a per-package statement-coverage floor of 80 over the root
// module's ./... — examples/ is excluded from that run for exactly this reason,
// and widening the exclusion is worse than staying outside it.
//
// The replace keeps it building against the tree it sits in rather than a
// published tag, so an API change here fails at `make interop-build` and in the
// Docker build instead of drifting.
module github.com/lodgvideon/poseidon-http-client/test/interop/quic

go 1.25.0

toolchain go1.25.13

require github.com/lodgvideon/poseidon-http-client v0.0.0

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/lodgvideon/poseidon-http-client => ../../..
