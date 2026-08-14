module github.com/lodgvideon/poseidon-http-client

go 1.25.0

// Build floor, deliberately above the `go` directive above: go1.25.13 is the
// first 1.25 patch clearing GO-2026-6090 (crypto/tls KeyUpdate DoS, reachable
// from quic.TLSHandshake and every TLS dial here) and GO-2026-5972
// (encoding/asn1 stack exhaustion). The `go` line stays at 1.25.0 because it is
// this library's compatibility promise to importers; `toolchain` applies only
// when this module is the main one, so it patches our own builds and CI without
// forcing the bump on consumers.
toolchain go1.25.13

require (
	github.com/andybalholm/brotli v1.2.2
	github.com/klauspost/compress v1.19.2
	golang.org/x/crypto v0.54.0
	golang.org/x/net v0.57.0
)

require (
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)
