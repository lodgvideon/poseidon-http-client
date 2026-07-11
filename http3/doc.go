// Package http3 implements HTTP/3 framing and request/response mapping over
// QUIC (RFC 9114). It is the H-layer counterpart to the HTTP/2 conn package:
// it frames DATA / HEADERS / SETTINGS / ... on QUIC streams and maps HTTP
// semantics onto them, using the qpack package for header compression.
//
// The package provides the HTTP/3 frame codec (§7.2 Type+Length framing, typed
// frame writers, and the SETTINGS payload codec), the control stream, and the
// request/response mapping, with header compression via the qpack package. It
// runs over the QUIC transport in the quic package, which implements loss
// detection and retransmission (RFC 9002), so the client is reliable on lossy
// paths — see docs/RFC_COVERAGE.md for the conformance and interop coverage.
package http3
