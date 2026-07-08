// Package http3 implements HTTP/3 framing and request/response mapping over
// QUIC (RFC 9114). It is the H-layer counterpart to the HTTP/2 conn package:
// it frames DATA / HEADERS / SETTINGS / ... on QUIC streams and maps HTTP
// semantics onto them, using the qpack package for header compression.
//
// G.7a lands the frame codec (§7.2): the Type+Length framing common to every
// HTTP/3 frame, typed writers for the frames a client sends, and a SETTINGS
// payload codec. The control stream, request/response mapping, and QPACK wiring
// follow in later phases.
//
// The client built on this package is currently reliable only on a lossless
// path: QUIC loss detection and retransmission (RFC 9002) are not yet
// implemented.
package http3
