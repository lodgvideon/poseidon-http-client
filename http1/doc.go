// Package http1 implements the HTTP/1.1 wire protocol (RFC 7230/7231) as a
// zero-dependency (no net/http) connection abstraction for the poseidon
// client layer.
//
// # Dependency order
//
// http1 → hpack (for HeaderField only); no import of conn, frame, or client.
//
// # Design
//
// Exchange is the unit of one request/response pair. It translates the H2-style
// pseudo-header API (":method", ":path", ":authority", ":scheme") to HTTP/1.1
// wire format, writes the request using net.Buffers (writev syscall when the
// OS supports it), and parses the response back into hpack.HeaderField slices.
//
// At most one Exchange is in-flight per Conn (no pipelining). The caller
// must serialize exchanges — typically by wiring Conn into a transport that
// holds an exclusive lock for the exchange lifetime.
package http1
