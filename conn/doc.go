// Package conn implements the HTTP/2 connection layer for poseidon-http-client.
//
// One *Conn wraps one net.Conn (TLS or cleartext). The caller drives the
// receive loop via Pump(ctx) in a dedicated goroutine. NewStream and all
// Stream methods are concurrency-safe. The single Pump invariant is the
// caller's responsibility — do not call Pump from multiple goroutines.
//
// Layered atop the frame and hpack packages; uses crypto/tls for transport
// security with ALPN h2 negotiation. Cleartext HTTP/2 (h2c) is supported
// via H2CDialer for load-test scenarios where TLS overhead must be excluded.
package conn
