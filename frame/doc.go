// Package frame implements the HTTP/2 framing layer (RFC 7540) without
// any networking. It provides a Framer that reads frames via a Handler
// visitor (zero-copy, caller-owned scratch) and writes frames via explicit
// per-type methods. Framer is NOT goroutine-safe.
package frame
