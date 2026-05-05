// Package conn implements the Phase B HTTP/2 connection layer on top of
// the Phase A frame and HPACK codecs. It owns one *frame.Framer, one
// *hpack.Encoder, and one *hpack.Decoder per *Conn, manages the client
// preface and SETTINGS handshake, and exposes a Stream-per-request API.
//
// Phase B.1 (this milestone) enforces one in-flight stream per Conn.
// B.2 lifts that limit and adds the full RFC 7540 §5.1 stream state
// machine plus real flow control.
//
// *Conn is goroutine-safe across Send/Recv/Close. *Stream methods may
// be called from one goroutine at a time; the package serializes writes
// to the underlying transport internally.
package conn
