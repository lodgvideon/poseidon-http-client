// Package conn implements the Phase B HTTP/2 connection layer on top of
// the Phase A frame and HPACK codecs. It owns one *frame.Framer, one
// *hpack.Encoder, and one *hpack.Decoder per *Conn, manages the client
// preface and SETTINGS handshake, and exposes a Stream-per-request API.
//
// Phase B.1 (single in-flight stream) is complete. Phase B.2.1 lifts
// the cap to a configurable AdvertisedSettings.MaxConcurrentStreams
// (default 100). Stream IDs are assigned at first HEADERS write under
// the writer mutex, preserving the monotonic-id ordering required by
// RFC 7540 §5.1.1 even with many concurrent NewStream callers. Flow
// control, dynamic SETTINGS, peer-advertised MAX_CONCURRENT_STREAMS
// enforcement, and GOAWAY-received drain remain B.2.2-B.2.6 work.
//
// *Conn is goroutine-safe across Send/Recv/Close. *Stream methods may
// be called from one goroutine at a time; the package serializes writes
// to the underlying transport internally.
package conn
