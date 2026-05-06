// Package conn implements the Phase B HTTP/2 connection layer on top of
// the Phase A frame and HPACK codecs. It owns one *frame.Framer, one
// *hpack.Encoder, and one *hpack.Decoder per *Conn, manages the client
// preface and SETTINGS handshake, and exposes a Stream-per-request API.
//
// Phase B.1 (single in-flight stream) is complete. Phase B.2.1 lifts
// the cap to a configurable AdvertisedSettings.MaxConcurrentStreams
// (default 100) with first-HEADERS-write ID assignment under the
// writer mutex (RFC 7540 §5.1.1). Phase B.2.2 adds receive-side flow
// control: per-stream and connection recv windows are debited on each
// inbound DATA frame, and WINDOW_UPDATE refunds are batched once an
// accumulated counter crosses recvWindowRefundThreshold (32 KiB).
// Peer overruns surface as typed StreamError or
// ConnError(FLOW_CONTROL_ERROR). Outbound flow control,
// dynamic SETTINGS, peer-advertised MAX_CONCURRENT_STREAMS
// enforcement, and GOAWAY-received drain remain B.2.3-B.2.6 work.
//
// *Conn is goroutine-safe across Send/Recv/Close. *Stream methods may
// be called from one goroutine at a time; the package serializes writes
// to the underlying transport internally.
package conn
