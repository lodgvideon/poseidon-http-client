// Package conn implements the Phase B HTTP/2 connection layer on top of
// the Phase A frame and HPACK codecs. It owns one *frame.Framer, one
// *hpack.Encoder, and one *hpack.Decoder per *Conn, manages the client
// preface and SETTINGS handshake, and exposes a Stream-per-request API.
//
// Phase B.1 (single in-flight stream) is complete. Phase B.2.1 lifts
// the cap to a configurable AdvertisedSettings.MaxConcurrentStreams
// (default 100) with first-HEADERS-write ID assignment under the
// writer mutex (RFC 7540 §5.1.1). Phase B.2.2 adds receive-side flow
// control: per-stream and connection recv windows debited on each
// inbound DATA frame, batched WINDOW_UPDATE refunds at 32 KiB,
// typed FLOW_CONTROL_ERROR on peer overrun. Phase B.2.3 adds
// outbound flow control: writeData chunks at min(peer
// MAX_FRAME_SIZE, our advertised MAX_FRAME_SIZE) and blocks in
// acquireSendCredits until both per-stream and connection-level
// peer-advertised send windows have credit; OnWindowUpdate
// replenishes those windows and Broadcasts the writer cond, with
// 2^31-1 overflow returning a typed StreamError or ConnError.
// Phase B.2.4 wires dynamic SETTINGS: connHandler.OnSettings merges
// non-ACK frames into c.peerSettings, applies side effects
// (HPACK encoder resize, retroactive INITIAL_WINDOW_SIZE delta on
// every open stream — RFC §6.9.2), and emits a SETTINGS ACK.
// Phase B.2.5 honors peer-advertised SETTINGS_MAX_CONCURRENT_STREAMS:
// NewStream gates inflight on min(local advertised, peer-advertised);
// dynamic shrinks via applyPeerSettings refuse new streams without
// disturbing open ones (RFC §6.5.2). Phase B.2.6 finishes the lifecycle:
// connHandler.OnGoAway records the peer's GOAWAY state on *Conn so
// future NewStream calls return ErrGoAway, drains streams whose id
// exceeds lastStreamID with EventReset(REFUSED_STREAM), and wakes
// writers blocked on send credit (RFC §6.8); connHandler.OnPing echoes
// non-ACK PING frames back with ACK=1 and the original 8-byte
// payload, dropping ACK frames silently (RFC §6.7).
//
// *Conn is goroutine-safe across Send/Recv/Close. *Stream methods may
// be called from one goroutine at a time; the package serializes writes
// to the underlying transport internally.
//
// For a higher-level request/response API, see the client package
// (Phase C.1), which builds Do and DoStream on top of *Conn.
package conn
