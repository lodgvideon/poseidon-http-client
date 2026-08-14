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
// *Conn is goroutine-safe across Send/Recv/Close. A stream's methods may be
// called from one goroutine at a time; the package serializes writes to the
// underlying transport internally.
//
// Streams are handled through StreamRef, not *Stream. The structs are pooled
// and reused across requests, so a raw pointer retained past Close names a
// struct another request may already own — and nothing on it can tell the
// difference, the receiver being the struct itself. A StreamRef names one
// LIFETIME of that struct: NewStream and LookupStream hand one out, and every
// method on it fails with ErrStaleStream once the stream has been recycled,
// instead of reading or writing whatever request holds the struct next.
//
// The check covers the send path all the way down, not just the method call:
// a writer parked on flow-control credit re-validates the lifetime when it
// wakes and again before each frame reaches the wire, because that park can
// outlast the stream it was authorised for.
//
// # Pooled buffers are handed to the caller, and the caller returns them
//
// A StreamEvent can carry ownership of pooled memory, and this package does NOT
// reclaim it. There are two, returned two different ways:
//
//   - EventHeaders / EventInterimHeaders / EventTrailers / EventPushPromise
//     carry Block, which owns the field slice AND the bytes every
//     Headers[i].Name and .Value points into. Return it with ev.Release().
//   - EventData carries DataSlab, the pooled buffer backing Data. Return the
//     POINTER with GetDataBufPool().Put(ev.DataSlab).
//
// Release is nil-safe, so it needs no guard; DataSlab does:
//
//	ev, err := s.Recv(ctx)
//	// ...
//	ev.Release()
//	if ev.DataSlab != nil {
//	    conn.GetDataBufPool().Put(ev.DataSlab)
//	}
//
// Put the POINTER the event carries, not the slice it names: a []byte handed to
// an interface parameter escapes to the heap, which costs the allocation the
// pool exists to avoid. Release exists partly so the header half of this rule
// cannot be got wrong — it used to be a pool accessor and a *[]byte, and of the
// three callers that reimplemented it, the one that got it wrong was this
// package's own benchmark suite.
//
// Return them only once the bytes have been consumed — copied out, or read —
// because the next Get hands the same memory to another frame. Everything
// reachable from a block belongs to whoever draws it next.
//
// Callers of the client package need none of this: Response.Reset and
// StreamResponse.Close do it. It matters for code written straight against
// *Conn, which is the audience for NewStream and SendBatch. Forgetting is not a
// crash and not a leak; it is a fresh allocation per header block and a fresh
// 16 KiB one per DATA frame, quietly, forever. The DATA half was measured at
// 17153 B/op against an expected 2 allocs/op.
//
// For a higher-level request/response API, see the client package
// (Phase C.1), which builds Do and DoStream on top of *Conn.
package conn
