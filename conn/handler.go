package conn

import (
	"sync"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// headerSlabPool recycles the byte backing for HPACK-decoded header
// fields. The client layer transfers slab ownership via StreamEvent.Slab
// and returns slabs here via GetHeaderSlabPool().Put in Response.Reset /
// StreamResponse.Close.
var headerSlabPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// GetHeaderSlabPool returns the shared pool for header-slab byte buffers.
// The client package calls this to return slabs after use.
func GetHeaderSlabPool() *sync.Pool { return &headerSlabPool }

// dataBufPool recycles the per-DATA-frame payload copy. OnData copies the
// framer's reused read buffer into a pooled buffer rather than a fresh heap
// allocation; the client transfers ownership via StreamEvent.DataSlab and
// returns it here once the payload is consumed.
var dataBufPool = sync.Pool{
	New: func() any {
		// Sized to the default SETTINGS_MAX_FRAME_SIZE so a typical DATA frame
		// never forces a warm-up regrow before the buffer settles in the pool.
		b := make([]byte, 0, 16384)
		return &b
	},
}

// GetDataBufPool returns the shared pool for DATA-frame payload buffers. The
// client package returns a StreamEvent.DataSlab pointer here after the payload
// has been consumed (copied out or fully read).
func GetDataBufPool() *sync.Pool { return &dataBufPool }

// connOps is the contract handler.go needs from its owner. In
// production it's *Conn; tests can supply a fake. Widening this beyond
// lookupStream removes 8 unsafe *Conn type-assertions in the dispatch
// path (DIP fix from W4 review).
type connOps interface {
	lookupStream(id uint32) *Stream
	onDataReceived(s *Stream, length uint32) error
	markStreamDone(id uint32)
	onWindowUpdate(streamID, increment uint32) error
	applyPeerSettings(s frame.SettingsParams) error
	writeSettingsAck() error
	writePingAck(payload [8]byte) error
	deliverPingAck(payload [8]byte)
	onGoAwayReceived(lastStreamID uint32, code frame.ErrCode)

	// pushSupport returns whether server push is enabled and the
	// stream-event buffer size for new (pushed) streams.
	pushSupport() (enabled bool, eventBuf int)

	// registerPushedStream creates a server-initiated stream with the
	// given even ID and returns it.
	registerPushedStream(id uint32) *Stream

	// rstStream sends RST_STREAM for the given stream ID.
	rstStream(id uint32, code frame.ErrCode) error

	// storeOrigins saves the origin list from an ORIGIN frame.
	storeOrigins(origins []string)

	// storeAltSvc saves ALTSVC entries from an ALTSVC frame.
	storeAltSvc(entries []frame.AltSvcEntry)

	// bumpFramesReceived increments the connection's received-frame counter.
	// Called at the start of each On* handler so the counter reflects the
	// frame as soon as it is dispatched, before Recv can observe it.
	bumpFramesReceived()
}

// streamLookup is retained as the legacy alias for tests that only
// fake the lookup behavior; production wiring uses connOps.
type streamLookup = connOps

// connHandler bridges Phase A's frame.Handler interface into per-stream
// StreamEvent pushes.
type connHandler struct {
	streams streamLookup
	dec     *hpack.Decoder

	// scratch holds a slice of decoded HeaderField values for the current
	// header block. Reused across blocks; the contract is that
	// stream.push hands the slice to the consumer and the consumer must
	// not retain it past the next Recv (see spec §4.1).
	scratch []hpack.HeaderField

	// pendingHeaderBlock buffers HEADERS+CONTINUATION fragments of the
	// in-flight stream until END_HEADERS arrives.
	pendingStreamID  uint32
	pendingBuf       []byte
	pendingEndStream bool
	pendingTrailer   bool // true if buffered HEADERS is a trailers frame

	// maxHeaderBytes caps the total bytes accumulated across a
	// HEADERS+CONTINUATION sequence before END_HEADERS arrives. Without it a
	// peer can stream unbounded CONTINUATION frames (none setting END_HEADERS)
	// and exhaust memory (RFC 7540 §6.10 / §10.5.1, CVE-2024-27316). Raised
	// to the advertised SETTINGS_MAX_HEADER_LIST_SIZE when that is larger.
	maxHeaderBytes int
}

// defaultMaxHeaderBytes is the fallback ceiling on accumulated (compressed)
// header-block bytes when no larger SETTINGS_MAX_HEADER_LIST_SIZE is
// advertised. Generous for legitimate headers; bounds CONTINUATION floods.
const defaultMaxHeaderBytes = 8 << 20 // 8 MiB

func newConnHandler(streams streamLookup, dec *hpack.Decoder) *connHandler {
	return &connHandler{
		streams:        streams,
		dec:            dec,
		scratch:        make([]hpack.HeaderField, 0, 16),
		maxHeaderBytes: defaultMaxHeaderBytes,
	}
}

// maxInt is the platform int ceiling (32- or 64-bit).
const maxInt = int(^uint(0) >> 1)

// raiseMaxHeaderBytes lifts the header-block accumulation cap to honor a larger
// advertised SETTINGS_MAX_HEADER_LIST_SIZE, so we never reject a block we told
// the peer we would accept. The default ceiling still bounds CONTINUATION
// floods otherwise. int64 + clamp avoid a 32-bit wrap of the uint32 setting;
// using the uncompressed limit as a compressed-bytes ceiling is conservative
// (compressed <= uncompressed).
func (h *connHandler) raiseMaxHeaderBytes(advertised uint32) {
	adv := int64(advertised)
	if adv > int64(maxInt) {
		adv = int64(maxInt)
	}
	if int(adv) > h.maxHeaderBytes {
		h.maxHeaderBytes = int(adv)
	}
}

// OnData implements frame.Handler. It debits flow-control windows
// (RFC 7540 §6.9.1), surfaces an EventData event to the stream, and
// emits batched WINDOW_UPDATE refunds via the owning Conn. Returns a
// typed StreamError or ConnError when the peer overruns either window.
func (h *connHandler) OnData(fh frame.FrameHeader, p []byte, _ uint8) error {
	h.streams.bumpFramesReceived()
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil // unknown stream — peer chatter, ignored
	}
	if err := h.streams.onDataReceived(s, fh.Length); err != nil {
		return err
	}
	end := fh.Flags&frame.FlagDataEndStream != 0
	// Pooled copy of the framer's reused read buffer; ownership transfers to the
	// client via StreamEvent.DataSlab, returned to dataBufPool once Data is
	// consumed (eliminates a per-DATA-frame heap allocation).
	bufPtr := dataBufPool.Get().(*[]byte)
	*bufPtr = append((*bufPtr)[:0], p...)
	if end {
		s.markRemoteEnd()
		h.streams.markStreamDone(fh.StreamID)
	}
	if !s.push(StreamEvent{Type: EventData, Data: *bufPtr, DataSlab: bufPtr, EndStream: end}) {
		// Event dropped on channel overflow (push reset the stream); return the
		// pooled buffer rather than leaking it to GC under backpressure.
		dataBufPool.Put(bufPtr)
	}
	return nil
}

// OnHeaders implements frame.Handler.
func (h *connHandler) OnHeaders(fh frame.FrameHeader, hb frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	h.streams.bumpFramesReceived()
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil
	}
	end := fh.Flags&frame.FlagHeadersEndStream != 0
	endHeaders := fh.Flags&frame.FlagHeadersEndHeaders != 0
	// A second HEADERS block on the same stream after the response
	// headers have been delivered is a trailers frame (RFC 7540 §8.1).
	isTrailer := s.headersReceived

	if !endHeaders {
		// Buffer until CONTINUATION completes the block. A single HEADERS frame
		// is already bounded by MAX_FRAME_SIZE; the unbounded vector is the
		// CONTINUATION stream, capped in OnContinuation.
		h.pendingStreamID = fh.StreamID
		h.pendingBuf = append(h.pendingBuf[:0], hb...)
		h.pendingEndStream = end
		h.pendingTrailer = isTrailer
		return nil
	}
	if !isTrailer {
		s.headersReceived = true
	}
	return h.emitHeaderBlock(s, hb, end, isTrailer)
}

// OnContinuation implements frame.Handler.
func (h *connHandler) OnContinuation(fh frame.FrameHeader, hb frame.HeaderBlock) error {
	h.streams.bumpFramesReceived()
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil || h.pendingStreamID != fh.StreamID {
		return nil
	}
	h.pendingBuf = append(h.pendingBuf, hb...)
	if len(h.pendingBuf) > h.maxHeaderBytes {
		// CONTINUATION-flood guard (RFC 7540 §6.10 / §10.5.1, CVE-2024-27316).
		return &ConnError{Code: frame.ErrCodeEnhanceYourCalm, Reason: "header block exceeds accumulation limit (CONTINUATION flood)"}
	}
	if fh.Flags&frame.FlagContinuationEndHeaders == 0 {
		return nil
	}
	end := h.pendingEndStream
	if !h.pendingTrailer {
		s.headersReceived = true
	}
	return h.emitHeaderBlock(s, h.pendingBuf, end, h.pendingTrailer)
}

func (h *connHandler) emitHeaderBlock(s *Stream, hb []byte, endStream, isTrailer bool) error {
	h.scratch = h.scratch[:0]
	err := h.dec.DecodeBlock(hb, func(f hpack.HeaderField) error {
		h.scratch = append(h.scratch, f)
		return nil
	})
	if err != nil {
		return &ConnError{Code: frame.ErrCodeCompressionError, Reason: err.Error()}
	}
	evType := EventHeaders
	if isTrailer {
		evType = EventTrailers
	}
	if endStream {
		s.markRemoteEnd()
		h.streams.markStreamDone(s.id)
	}
	// Build one slab for all header bytes, one slice for all fields.
	// Ownership of the slab transfers to the client via StreamEvent.Slab;
	// the client returns it to headerSlabPool in Response.Reset / sr.Close.
	slabPtr := headerSlabPool.Get().(*[]byte)
	*slabPtr = (*slabPtr)[:0]
	copied := make([]hpack.HeaderField, len(h.scratch))
	for i, f := range h.scratch {
		nameOff := len(*slabPtr)
		*slabPtr = append(*slabPtr, f.Name...)
		valOff := len(*slabPtr)
		*slabPtr = append(*slabPtr, f.Value...)
		endOff := len(*slabPtr)
		copied[i] = hpack.HeaderField{
			Name:      (*slabPtr)[nameOff:valOff:valOff],
			Value:     (*slabPtr)[valOff:endOff:endOff],
			Sensitive: f.Sensitive,
		}
	}
	if !s.push(StreamEvent{
		Type:      evType,
		Headers:   copied,
		Slab:      slabPtr,
		EndStream: endStream,
	}) {
		// push dropped the event (channel overflow); return slab to pool.
		*slabPtr = (*slabPtr)[:0]
		headerSlabPool.Put(slabPtr)
	}
	return nil
}

// --- Stub implementations of the rest of frame.Handler. B.1 honors them
// to the spec but does not surface them as caller-visible events.

func (h *connHandler) OnPriority(_ frame.FrameHeader, _ frame.Priority) error {
	h.streams.bumpFramesReceived()
	return nil // deprecated by RFC 9113; ignored
}

// OnRSTStream implements frame.Handler.
func (h *connHandler) OnRSTStream(fh frame.FrameHeader, code frame.ErrCode) error {
	h.streams.bumpFramesReceived()
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil
	}
	s.markRemoteEnd()
	h.streams.markStreamDone(fh.StreamID)
	s.push(StreamEvent{Type: EventReset, RSTCode: code, EndStream: true})
	return nil
}

// OnSettings implements frame.Handler.
// OnSettings implements frame.Handler. ACK frames are accepted
// silently. Non-ACK SETTINGS are merged into Conn.peerSettings, side
// effects applied (HPACK encoder resize, retroactive
// INITIAL_WINDOW_SIZE delta on open streams), and a SETTINGS ACK is
// written back per RFC 7540 §6.5.3.
func (h *connHandler) OnSettings(fh frame.FrameHeader, s frame.SettingsParams) error {
	h.streams.bumpFramesReceived()
	if fh.Flags&frame.FlagSettingsAck != 0 {
		return nil
	}
	if err := h.streams.applyPeerSettings(s); err != nil {
		return err
	}
	return h.streams.writeSettingsAck()
}

// OnPushPromise implements frame.Handler.
// When EnablePush is false, returns PROTOCOL_ERROR per RFC 7540 §8.2.
// When true, registers the promised (even-ID) stream and delivers an
// EventPushPromise on the parent stream's Recv channel.
func (h *connHandler) OnPushPromise(fh frame.FrameHeader, promisedStreamID uint32, hb frame.HeaderBlock, _ uint8) error {
	h.streams.bumpFramesReceived()
	enabled, _ := h.streams.pushSupport()
	if !enabled {
		return &ConnError{
			Code:   frame.ErrCodeProtocolError,
			Reason: ErrUnexpectedPushPromise.Error(),
		}
	}

	// Look up the parent stream.
	parent := h.streams.lookupStream(fh.StreamID)
	if parent == nil {
		// Parent gone; reset the promised stream to be safe.
		_ = h.streams.rstStream(promisedStreamID, frame.ErrCodeCancel)
		return nil
	}

	// Decode the promised pseudo-headers.
	h.scratch = h.scratch[:0]
	if err := h.dec.DecodeBlock(hb, func(f hpack.HeaderField) error {
		h.scratch = append(h.scratch, f)
		return nil
	}); err != nil {
		return &ConnError{Code: frame.ErrCodeCompressionError, Reason: err.Error()}
	}

	// Register the pushed (server-initiated, even) stream.
	pushed := h.streams.registerPushedStream(promisedStreamID)

	// Build slab-backed header fields (same pattern as emitHeaderBlock).
	slabPtr := headerSlabPool.Get().(*[]byte)
	*slabPtr = (*slabPtr)[:0]
	copied := make([]hpack.HeaderField, len(h.scratch))
	for i, f := range h.scratch {
		off := len(*slabPtr)
		*slabPtr = append(*slabPtr, f.Name...)
		copied[i].Name = (*slabPtr)[off : off+len(f.Name)]
		off = len(*slabPtr)
		*slabPtr = append(*slabPtr, f.Value...)
		copied[i].Value = (*slabPtr)[off : off+len(f.Value)]
	}

	// Deliver push event on the parent stream.
	parent.push(StreamEvent{
		Type:         EventPushPromise,
		Headers:      copied,
		PushStreamID: promisedStreamID,
		Slab:         slabPtr,
	})
	_ = pushed // stream registered; subsequent HEADERS/DATA frames find it
	return nil
}

// OnPing implements frame.Handler. Non-ACK PING frames are echoed
// back with ACK=1 and the same opaque 8-byte payload (RFC 7540 §6.7).
// ACK frames are delivered to any Ping call waiting for that payload.
func (h *connHandler) OnPing(fh frame.FrameHeader, payload [8]byte) error {
	h.streams.bumpFramesReceived()
	if fh.Flags&frame.FlagPingAck != 0 {
		h.streams.deliverPingAck(payload)
		return nil
	}
	return h.streams.writePingAck(payload)
}

// OnGoAway implements frame.Handler.
// OnGoAway implements frame.Handler. Records the peer's GOAWAY state
// on the *Conn so future NewStream calls return ErrGoAway, drains any
// streams whose id exceeds lastStreamID with EventReset(REFUSED_STREAM)
// per RFC 7540 §6.8, and wakes writers blocked on send credit.
func (h *connHandler) OnGoAway(_ frame.FrameHeader, lastStreamID uint32, code frame.ErrCode, _ []byte) error {
	h.streams.bumpFramesReceived()
	h.streams.onGoAwayReceived(lastStreamID, code)
	return nil
}

// OnOrigin implements frame.Handler. It stores the server's advertised
// origin list (RFC 8336 §3) for connection coalescing decisions.
func (h *connHandler) OnOrigin(_ frame.FrameHeader, origins []string) error {
	h.streams.bumpFramesReceived()
	dup := make([]string, len(origins))
	copy(dup, origins)
	h.streams.storeOrigins(dup)
	return nil
}

// OnAltSvc implements frame.Handler. It parses ALTSVC entries (RFC 7838
// §4) and stores them for alternative-service routing decisions.
func (h *connHandler) OnAltSvc(_ frame.FrameHeader, entries []frame.AltSvcEntry) error {
	h.streams.bumpFramesReceived()
	dup := make([]frame.AltSvcEntry, len(entries))
	copy(dup, entries)
	h.streams.storeAltSvc(dup)
	return nil
}

// OnWindowUpdate implements frame.Handler.
// OnWindowUpdate implements frame.Handler. It replenishes the
// connection-level (streamID==0) or per-stream outbound send window
// and wakes any writers blocked in acquireSendCredits. Returns a
// typed error when the increment would overflow the window
// (RFC 7540 §6.9.1: 2^31-1).
func (h *connHandler) OnWindowUpdate(fh frame.FrameHeader, increment uint32) error {
	h.streams.bumpFramesReceived()
	return h.streams.onWindowUpdate(fh.StreamID, increment)
}

// Compile-time check that *connHandler satisfies frame.Handler.
var _ frame.Handler = (*connHandler)(nil)
