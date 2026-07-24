package conn

import (
	"errors"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// headerDecodeConnError maps an HPACK decode failure to a typed connection
// error. A decoded header list exceeding SETTINGS_MAX_HEADER_LIST_SIZE is
// ENHANCE_YOUR_CALM (RFC 7540 §10.5.1) — the block was well-formed, the peer
// simply exceeded a limit we advertised. Every other decode failure is a
// genuine COMPRESSION_ERROR (RFC 7541 malformed input).
func headerDecodeConnError(err error) *ConnError {
	if errors.Is(err, hpack.ErrHeaderListTooLarge) {
		return &ConnError{Code: frame.ErrCodeEnhanceYourCalm, Reason: err.Error()}
	}
	return &ConnError{Code: frame.ErrCodeCompressionError, Reason: err.Error()}
}

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
	// isIdleStream reports whether id names a never-opened stream (RFC 9113
	// §5.1). A frame other than HEADERS/PRIORITY on such a stream is a
	// connection error PROTOCOL_ERROR; a late frame on a closed stream is not.
	isIdleStream(id uint32) bool
	// isKnownOrigin reports whether authority was advertised via an ORIGIN
	// frame (RFC 8336) — an authority the server is authoritative for on the
	// push accept path.
	isKnownOrigin(authority []byte) bool
	onDataReceived(s *Stream, length uint32) error
	// accountConnRecvOnly settles the connection-level recv window for a DATA
	// frame whose stream is no longer in the registry (RFC 7540 §6.9, §5.1).
	accountConnRecvOnly(length uint32) error
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

	// reservePushedStream validates a promised stream id and registers
	// the server-initiated stream it reserves. Returns
	// ErrIllegalPromisedID for an id that is not idle (RFC 7540 §6.6)
	// and ErrPushRefused when our advertised concurrent-stream cap for
	// server-initiated streams is already reached.
	reservePushedStream(id uint32) (*Stream, error)

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
	// in-flight stream until END_HEADERS arrives. The block is classified
	// (final / informational / trailers) only once it is complete and
	// decoded — the classification keys on :status, which is not knowable
	// until then (RFC 7540 §8.1).
	pendingStreamID  uint32
	pendingBuf       []byte
	pendingEndStream bool
	// pendingIsPush distinguishes a buffered PUSH_PROMISE header block (spanning
	// CONTINUATION) from a HEADERS block: both accumulate in pendingBuf keyed to
	// the frame's stream, but a completed push block routes to the push handler,
	// not emitHeaderBlock. pendingPromisedID carries the promised stream id.
	pendingIsPush     bool
	pendingPromisedID uint32

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
		// A DATA frame on an idle stream (never opened) is a connection error of
		// type PROTOCOL_ERROR (RFC 9113 §5.1). On a closed stream — one we reset,
		// or that completed and was evicted — the payload is discarded (§5.1
		// leniency for a stream we reset), but its bytes still count against the
		// CONNECTION flow-control window (§6.9), so the window must be settled or it
		// leaks and the connection eventually stalls.
		if h.streams.isIdleStream(fh.StreamID) {
			return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "DATA on idle stream"}
		}
		return h.streams.accountConnRecvOnly(fh.Length)
	}
	// A DATA frame after the peer's END_STREAM — half-closed(remote), still
	// registered because our upload is open — is a stream error STREAM_CLOSED
	// (RFC 9113 §5.1 / §6.1). Settle the connection window (the octets are spent)
	// and reset just this stream; the connection and its siblings survive.
	if s.hasRemoteEnded() {
		_ = h.streams.accountConnRecvOnly(fh.Length)
		return &StreamError{StreamID: s.id, Code: frame.ErrCodeStreamClosed}
	}
	if err := h.streams.onDataReceived(s, fh.Length); err != nil {
		return err
	}
	// A DATA frame before the response HEADERS is a malformed message and is
	// rejected AFTER the flow-control accounting above (which §6.9.1 requires even
	// for a frame in error). RFC 7540 §8.1 defines a response as HEADERS then
	// optional DATA; a body with no preceding response is not one. Without this,
	// a single DATA(END_STREAM) with no HEADERS made client.Do return
	// (Response{Status:0, Body:<server bytes>}, nil) — a nil error with an
	// attacker-controlled body and an impossible status. §8.1.2.6 routes a
	// malformed message to a stream error of type PROTOCOL_ERROR; the HTTP/3
	// sibling (dispatchFrame's rb.resp==nil check) has always rejected this.
	if !s.headersReceived {
		return &StreamError{StreamID: s.id, Code: frame.ErrCodeProtocolError}
	}
	// Count every DATA payload byte for the END_STREAM Content-Length check.
	s.bodyLen += int64(len(p))
	end := fh.Flags&frame.FlagDataEndStream != 0
	// Pooled copy of the framer's reused read buffer; ownership transfers to the
	// client via StreamEvent.DataSlab, returned to dataBufPool once Data is
	// consumed (eliminates a per-DATA-frame heap allocation).
	bufPtr := dataBufPool.Get().(*[]byte)
	*bufPtr = append((*bufPtr)[:0], p...)
	if end {
		// Validate before ending the stream: on a Content-Length mismatch the
		// caller must see a reset, not this frame delivered as a clean
		// completion. Returning the StreamError here routes to the reader loop's
		// non-fatal path, which pushes EventReset and RSTs just this stream.
		if cerr := h.checkContentLength(s); cerr != nil {
			dataBufPool.Put(bufPtr)
			return cerr
		}
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
	end := fh.Flags&frame.FlagHeadersEndStream != 0
	endHeaders := fh.Flags&frame.FlagHeadersEndHeaders != 0

	if !endHeaders {
		// Buffer until CONTINUATION completes the block — even when the stream is
		// gone. A single HEADERS frame is already bounded by MAX_FRAME_SIZE; the
		// unbounded vector is the CONTINUATION stream, capped in OnContinuation.
		h.pendingStreamID = fh.StreamID
		h.pendingBuf = append(h.pendingBuf[:0], hb...)
		h.pendingEndStream = end
		h.pendingIsPush = false // a HEADERS block, not a promise
		return nil
	}
	if s == nil {
		return h.headerBlockForAbsentStream(fh.StreamID, hb)
	}
	return h.emitHeaderBlock(s, hb, end)
}

// headerBlockForAbsentStream handles a completed HEADERS/CONTINUATION block whose
// stream is not in the registry. A block on an idle stream — a server opening a
// stream the client did not (odd), or that no PUSH_PROMISE reserved (even, RFC
// 9113 §5.1.1) — is a connection error PROTOCOL_ERROR (§5.1). A block on a closed
// stream (reset or completed and evicted) is decoded to keep the connection-wide
// HPACK context in sync and then discarded (§5.1 leniency); the idle case tears
// the connection down, so its block need not be decoded.
func (h *connHandler) headerBlockForAbsentStream(streamID uint32, hb []byte) error {
	if h.streams.isIdleStream(streamID) {
		return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "HEADERS on idle stream"}
	}
	return h.drainHeaderBlock(hb)
}

// OnContinuation implements frame.Handler.
func (h *connHandler) OnContinuation(fh frame.FrameHeader, hb frame.HeaderBlock) error {
	h.streams.bumpFramesReceived()
	// Match against the in-flight block regardless of stream liveness: a block
	// buffered while its stream still existed must be completed and decoded even
	// if the stream was reset in between, or the decoder desyncs.
	if h.pendingStreamID != fh.StreamID {
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
	// A reassembled PUSH_PROMISE routes back to its own handler, which does its
	// own parent lookup and reservation. A HEADERS block emits on its stream, or —
	// if the stream is gone — is decoded and discarded to keep the decoder synced.
	if h.pendingIsPush {
		h.pendingIsPush = false
		return h.handlePushPromiseBlock(h.pendingStreamID, h.pendingPromisedID, h.pendingBuf)
	}
	if s := h.streams.lookupStream(fh.StreamID); s != nil {
		return h.emitHeaderBlock(s, h.pendingBuf, h.pendingEndStream)
	}
	return h.headerBlockForAbsentStream(fh.StreamID, h.pendingBuf)
}

// drainHeaderBlock decodes hb through the connection-wide HPACK decoder and
// discards the fields. A HEADERS/CONTINUATION block for a stream we have already
// evicted (reset or completed) — or never opened — MUST still be decoded: HPACK
// is a connection-wide stateful context (RFC 7541 §2.2), and a decoder that skips
// a block misses any Literal-with-Incremental-Indexing entry the peer's encoder
// added to the dynamic table, so every later block on the connection resolves an
// index to the wrong field or fails with a COMPRESSION_ERROR that tears the whole
// pooled connection down. This mirrors the decode-before-bail OnPushPromise
// already performs; OnData's evicted-stream path settles flow control the same
// way, and this settles the decoder.
func (h *connHandler) drainHeaderBlock(hb []byte) error {
	if err := h.dec.DecodeBlock(hb, func(hpack.HeaderField) error { return nil }); err != nil {
		return headerDecodeConnError(err)
	}
	return nil
}

// maxInterimResponses bounds the informational (1xx) header blocks a peer may
// send on one stream before the final response. Each is decoded and handed to
// the caller, so an unbounded run is peer-driven work with no natural end.
// 100 matches http1.maxInterimResponses and http3.maxInterimResponses — one
// number for one concept across all three protocols.
const maxInterimResponses = 100

// isInformationalStatus reports whether a decoded header block carries a 1xx
// status. RFC 7540 §8.1 keys trailer classification on the arrival of a
// *final* (non-informational) status code, so a 1xx block must not latch
// Stream.headersReceived. :status is a pseudo-header and so precedes the
// regular fields (§8.1.2.1); the scan tolerates it being absent (a malformed
// block, which the client layer rejects on its own).
func isInformationalStatus(fields []hpack.HeaderField) bool {
	for i := range fields {
		if string(fields[i].Name) == ":status" {
			return len(fields[i].Value) == 3 && fields[i].Value[0] == '1'
		}
	}
	return false
}

// classifyHeaderBlock decides which event a just-decoded header block becomes
// and enforces RFC 7540 §8.1's ordering rules on the response side:
//
//   - Before a final status, a 1xx block is informational: it does not end the
//     stream and does not latch headersReceived, so the *next* block is still a
//     response and not a trailer section. §8.1 defines trailers as HEADERS
//     "after receiving a final (non-informational) status code".
//   - After a final status, a block is a trailer section, which §8.1 requires
//     to terminate the stream: "An endpoint that receives a HEADERS frame
//     without the END_STREAM flag set after receiving a final (non-
//     informational) status code MUST treat the corresponding request or
//     response as malformed". §8.1.2.6 routes malformed to a *stream* error of
//     type PROTOCOL_ERROR (§5.4.2), so the connection survives.
//
// A 1xx that sets END_STREAM is likewise malformed: an informational response
// is not a complete response (RFC 7230 §3.3, RFC 9113 §8.1 states this
// explicitly), and admitting it would end the stream with no final status —
// silently yielding a 1xx as the caller's result.
func (h *connHandler) classifyHeaderBlock(s *Stream, endStream bool) (StreamEventType, error) {
	if s.headersReceived {
		if !endStream {
			return 0, &StreamError{StreamID: s.id, Code: frame.ErrCodeProtocolError}
		}
		return EventTrailers, nil
	}
	if isInformationalStatus(h.scratch) {
		if endStream {
			return 0, &StreamError{StreamID: s.id, Code: frame.ErrCodeProtocolError}
		}
		s.interimCount++
		if s.interimCount > maxInterimResponses {
			return 0, &StreamError{StreamID: s.id, Code: frame.ErrCodeEnhanceYourCalm}
		}
		return EventInterimHeaders, nil
	}
	s.headersReceived = true
	// Capture the final response's status and Content-Length declaration for the
	// END_STREAM check. Only the final block declares the body's length; a 1xx or
	// a trailer block does not, and both are handled above.
	s.respStatus = numericStatus(h.scratch)
	s.clDeclared, s.clPresent, s.clValid = responseContentLength(h.scratch)
	return EventHeaders, nil
}

// statusOf returns the numeric :status of a decoded response block, or 0 when it
// is absent or malformed. classifyHeaderBlock rejects a block with no valid
// status downstream; 0 here simply means "not a content-bearing status", which
// is the safe default for the Content-Length check.
func numericStatus(fields []hpack.HeaderField) int {
	for i := range fields {
		if string(fields[i].Name) != ":status" {
			continue
		}
		n, ok := parseDecimalDigits(fields[i].Value)
		if !ok || n > 599 {
			return 0
		}
		return int(n)
	}
	return 0
}

// checkContentLength enforces RFC 7540 §8.1.2.6's Content-Length rule once the
// whole body has been observed: a content-bearing response that declared a
// Content-Length must have it equal the DATA received, and a declared value that
// was not 1*DIGIT (or self-contradictory across repeats) is malformed on its own.
// Returns a stream-scoped PROTOCOL_ERROR, per "Malformed requests or responses
// that are detected MUST be treated as a stream error ... of type
// PROTOCOL_ERROR" — one bad response costs one stream, not the connection.
func (h *connHandler) checkContentLength(s *Stream) error {
	if !s.clPresent {
		return nil
	}
	s.mu.Lock()
	isHead := s.reqIsHead
	s.mu.Unlock()
	if !statusCanHaveContent(s.respStatus, isHead) {
		return nil
	}
	if !s.clValid || s.clDeclared != s.bodyLen {
		return &StreamError{StreamID: s.id, Code: frame.ErrCodeProtocolError}
	}
	return nil
}

func (h *connHandler) emitHeaderBlock(s *Stream, hb []byte, endStream bool) error {
	h.scratch = h.scratch[:0]
	err := h.dec.DecodeBlock(hb, func(f hpack.HeaderField) error {
		h.scratch = append(h.scratch, f)
		return nil
	})
	if err != nil {
		return headerDecodeConnError(err)
	}
	// Every decoded field is checked here, and here only: this is the one place
	// an HPACK block becomes fields the caller sees, so a rule applied here
	// cannot be walked around by a later path. See conn/validate.go for what
	// RFC 7540 §10.3 and §8.1.2.2 require and why they bind a client and not
	// only an intermediary.
	if verr := h.validateResponseFields(s); verr != nil {
		return verr
	}
	evType, cerr := h.classifyHeaderBlock(s, endStream)
	if cerr != nil {
		return cerr
	}
	if endStream {
		// An empty-body final response: bodyLen is 0, so a declared non-zero
		// Content-Length on a content-bearing status is a mismatch caught here.
		if cerr := h.checkContentLength(s); cerr != nil {
			return cerr
		}
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

// OnRSTStream implements frame.Handler. A peer RST_STREAM moves the stream to
// "closed" in both directions (RFC 7540 §5.1): the client can no longer send on
// it, so its local half is done too. markStreamDone releases the inflight slot
// and evicts the stream only once both ends have ended, so localEnded is forced
// here. Without it a stream reset mid-upload held its MAX_CONCURRENT_STREAMS slot
// until the caller happened to call Stream.Close(), leaking slots and registry
// entries on a long-lived conn.
//
// Ordering is load-bearing against a concurrent Stream.Close():
//
//   - The EventReset is pushed FIRST, while the stream is not yet bothEnded, so
//     it can never race Close() -> recycleStream (which rewrites s.events with no
//     lock held) or land on a recycled/reused struct.
//   - remoteEnded, localEnded and closed are set together in ONE s.mu section.
//     Close() returns early when it observes closed and only recycles when
//     !closed && bothEnded (== localEnded && remoteEnded). remoteEnded is set
//     here in the same hold as closed, so Close() can never observe
//     bothEnded && !closed: it either sees closed (no-op) or sees remoteEnded
//     still false (sends its own CANCEL, never recycles). This holds regardless
//     of localEnded's prior value — a request whose upload already completed has
//     localEnded==true before the RST, so setting remoteEnded in a section
//     separate from closed would leave a window where a concurrent Close() sees
//     bothEnded && !closed and recycles the stream out from under this handler.
func (h *connHandler) OnRSTStream(fh frame.FrameHeader, code frame.ErrCode) error {
	h.streams.bumpFramesReceived()
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		// RFC 9113 §6.4: a RST_STREAM naming an idle stream is a connection error
		// PROTOCOL_ERROR. On a closed stream — one we reset, or that completed and
		// was evicted — a late RST_STREAM is ignored (§5.1).
		if h.streams.isIdleStream(fh.StreamID) {
			return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "RST_STREAM on idle stream"}
		}
		return nil
	}
	s.push(StreamEvent{Type: EventReset, RSTCode: code, EndStream: true})
	s.mu.Lock()
	s.remoteEnded = true
	s.localEnded = true
	s.closed = true
	s.mu.Unlock()
	h.streams.markStreamDone(fh.StreamID)
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
// When true, validates the promised id (§6.6), registers the promised
// (even-ID) stream and delivers an EventPushPromise on the parent stream's
// Recv channel. A promise beyond our advertised concurrent server-initiated
// stream cap is rejected with RST_STREAM(REFUSED_STREAM) per §6.6.
func (h *connHandler) OnPushPromise(fh frame.FrameHeader, promisedStreamID uint32, hb frame.HeaderBlock, _ uint8) error {
	h.streams.bumpFramesReceived()
	enabled, _ := h.streams.pushSupport()
	if !enabled {
		return &ConnError{
			Code:   frame.ErrCodeProtocolError,
			Reason: ErrUnexpectedPushPromise.Error(),
		}
	}

	// A PUSH_PROMISE header block may span CONTINUATION frames (RFC 7540 §6.6,
	// §6.10). Buffer the fragment until END_HEADERS rather than decoding it
	// truncated, which was a COMPRESSION_ERROR that tore the whole connection
	// down. The block accumulates in the same pendingBuf the HEADERS path uses,
	// keyed to the parent stream, with pendingIsPush marking it a promise so
	// OnContinuation routes the completed block back here.
	if fh.Flags&frame.FlagPushPromiseEndHeaders == 0 {
		h.pendingStreamID = fh.StreamID
		h.pendingBuf = append(h.pendingBuf[:0], hb...)
		h.pendingIsPush = true
		h.pendingPromisedID = promisedStreamID
		return nil
	}
	return h.handlePushPromiseBlock(fh.StreamID, promisedStreamID, hb)
}

// handlePushPromiseBlock decodes and processes a complete promised header block
// (RFC 7540 §6.6). Split out of OnPushPromise so a CONTINUATION-spanned promise,
// reassembled in OnContinuation, is handled identically to a single-frame one.
func (h *connHandler) handlePushPromiseBlock(parentStreamID, promisedStreamID uint32, hb []byte) error {
	// Decode the promised pseudo-headers BEFORE any decision that keeps the
	// connection alive. HPACK is a connection-wide stateful context (RFC
	// 7541 §2.2): every header block mutates the dynamic table, so skipping
	// one desynchronizes the decoder for every later block on the
	// connection. Both survivable outcomes below — a vanished parent and a
	// refused promise — return nil and keep reading, so neither may skip it.
	h.scratch = h.scratch[:0]
	if err := h.dec.DecodeBlock(hb, func(f hpack.HeaderField) error {
		h.scratch = append(h.scratch, f)
		return nil
	}); err != nil {
		return headerDecodeConnError(err)
	}

	// Validate the promised request fields before anything downstream sees
	// them. A PUSH_PROMISE carries the header set of the request the server is
	// promising to answer, and RFC 7540 §10.3 makes an invalid field name or a
	// value carrying CR/LF/NUL malformed regardless of direction; §8.1.2.6
	// treats a malformed block as a stream error of type PROTOCOL_ERROR. This
	// is a stream-level refusal — the connection and the parent stream survive,
	// like the ErrPushRefused path below — so the promised stream is reset and
	// nothing is reserved or delivered. Done before reservePushedStream so there
	// is no half-reserved stream to unwind on the malformed path.
	if !validatePromisedRequestFields(h.scratch) {
		_ = h.streams.rstStream(promisedStreamID, frame.ErrCodeProtocolError)
		return nil
	}

	// Look up the parent stream.
	parent := h.streams.lookupStream(parentStreamID)
	if parent == nil {
		// RFC 9113 §6.5.2 / §5.1: a PUSH_PROMISE on an idle parent stream (one the
		// client never opened) is a connection error PROTOCOL_ERROR. On a closed
		// parent (opened then evicted) the promise is refused at the stream level
		// (§5.1 leniency for a stream we reset).
		if h.streams.isIdleStream(parentStreamID) {
			return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "PUSH_PROMISE on idle stream"}
		}
		_ = h.streams.rstStream(promisedStreamID, frame.ErrCodeCancel)
		return nil
	}

	// RFC 9113 §8.4: refuse a promised request whose method is not safe and
	// cacheable, that indicates request content, or whose :authority the server
	// is not authoritative for. This is a stream-level refusal — the connection
	// and the parent stream survive — done before reservePushedStream so no
	// stream is half-reserved on the refusal path.
	if !h.validatePushedRequest(parent.requestAuthority()) {
		_ = h.streams.rstStream(promisedStreamID, frame.ErrCodeProtocolError)
		return nil
	}

	// Reserve the pushed (server-initiated, even) stream.
	pushed, err := h.streams.reservePushedStream(promisedStreamID)
	if err != nil {
		if errors.Is(err, ErrPushRefused) {
			// §6.6: "Recipients of PUSH_PROMISE frames can choose to reject
			// promised streams by returning a RST_STREAM referencing the
			// promised stream identifier back to the sender of the
			// PUSH_PROMISE." A stream-level refusal — the connection and the
			// parent stream both survive.
			_ = h.streams.rstStream(promisedStreamID, frame.ErrCodeRefusedStream)
			return nil
		}
		// §6.6: a PUSH_PROMISE promising an id that is not idle is a
		// connection error of type PROTOCOL_ERROR.
		return &ConnError{Code: frame.ErrCodeProtocolError, Reason: err.Error()}
	}

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

// validatePushedRequest checks the decoded promised request (in h.scratch)
// against RFC 9113 §8.4: the :method must be safe and cacheable (GET/HEAD), the
// promise must not indicate request content, and the :authority must be one the
// server is authoritative for — matching the request that triggered the push
// (answered over the cert-validated connection), or an ORIGIN-frame authority.
// A false return means the promised stream must be reset with PROTOCOL_ERROR.
func (h *connHandler) validatePushedRequest(parentAuthority string) bool {
	var method, authority []byte
	for i := range h.scratch {
		switch string(h.scratch[i].Name) {
		case ":method":
			method = h.scratch[i].Value
		case ":authority":
			authority = h.scratch[i].Value
		case "content-length":
			// A promise that indicates request content is refused; only an
			// explicit zero is tolerated (a bodyless GET/HEAD may carry it).
			if n, ok := parseDecimalDigits(h.scratch[i].Value); !ok || n != 0 {
				return false
			}
		}
	}
	if !pushMethodSafeCacheable(method) {
		return false
	}
	if len(authority) == 0 {
		return false // incomplete header set (§8.3.1): a request needs :authority
	}
	return string(authority) == parentAuthority || h.streams.isKnownOrigin(authority)
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

// validateResponseFields treats a malformed response as RFC 7540 §8.1.2.6
// requires: "Malformed requests or responses that are detected MUST be treated
// as a stream error (Section 5.4.2) of type PROTOCOL_ERROR." A stream error and
// not a connection error, so one hostile response costs one stream and the
// connection survives — which matters for a pooled client.
//
// §8.1.2.6 is explicit about the direction that applies to us: "Clients MUST NOT
// accept a malformed response." This path accepted every one of these until now.
func (h *connHandler) validateResponseFields(s *Stream) error {
	streamErr := func() error { return &StreamError{StreamID: s.id, Code: frame.ErrCodeProtocolError} }

	// classifyHeaderBlock sets headersReceived only after a FINAL header block,
	// and it runs after this, so headersReceived==true here means this block is a
	// trailer section. RFC 7540 §8.1.2.1: "Pseudo-header fields MUST NOT appear in
	// trailers." Until now the ':' branch below only checked the value, so every
	// pseudo-header rule this enforces — undefined pseudo, duplicate :status,
	// pseudo-after-regular, pseudo-in-trailers — was accepted, a malformed
	// response delivered to the caller as success. The HTTP/3 sibling
	// (http3/response.go) has always rejected all of these; this is the H2 path
	// catching up.
	isTrailer := s.headersReceived
	sawRegular, sawStatus := false, false
	for i := range h.scratch {
		name, value := h.scratch[i].Name, h.scratch[i].Value
		if len(name) > 0 && name[0] == ':' {
			// §8.1.2.1: "All pseudo-header fields MUST appear in the header block
			// before regular header fields. Any request or response that contains
			// a pseudo-header field that appears in a header block after a regular
			// header field MUST be treated as malformed".
			if sawRegular {
				return streamErr()
			}
			// The only pseudo-header defined for a response is :status, at most
			// once, and none is allowed in a trailer section. §8.1.2.1: "Endpoints
			// MUST treat a request or response that contains undefined or invalid
			// pseudo-header fields as malformed".
			if isTrailer || string(name) != ":status" || sawStatus {
				return streamErr()
			}
			if !validFieldValue(value) {
				return streamErr()
			}
			sawStatus = true
			continue
		}
		if !validFieldName(name) || !validFieldValue(value) || forbiddenResponseField(name) {
			return streamErr()
		}
		sawRegular = true
	}
	// A response header block (final or interim) must carry :status; a trailer
	// section must not. RFC 7540 §8.1.2.4: the :status pseudo-header "MUST be
	// included in all responses; otherwise, the response is malformed".
	if !isTrailer && !sawStatus {
		return streamErr()
	}
	return nil
}
