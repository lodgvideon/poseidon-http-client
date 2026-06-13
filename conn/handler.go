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
}

func newConnHandler(streams streamLookup, dec *hpack.Decoder) *connHandler {
	return &connHandler{
		streams: streams,
		dec:     dec,
		scratch: make([]hpack.HeaderField, 0, 16),
	}
}

// OnData implements frame.Handler. It debits flow-control windows
// (RFC 7540 §6.9.1), surfaces an EventData event to the stream, and
// emits batched WINDOW_UPDATE refunds via the owning Conn. Returns a
// typed StreamError or ConnError when the peer overruns either window.
func (h *connHandler) OnData(fh frame.FrameHeader, p []byte, _ uint8) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil // unknown stream — peer chatter, ignored
	}
	if err := h.streams.onDataReceived(s, fh.Length); err != nil {
		return err
	}
	end := fh.Flags&frame.FlagDataEndStream != 0
	dataCopy := append([]byte(nil), p...)
	if end {
		s.markRemoteEnd()
		h.streams.markStreamDone(fh.StreamID)
	}
	s.push(StreamEvent{Type: EventData, Data: dataCopy, EndStream: end})
	return nil
}

// OnHeaders implements frame.Handler.
func (h *connHandler) OnHeaders(fh frame.FrameHeader, hb frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
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
		// Buffer until CONTINUATION completes the block.
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
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil || h.pendingStreamID != fh.StreamID {
		return nil
	}
	h.pendingBuf = append(h.pendingBuf, hb...)
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
	return nil // deprecated by RFC 9113; ignored
}

// OnRSTStream implements frame.Handler.
func (h *connHandler) OnRSTStream(fh frame.FrameHeader, code frame.ErrCode) error {
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
	if fh.Flags&frame.FlagSettingsAck != 0 {
		return nil
	}
	if err := h.streams.applyPeerSettings(s); err != nil {
		return err
	}
	return h.streams.writeSettingsAck()
}

// OnPushPromise implements frame.Handler.
func (h *connHandler) OnPushPromise(_ frame.FrameHeader, _ uint32, _ frame.HeaderBlock, _ uint8) error {
	return &ConnError{
		Code:   frame.ErrCodeProtocolError,
		Reason: ErrUnexpectedPushPromise.Error(),
	}
}

// OnPing implements frame.Handler. Non-ACK PING frames are echoed
// back with ACK=1 and the same opaque 8-byte payload (RFC 7540 §6.7).
// ACK frames are delivered to any Ping call waiting for that payload.
func (h *connHandler) OnPing(fh frame.FrameHeader, payload [8]byte) error {
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
	h.streams.onGoAwayReceived(lastStreamID, code)
	return nil
}

// OnWindowUpdate implements frame.Handler.
// OnWindowUpdate implements frame.Handler. It replenishes the
// connection-level (streamID==0) or per-stream outbound send window
// and wakes any writers blocked in acquireSendCredits. Returns a
// typed error when the increment would overflow the window
// (RFC 7540 §6.9.1: 2^31-1).
func (h *connHandler) OnWindowUpdate(fh frame.FrameHeader, increment uint32) error {
	return h.streams.onWindowUpdate(fh.StreamID, increment)
}

// Compile-time check that *connHandler satisfies frame.Handler.
var _ frame.Handler = (*connHandler)(nil)
