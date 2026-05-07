package conn

import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// streamLookup is the narrow contract handler.go needs from its owner.
// In production it's *Conn; in tests it's fakeStreamMap.
type streamLookup interface {
	lookupStream(id uint32) *Stream
}

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

// OnData implements frame.Handler.
// OnData implements frame.Handler. It debits flow-control windows
// (RFC 7540 §6.9.1), surfaces an EventData event to the stream, and
// emits batched WINDOW_UPDATE refunds via the owning Conn. Returns a
// typed StreamError or ConnError when the peer overruns either window.
func (h *connHandler) OnData(fh frame.FrameHeader, p []byte, _ uint8) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil // unknown stream — peer chatter, ignored
	}
	if c, ok := h.streams.(*Conn); ok {
		if err := c.onDataReceived(s, fh.Length); err != nil {
			return err
		}
	}
	end := fh.Flags&frame.FlagDataEndStream != 0
	dataCopy := append([]byte(nil), p...)
	if end {
		s.markRemoteEnd()
		if c, ok := h.streams.(*Conn); ok {
			c.markStreamDone(fh.StreamID)
		}
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

	if !endHeaders {
		// Buffer until CONTINUATION completes the block.
		h.pendingStreamID = fh.StreamID
		h.pendingBuf = append(h.pendingBuf[:0], hb...)
		h.pendingEndStream = end
		h.pendingTrailer = false
		return nil
	}
	return h.emitHeaderBlock(s, hb, end, false)
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
		if c, ok := h.streams.(*Conn); ok {
			c.markStreamDone(s.id)
		}
	}
	// Copy header fields into per-event memory so the channel
	// buffer cannot expose a slice that the next emitHeaderBlock
	// call has overwritten in scratch.
	copied := make([]hpack.HeaderField, len(h.scratch))
	for i := range h.scratch {
		nm := append([]byte(nil), h.scratch[i].Name...)
		vl := append([]byte(nil), h.scratch[i].Value...)
		copied[i] = hpack.HeaderField{Name: nm, Value: vl, Sensitive: h.scratch[i].Sensitive}
	}
	s.push(StreamEvent{
		Type:      evType,
		Headers:   copied,
		EndStream: endStream,
	})
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
	if c, ok := h.streams.(*Conn); ok {
		c.markStreamDone(fh.StreamID)
	}
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
	c, ok := h.streams.(*Conn)
	if !ok {
		return nil
	}
	if err := c.applyPeerSettings(s); err != nil {
		return err
	}
	return c.writeSettingsAck()
}

// OnPushPromise implements frame.Handler.
func (h *connHandler) OnPushPromise(_ frame.FrameHeader, _ uint32, _ frame.HeaderBlock, _ uint8) error {
	return &ConnError{
		Code:   frame.ErrCodeProtocolError,
		Reason: ErrUnexpectedPushPromise.Error(),
	}
}

// OnPing implements frame.Handler.
// OnPing implements frame.Handler. Non-ACK PING frames are echoed
// back with ACK=1 and the same opaque 8-byte payload (RFC 7540 §6.7).
// ACK frames are silently accepted — B.2.6 does not initiate active
// PINGs, so we never expect ACKs of our own.
func (h *connHandler) OnPing(fh frame.FrameHeader, payload [8]byte) error {
	if fh.Flags&frame.FlagPingAck != 0 {
		return nil
	}
	c, ok := h.streams.(*Conn)
	if !ok {
		return nil
	}
	return c.writePingAck(payload)
}

// OnGoAway implements frame.Handler.
// OnGoAway implements frame.Handler. Records the peer's GOAWAY state
// on the *Conn so future NewStream calls return ErrGoAway, drains any
// streams whose id exceeds lastStreamID with EventReset(REFUSED_STREAM)
// per RFC 7540 §6.8, and wakes writers blocked on send credit.
func (h *connHandler) OnGoAway(_ frame.FrameHeader, lastStreamID uint32, code frame.ErrCode, _ []byte) error {
	c, ok := h.streams.(*Conn)
	if !ok {
		return nil
	}
	c.onGoAwayReceived(lastStreamID, code)
	return nil
}

// OnWindowUpdate implements frame.Handler.
// OnWindowUpdate implements frame.Handler. It replenishes the
// connection-level (streamID==0) or per-stream outbound send window
// and wakes any writers blocked in acquireSendCredits. Returns a
// typed error when the increment would overflow the window
// (RFC 7540 §6.9.1: 2^31-1).
func (h *connHandler) OnWindowUpdate(fh frame.FrameHeader, increment uint32) error {
	c, ok := h.streams.(*Conn)
	if !ok {
		return nil
	}
	return c.onWindowUpdate(fh.StreamID, increment)
}

// Compile-time check that *connHandler satisfies frame.Handler.
var _ frame.Handler = (*connHandler)(nil)
