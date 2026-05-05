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

func (h *connHandler) OnData(fh frame.FrameHeader, p []byte, _ uint8) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil // unknown stream — peer chatter, ignored in B.1
	}
	end := fh.Flags&frame.FlagDataEndStream != 0
	dataCopy := append([]byte(nil), p...) // see B.2 TODO: pool
	if end {
		s.markRemoteEnd()
	}
	s.push(StreamEvent{Type: EventData, Data: dataCopy, EndStream: end})
	if end {
		if c, ok := h.streams.(*Conn); ok {
			c.markStreamDone(fh.StreamID)
		}
	}
	return nil
}

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
	}
	s.push(StreamEvent{
		Type:      evType,
		Headers:   h.scratch,
		EndStream: endStream,
	})
	if endStream {
		if c, ok := h.streams.(*Conn); ok {
			c.markStreamDone(s.id)
		}
	}
	return nil
}

// --- Stub implementations of the rest of frame.Handler. B.1 honors them
// to the spec but does not surface them as caller-visible events.

func (h *connHandler) OnPriority(_ frame.FrameHeader, _ frame.Priority) error {
	return nil // deprecated by RFC 9113; ignored
}

func (h *connHandler) OnRSTStream(fh frame.FrameHeader, code frame.ErrCode) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil
	}
	s.markRemoteEnd()
	s.push(StreamEvent{Type: EventReset, RSTCode: code, EndStream: true})
	if c, ok := h.streams.(*Conn); ok {
		c.markStreamDone(fh.StreamID)
	}
	return nil
}

func (h *connHandler) OnSettings(_ frame.FrameHeader, _ frame.SettingsParams) error {
	return nil // handled by handshakeSettings (Task 7) and conn.go control loop
}

func (h *connHandler) OnPushPromise(_ frame.FrameHeader, _ uint32, _ frame.HeaderBlock, _ uint8) error {
	return &ConnError{
		Code:   frame.ErrCodeProtocolError,
		Reason: ErrUnexpectedPushPromise.Error(),
	}
}

func (h *connHandler) OnPing(_ frame.FrameHeader, _ [8]byte) error {
	return nil // PING ACK is sent by conn.go (Task 9), not from here
}

func (h *connHandler) OnGoAway(_ frame.FrameHeader, _ uint32, _ frame.ErrCode, _ []byte) error {
	return nil // surfaced by conn.go control loop
}

func (h *connHandler) OnWindowUpdate(_ frame.FrameHeader, _ uint32) error {
	return nil // B.1 does not manage flow-control windows; B.2 will
}

// Compile-time check that *connHandler satisfies frame.Handler.
var _ frame.Handler = (*connHandler)(nil)
