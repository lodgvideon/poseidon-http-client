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
	s.push(StreamEvent{
		Type:      evType,
		Headers:   h.scratch,
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
func (h *connHandler) OnSettings(_ frame.FrameHeader, _ frame.SettingsParams) error {
	return nil // handled by handshakeSettings (Task 7) and conn.go control loop
}

// OnPushPromise implements frame.Handler.
func (h *connHandler) OnPushPromise(_ frame.FrameHeader, _ uint32, _ frame.HeaderBlock, _ uint8) error {
	return &ConnError{
		Code:   frame.ErrCodeProtocolError,
		Reason: ErrUnexpectedPushPromise.Error(),
	}
}

// OnPing implements frame.Handler.
func (h *connHandler) OnPing(_ frame.FrameHeader, _ [8]byte) error {
	return nil // PING ACK is sent by conn.go (Task 9), not from here
}

// OnGoAway implements frame.Handler.
func (h *connHandler) OnGoAway(_ frame.FrameHeader, _ uint32, _ frame.ErrCode, _ []byte) error {
	return nil // surfaced by conn.go control loop
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
