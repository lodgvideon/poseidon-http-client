package quic

import "github.com/lodgvideon/poseidon-http-client/internal/bytesx"

// Frame types (RFC 9000 §19 / §12.4). STREAM occupies 0x08–0x0f; the three low
// bits are flags (see streamFin/streamLen/streamOff).
const (
	FramePadding            uint64 = 0x00
	FramePing               uint64 = 0x01
	FrameACK                uint64 = 0x02
	FrameACKECN             uint64 = 0x03
	FrameResetStream        uint64 = 0x04
	FrameStopSending        uint64 = 0x05
	FrameCrypto             uint64 = 0x06
	FrameNewToken           uint64 = 0x07
	FrameStreamBase         uint64 = 0x08
	FrameStreamMax          uint64 = 0x0f
	FrameMaxData            uint64 = 0x10
	FrameMaxStreamData      uint64 = 0x11
	FrameMaxStreamsBidi     uint64 = 0x12
	FrameMaxStreamsUni      uint64 = 0x13
	FrameDataBlocked        uint64 = 0x14
	FrameStreamDataBlocked  uint64 = 0x15
	FrameStreamsBlockedBidi uint64 = 0x16
	FrameStreamsBlockedUni  uint64 = 0x17
	FrameNewConnectionID    uint64 = 0x18
	FrameRetireConnectionID uint64 = 0x19
	FramePathChallenge      uint64 = 0x1a
	FramePathResponse       uint64 = 0x1b
	FrameConnectionClose    uint64 = 0x1c
	FrameConnectionCloseApp uint64 = 0x1d
	FrameHandshakeDone      uint64 = 0x1e
)

// STREAM frame flag bits within the type byte (RFC 9000 §19.8).
const (
	streamFin = 0x01
	streamLen = 0x02
	streamOff = 0x04
)

// AckRange is one additional (post-first) ACK range in an ACK frame.
type AckRange struct{ Gap, Length uint64 }

// FrameHandler receives each frame parsed from a packet payload. Byte slices
// and array pointers alias the input payload and are valid only for the call;
// copy to retain. Any non-nil error aborts parsing and propagates out of
// ParseFrames. ACK ranges are delivered as OnAck followed by OnAckRange per
// additional range, then OnAckECN for the 0x03 variant.
type FrameHandler interface {
	OnPadding(runLen int) error
	OnPing() error
	OnAck(largestAcked, ackDelay, firstAckRange uint64) error
	OnAckRange(gap, length uint64) error
	OnAckECN(ect0, ect1, ce uint64) error
	OnResetStream(streamID, errCode, finalSize uint64) error
	OnStopSending(streamID, errCode uint64) error
	OnCrypto(offset uint64, data []byte) error
	OnNewToken(token []byte) error
	OnStream(streamID, offset uint64, fin bool, data []byte) error
	OnMaxData(maximum uint64) error
	OnMaxStreamData(streamID, maximum uint64) error
	OnMaxStreams(uni bool, maximum uint64) error
	OnDataBlocked(limit uint64) error
	OnStreamDataBlocked(streamID, limit uint64) error
	OnStreamsBlocked(uni bool, limit uint64) error
	OnNewConnectionID(seq, retirePriorTo uint64, connID []byte, resetToken *[16]byte) error
	OnRetireConnectionID(seq uint64) error
	OnPathChallenge(data *[8]byte) error
	OnPathResponse(data *[8]byte) error
	OnConnectionClose(app bool, errCode, frameType uint64, reason []byte) error
	OnHandshakeDone() error
}

// nopFrameHandler implements FrameHandler with no-op methods returning nil.
// Embed it and override only the frames of interest.
type nopFrameHandler struct{}

func (nopFrameHandler) OnPadding(int) error                                        { return nil }
func (nopFrameHandler) OnPing() error                                              { return nil }
func (nopFrameHandler) OnAck(_, _, _ uint64) error                                 { return nil }
func (nopFrameHandler) OnAckRange(_, _ uint64) error                               { return nil }
func (nopFrameHandler) OnAckECN(_, _, _ uint64) error                              { return nil }
func (nopFrameHandler) OnResetStream(_, _, _ uint64) error                         { return nil }
func (nopFrameHandler) OnStopSending(_, _ uint64) error                            { return nil }
func (nopFrameHandler) OnCrypto(uint64, []byte) error                              { return nil }
func (nopFrameHandler) OnNewToken([]byte) error                                    { return nil }
func (nopFrameHandler) OnStream(_, _ uint64, _ bool, _ []byte) error               { return nil }
func (nopFrameHandler) OnMaxData(uint64) error                                     { return nil }
func (nopFrameHandler) OnMaxStreamData(_, _ uint64) error                          { return nil }
func (nopFrameHandler) OnMaxStreams(bool, uint64) error                            { return nil }
func (nopFrameHandler) OnDataBlocked(uint64) error                                 { return nil }
func (nopFrameHandler) OnStreamDataBlocked(_, _ uint64) error                      { return nil }
func (nopFrameHandler) OnStreamsBlocked(bool, uint64) error                        { return nil }
func (nopFrameHandler) OnNewConnectionID(_, _ uint64, _ []byte, _ *[16]byte) error { return nil }
func (nopFrameHandler) OnRetireConnectionID(uint64) error                          { return nil }
func (nopFrameHandler) OnPathChallenge(*[8]byte) error                             { return nil }
func (nopFrameHandler) OnPathResponse(*[8]byte) error                              { return nil }
func (nopFrameHandler) OnConnectionClose(bool, uint64, uint64, []byte) error       { return nil }
func (nopFrameHandler) OnHandshakeDone() error                                     { return nil }

// spacePermitter is implemented by a FrameHandler that enforces which frame types
// may appear in the packet-number space of the packet being parsed (RFC 9000
// §12.4 Table 3 / §12.5). ParseFrames consults it, when present, before dispatching
// each frame, so a frame carried in a space that does not permit it is rejected
// before it reaches a handler method. Handlers that do not implement it (tests,
// the nop handler) accept every frame type.
type spacePermitter interface {
	permitInSpace(typ uint64) error
}

// ParseFrames parses every frame in a decrypted packet payload, dispatching each
// to h in order. It returns ErrFrameEncoding on any malformed frame (truncated
// field, varint past end, length past end, unknown type) — a connection error
// per RFC 9000 §12.4 — or the handler's error.
func ParseFrames(payload []byte, h FrameHandler) error {
	permit, _ := h.(spacePermitter)
	p := 0
	for p < len(payload) {
		// PADDING (0x00) is a single zero byte; coalesce a run into one call. PADDING
		// is permitted in every packet type, so it needs no space check.
		if payload[p] == 0x00 {
			start := p
			for p < len(payload) && payload[p] == 0x00 {
				p++
			}
			if err := h.OnPadding(p - start); err != nil {
				return err
			}
			continue
		}
		typ, n := bytesx.ReadVarint(payload[p:])
		if n == 0 {
			return ErrFrameEncoding
		}
		p += n
		if permit != nil {
			if err := permit.permitInSpace(typ); err != nil {
				return err
			}
		}
		np, err := parseFrameBody(typ, payload, p, h)
		if err != nil {
			return err
		}
		p = np
	}
	return nil
}

// parseFrameBody parses the fields of the frame whose type is typ, starting at
// payload[p] (just past the type). It returns the offset past the frame.
//
//nolint:gocyclo // a flat dispatch over the RFC 9000 §19 frame set.
func parseFrameBody(typ uint64, payload []byte, p int, h FrameHandler) (int, error) {
	// STREAM frames occupy the contiguous range 0x08–0x0f.
	if typ >= FrameStreamBase && typ <= FrameStreamMax {
		return parseStream(byte(typ), payload, p, h)
	}
	switch typ {
	case FramePing:
		return p, h.OnPing()
	case FrameHandshakeDone:
		return p, h.OnHandshakeDone()
	case FrameACK, FrameACKECN:
		return parseAck(typ == FrameACKECN, payload, p, h)
	case FrameResetStream:
		id, ec, fs, np, ok := readV3(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnResetStream(id, ec, fs)
	case FrameStopSending:
		id, ec, np, ok := readV2(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnStopSending(id, ec)
	case FrameCrypto:
		off, data, np, ok := readLenPrefixed(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnCrypto(off, data)
	case FrameNewToken:
		tok, np, ok := readVarBytes(payload, p)
		if !ok || len(tok) == 0 {
			return p, ErrFrameEncoding
		}
		return np, h.OnNewToken(tok)
	case FrameMaxData:
		v, np, ok := readV(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnMaxData(v)
	case FrameMaxStreamData:
		id, m, np, ok := readV2(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnMaxStreamData(id, m)
	case FrameMaxStreamsBidi, FrameMaxStreamsUni:
		v, np, ok := readV(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnMaxStreams(typ == FrameMaxStreamsUni, v)
	case FrameDataBlocked:
		v, np, ok := readV(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnDataBlocked(v)
	case FrameStreamDataBlocked:
		id, l, np, ok := readV2(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnStreamDataBlocked(id, l)
	case FrameStreamsBlockedBidi, FrameStreamsBlockedUni:
		v, np, ok := readV(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnStreamsBlocked(typ == FrameStreamsBlockedUni, v)
	case FrameNewConnectionID:
		return parseNewConnectionID(payload, p, h)
	case FrameRetireConnectionID:
		v, np, ok := readV(payload, p)
		if !ok {
			return p, ErrFrameEncoding
		}
		return np, h.OnRetireConnectionID(v)
	case FramePathChallenge:
		if len(payload)-p < 8 {
			return p, ErrFrameEncoding
		}
		return p + 8, h.OnPathChallenge((*[8]byte)(payload[p : p+8]))
	case FramePathResponse:
		if len(payload)-p < 8 {
			return p, ErrFrameEncoding
		}
		return p + 8, h.OnPathResponse((*[8]byte)(payload[p : p+8]))
	case FrameConnectionClose, FrameConnectionCloseApp:
		return parseConnectionClose(typ == FrameConnectionCloseApp, payload, p, h)
	default:
		return p, ErrFrameEncoding // unknown frame type
	}
}

func parseStream(typeByte byte, payload []byte, p int, h FrameHandler) (int, error) {
	id, p1, ok := readV(payload, p)
	if !ok {
		return p, ErrFrameEncoding
	}
	var offset uint64
	if typeByte&streamOff != 0 {
		if offset, p1, ok = readV(payload, p1); !ok {
			return p, ErrFrameEncoding
		}
	}
	var data []byte
	if typeByte&streamLen != 0 {
		length, p2, ok2 := readV(payload, p1)
		if !ok2 || length > uint64(len(payload)-p2) {
			return p, ErrFrameEncoding
		}
		data = payload[p2 : p2+int(length)]
		p1 = p2 + int(length)
	} else {
		data = payload[p1:] // extends to the end of the packet
		p1 = len(payload)
	}
	return p1, h.OnStream(id, offset, typeByte&streamFin != 0, data)
}

func parseAck(ecn bool, payload []byte, p int, h FrameHandler) (int, error) {
	largest, p1, ok := readV(payload, p)
	if !ok {
		return p, ErrFrameEncoding
	}
	delay, p1, ok := readV(payload, p1)
	if !ok {
		return p, ErrFrameEncoding
	}
	rangeCount, p1, ok := readV(payload, p1)
	if !ok {
		return p, ErrFrameEncoding
	}
	firstRange, p1, ok := readV(payload, p1)
	if !ok {
		return p, ErrFrameEncoding
	}
	if err := h.OnAck(largest, delay, firstRange); err != nil {
		return p, err
	}
	for i := uint64(0); i < rangeCount; i++ {
		gap, p2, ok2 := readV(payload, p1)
		if !ok2 {
			return p, ErrFrameEncoding
		}
		length, p3, ok3 := readV(payload, p2)
		if !ok3 {
			return p, ErrFrameEncoding
		}
		p1 = p3
		if err := h.OnAckRange(gap, length); err != nil {
			return p, err
		}
	}
	if ecn {
		var ect0, ect1, ce uint64
		if ect0, p1, ok = readV(payload, p1); !ok {
			return p, ErrFrameEncoding
		}
		if ect1, p1, ok = readV(payload, p1); !ok {
			return p, ErrFrameEncoding
		}
		if ce, p1, ok = readV(payload, p1); !ok {
			return p, ErrFrameEncoding
		}
		if err := h.OnAckECN(ect0, ect1, ce); err != nil {
			return p, err
		}
	}
	return p1, nil
}

func parseNewConnectionID(payload []byte, p int, h FrameHandler) (int, error) {
	seq, p1, ok := readV(payload, p)
	if !ok {
		return p, ErrFrameEncoding
	}
	retire, p1, ok := readV(payload, p1)
	if !ok || retire > seq {
		return p, ErrFrameEncoding
	}
	if p1 >= len(payload) {
		return p, ErrFrameEncoding
	}
	cidLen := int(payload[p1])
	p1++
	if cidLen < 1 || cidLen > 20 || len(payload)-p1 < cidLen+16 {
		return p, ErrFrameEncoding
	}
	cid := payload[p1 : p1+cidLen]
	p1 += cidLen
	token := (*[16]byte)(payload[p1 : p1+16])
	p1 += 16
	return p1, h.OnNewConnectionID(seq, retire, cid, token)
}

func parseConnectionClose(app bool, payload []byte, p int, h FrameHandler) (int, error) {
	errCode, p1, ok := readV(payload, p)
	if !ok {
		return p, ErrFrameEncoding
	}
	var frameType uint64
	if !app {
		if frameType, p1, ok = readV(payload, p1); !ok {
			return p, ErrFrameEncoding
		}
	}
	reason, p2, ok := readVarBytes(payload, p1)
	if !ok {
		return p, ErrFrameEncoding
	}
	return p2, h.OnConnectionClose(app, errCode, frameType, reason)
}

// --- varint read helpers (all report ok=false on a short/malformed field) ---

func readV(b []byte, p int) (v uint64, np int, ok bool) {
	x, n := bytesx.ReadVarint(b[p:])
	if n == 0 {
		return 0, p, false
	}
	return x, p + n, true
}

func readV2(b []byte, p int) (a, c uint64, np int, ok bool) {
	if a, p, ok = readV(b, p); !ok {
		return
	}
	c, p, ok = readV(b, p)
	return a, c, p, ok
}

func readV3(b []byte, p int) (a, c, d uint64, np int, ok bool) {
	if a, p, ok = readV(b, p); !ok {
		return
	}
	if c, p, ok = readV(b, p); !ok {
		return
	}
	d, p, ok = readV(b, p)
	return a, c, d, p, ok
}

// readLenPrefixed reads an Offset varint, a Length varint, then Length bytes
// (the CRYPTO frame body shape).
func readLenPrefixed(b []byte, p int) (offset uint64, data []byte, np int, ok bool) {
	if offset, p, ok = readV(b, p); !ok {
		return
	}
	data, p, ok = readVarBytes(b, p)
	return offset, data, p, ok
}

// readVarBytes reads a Length varint then Length bytes, aliasing b.
func readVarBytes(b []byte, p int) (data []byte, np int, ok bool) {
	length, p, ok := readV(b, p)
	if !ok || length > uint64(len(b)-p) {
		return nil, p, false
	}
	return b[p : p+int(length)], p + int(length), true
}
