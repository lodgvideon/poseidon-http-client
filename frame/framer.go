package frame

import (
	"context"
	"errors"
	"io"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

const defaultMaxFrameSize uint32 = 16384

// Handler is a visitor for received frames. Slices passed to On* methods
// are valid only for the duration of the call; copy if you must retain.
type Handler interface {
	OnData(h FrameHeader, payload []byte, padLen uint8) error
	OnHeaders(h FrameHeader, hb HeaderBlock, prio *Priority, padLen uint8) error
	OnPriority(h FrameHeader, p Priority) error
	OnRSTStream(h FrameHeader, code ErrCode) error
	OnSettings(h FrameHeader, s SettingsParams) error
	OnPushPromise(h FrameHeader, promisedID uint32, hb HeaderBlock, padLen uint8) error
	OnPing(h FrameHeader, opaqueData [8]byte) error
	OnGoAway(h FrameHeader, lastStreamID uint32, code ErrCode, debug []byte) error
	OnWindowUpdate(h FrameHeader, increment uint32) error
	OnContinuation(h FrameHeader, hb HeaderBlock) error
}

// WriteHeadersParams bundles the optional fields of a HEADERS frame.
type WriteHeadersParams struct {
	StreamID      uint32
	BlockFragment []byte
	EndStream     bool
	EndHeaders    bool
	Priority      *Priority
	PadLength     uint8
}

// Framer reads and writes HTTP/2 frames over an io.Reader / io.Writer.
// NOT goroutine-safe.
type Framer struct {
	w io.Writer
	r io.Reader

	maxReadFrameSize  uint32
	maxHeaderListSize uint32

	readBuf  []byte
	hdrBuf   [FrameHeaderSize]byte
	smallBuf [16]byte
}

func NewFramer(w io.Writer, r io.Reader) *Framer {
	rb := bytesx.GetReadBuf(int(defaultMaxFrameSize) + FrameHeaderSize)
	return &Framer{
		w:                w,
		r:                r,
		maxReadFrameSize: defaultMaxFrameSize,
		readBuf:          *rb,
	}
}

func (f *Framer) SetMaxReadFrameSize(n uint32)  { f.maxReadFrameSize = n }
func (f *Framer) SetMaxHeaderListSize(n uint32) { f.maxHeaderListSize = n }
func (f *Framer) SetReadBuffer(buf []byte)      { f.readBuf = buf }

// paddingZeros provides a constant zero buffer for padded writes.
var paddingZeros [256]byte

// WriteClientPreface sends the connection preface (RFC 7540 §3.5).
func (f *Framer) WriteClientPreface() error {
	const preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	_, err := io.WriteString(f.w, preface)
	return err
}

func (f *Framer) writeHeader(h FrameHeader) error {
	WriteFrameHeader(f.hdrBuf[:], h)
	_, err := f.w.Write(f.hdrBuf[:])
	return err
}

func (f *Framer) writeFrame(h FrameHeader, payload []byte) error {
	if h.Length > f.maxReadFrameSize {
		return ErrFrameTooLarge
	}
	if err := f.writeHeader(h); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := f.w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// === Write side ===

func (f *Framer) WriteData(streamID uint32, endStream bool, data []byte) error {
	if streamID == 0 {
		return ErrInvalidStreamID
	}
	flags := Flags(0)
	if endStream {
		flags |= FlagDataEndStream
	}
	return f.writeFrame(FrameHeader{Length: uint32(len(data)), Type: FrameData, Flags: flags, StreamID: streamID}, data)
}

func (f *Framer) WriteDataPadded(streamID uint32, endStream bool, data []byte, padLen uint8) error {
	if streamID == 0 {
		return ErrInvalidStreamID
	}
	flags := Flags(FlagDataPadded)
	if endStream {
		flags |= FlagDataEndStream
	}
	totalLen := uint32(1 + len(data) + int(padLen))
	if totalLen > f.maxReadFrameSize {
		return ErrFrameTooLarge
	}
	if err := f.writeHeader(FrameHeader{Length: totalLen, Type: FrameData, Flags: flags, StreamID: streamID}); err != nil {
		return err
	}
	f.smallBuf[0] = padLen
	if _, err := f.w.Write(f.smallBuf[:1]); err != nil {
		return err
	}
	if _, err := f.w.Write(data); err != nil {
		return err
	}
	if padLen > 0 {
		if _, err := f.w.Write(paddingZeros[:padLen]); err != nil {
			return err
		}
	}
	return nil
}

func (f *Framer) WriteHeaders(p WriteHeadersParams) error {
	if p.StreamID == 0 {
		return ErrInvalidStreamID
	}
	flags := Flags(0)
	if p.EndStream {
		flags |= FlagHeadersEndStream
	}
	if p.EndHeaders {
		flags |= FlagHeadersEndHeaders
	}
	if p.PadLength > 0 {
		flags |= FlagHeadersPadded
	}
	if p.Priority != nil {
		flags |= FlagHeadersPriority
	}
	totalLen := uint32(len(p.BlockFragment) + int(p.PadLength))
	if p.PadLength > 0 {
		totalLen++ // pad length byte
	}
	if p.Priority != nil {
		totalLen += 5
	}
	if err := f.writeHeader(FrameHeader{Length: totalLen, Type: FrameHeaders, Flags: flags, StreamID: p.StreamID}); err != nil {
		return err
	}
	if p.PadLength > 0 {
		f.smallBuf[0] = p.PadLength
		if _, err := f.w.Write(f.smallBuf[:1]); err != nil {
			return err
		}
	}
	if p.Priority != nil {
		dep := p.Priority.StreamDep
		if p.Priority.Exclusive {
			dep |= 0x80000000
		}
		f.smallBuf[0] = byte(dep >> 24)
		f.smallBuf[1] = byte(dep >> 16)
		f.smallBuf[2] = byte(dep >> 8)
		f.smallBuf[3] = byte(dep)
		f.smallBuf[4] = p.Priority.Weight
		if _, err := f.w.Write(f.smallBuf[:5]); err != nil {
			return err
		}
	}
	if _, err := f.w.Write(p.BlockFragment); err != nil {
		return err
	}
	if p.PadLength > 0 {
		if _, err := f.w.Write(paddingZeros[:p.PadLength]); err != nil {
			return err
		}
	}
	return nil
}

func (f *Framer) WriteContinuation(streamID uint32, endHeaders bool, blockFragment []byte) error {
	if streamID == 0 {
		return ErrInvalidStreamID
	}
	flags := Flags(0)
	if endHeaders {
		flags |= FlagContinuationEndHeaders
	}
	return f.writeFrame(FrameHeader{Length: uint32(len(blockFragment)), Type: FrameContinuation, Flags: flags, StreamID: streamID}, blockFragment)
}

func (f *Framer) WritePriority(streamID uint32, p Priority) error {
	if streamID == 0 {
		return ErrInvalidStreamID
	}
	dep := p.StreamDep
	if p.Exclusive {
		dep |= 0x80000000
	}
	f.smallBuf[0] = byte(dep >> 24)
	f.smallBuf[1] = byte(dep >> 16)
	f.smallBuf[2] = byte(dep >> 8)
	f.smallBuf[3] = byte(dep)
	f.smallBuf[4] = p.Weight
	return f.writeFrame(FrameHeader{Length: 5, Type: FramePriority, StreamID: streamID}, f.smallBuf[:5])
}

func (f *Framer) WriteRSTStream(streamID uint32, code ErrCode) error {
	if streamID == 0 {
		return ErrInvalidStreamID
	}
	f.smallBuf[0] = byte(code >> 24)
	f.smallBuf[1] = byte(code >> 16)
	f.smallBuf[2] = byte(code >> 8)
	f.smallBuf[3] = byte(code)
	return f.writeFrame(FrameHeader{Length: 4, Type: FrameRSTStream, StreamID: streamID}, f.smallBuf[:4])
}

func (f *Framer) WriteSettings(s SettingsParams) error {
	if s.N > 16 {
		return ErrSettingsLength
	}
	payload := make([]byte, 0, s.N*6)
	for i := 0; i < s.N; i++ {
		p := s.Pairs[i]
		payload = append(payload,
			byte(p.ID>>8), byte(p.ID),
			byte(p.Value>>24), byte(p.Value>>16), byte(p.Value>>8), byte(p.Value))
	}
	return f.writeFrame(FrameHeader{Length: uint32(len(payload)), Type: FrameSettings, StreamID: 0}, payload)
}

func (f *Framer) WriteSettingsAck() error {
	return f.writeFrame(FrameHeader{Length: 0, Type: FrameSettings, Flags: FlagSettingsAck, StreamID: 0}, nil)
}

func (f *Framer) WritePushPromise(streamID, promisedID uint32, blockFragment []byte, endHeaders bool, padLen uint8) error {
	if streamID == 0 {
		return ErrInvalidStreamID
	}
	flags := Flags(0)
	if endHeaders {
		flags |= FlagPushPromiseEndHeaders
	}
	if padLen > 0 {
		flags |= FlagPushPromisePadded
	}
	totalLen := uint32(4 + len(blockFragment) + int(padLen))
	if padLen > 0 {
		totalLen++
	}
	if err := f.writeHeader(FrameHeader{Length: totalLen, Type: FramePushPromise, Flags: flags, StreamID: streamID}); err != nil {
		return err
	}
	if padLen > 0 {
		f.smallBuf[0] = padLen
		if _, err := f.w.Write(f.smallBuf[:1]); err != nil {
			return err
		}
	}
	pid := promisedID & 0x7fffffff
	f.smallBuf[0] = byte(pid >> 24)
	f.smallBuf[1] = byte(pid >> 16)
	f.smallBuf[2] = byte(pid >> 8)
	f.smallBuf[3] = byte(pid)
	if _, err := f.w.Write(f.smallBuf[:4]); err != nil {
		return err
	}
	if _, err := f.w.Write(blockFragment); err != nil {
		return err
	}
	if padLen > 0 {
		if _, err := f.w.Write(paddingZeros[:padLen]); err != nil {
			return err
		}
	}
	return nil
}

func (f *Framer) WritePing(ack bool, data [8]byte) error {
	flags := Flags(0)
	if ack {
		flags |= FlagPingAck
	}
	return f.writeFrame(FrameHeader{Length: 8, Type: FramePing, Flags: flags, StreamID: 0}, data[:])
}

func (f *Framer) WriteGoAway(lastStreamID uint32, code ErrCode, debug []byte) error {
	totalLen := uint32(8 + len(debug))
	payload := make([]byte, totalLen)
	last := lastStreamID & 0x7fffffff
	payload[0] = byte(last >> 24)
	payload[1] = byte(last >> 16)
	payload[2] = byte(last >> 8)
	payload[3] = byte(last)
	payload[4] = byte(code >> 24)
	payload[5] = byte(code >> 16)
	payload[6] = byte(code >> 8)
	payload[7] = byte(code)
	copy(payload[8:], debug)
	return f.writeFrame(FrameHeader{Length: totalLen, Type: FrameGoAway, StreamID: 0}, payload)
}

func (f *Framer) WriteWindowUpdate(streamID uint32, increment uint32) error {
	if increment == 0 {
		return ErrZeroIncrement
	}
	inc := increment & 0x7fffffff
	f.smallBuf[0] = byte(inc >> 24)
	f.smallBuf[1] = byte(inc >> 16)
	f.smallBuf[2] = byte(inc >> 8)
	f.smallBuf[3] = byte(inc)
	return f.writeFrame(FrameHeader{Length: 4, Type: FrameWindowUpdate, StreamID: streamID}, f.smallBuf[:4])
}

// === Read side ===

func (f *Framer) ReadFrame(ctx context.Context, h Handler) (FrameHeader, error) {
	if f.r == nil {
		return FrameHeader{}, errors.New("poseidon/frame: Framer has no reader")
	}
	if cap(f.readBuf) < FrameHeaderSize {
		f.readBuf = make([]byte, FrameHeaderSize)
	}
	hdr := f.readBuf[:FrameHeaderSize]
	if _, err := io.ReadFull(f.r, hdr); err != nil {
		return FrameHeader{}, err
	}
	fh, err := ReadFrameHeader(hdr)
	if err != nil {
		return FrameHeader{}, err
	}
	if fh.Length > f.maxReadFrameSize {
		return fh, ErrFrameTooLarge
	}
	if cap(f.readBuf) < int(fh.Length) {
		f.readBuf = make([]byte, fh.Length)
	}
	payload := f.readBuf[:fh.Length]
	if fh.Length > 0 {
		if _, err := io.ReadFull(f.r, payload); err != nil {
			return fh, err
		}
	}

	switch fh.Type {
	case FrameData:
		return fh, f.dispatchData(fh, payload, h)
	case FrameHeaders:
		return fh, f.dispatchHeaders(fh, payload, h)
	case FramePriority:
		return fh, f.dispatchPriority(fh, payload, h)
	case FrameRSTStream:
		return fh, f.dispatchRSTStream(fh, payload, h)
	case FrameSettings:
		return fh, f.dispatchSettings(fh, payload, h)
	case FramePushPromise:
		return fh, f.dispatchPushPromise(fh, payload, h)
	case FramePing:
		return fh, f.dispatchPing(fh, payload, h)
	case FrameGoAway:
		return fh, f.dispatchGoAway(fh, payload, h)
	case FrameWindowUpdate:
		return fh, f.dispatchWindowUpdate(fh, payload, h)
	case FrameContinuation:
		return fh, f.dispatchContinuation(fh, payload, h)
	default:
		return fh, ErrUnknownFrameType
	}
}

func (f *Framer) dispatchData(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID == 0 {
		return ErrInvalidStreamID
	}
	data := payload
	var padLen uint8
	if fh.Flags&FlagDataPadded != 0 {
		var err error
		data, padLen, err = bytesx.StripPadding(payload)
		if err != nil {
			return err
		}
	}
	return h.OnData(fh, data, padLen)
}

func (f *Framer) dispatchHeaders(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID == 0 {
		return ErrInvalidStreamID
	}
	body := payload
	var padLen uint8
	if fh.Flags&FlagHeadersPadded != 0 {
		var err error
		body, padLen, err = bytesx.StripPadding(payload)
		if err != nil {
			return err
		}
	}
	var prio *Priority
	if fh.Flags&FlagHeadersPriority != 0 {
		if len(body) < 5 {
			return ErrShortRead
		}
		dep := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
		p := Priority{
			StreamDep: dep & 0x7fffffff,
			Exclusive: dep&0x80000000 != 0,
			Weight:    body[4],
		}
		prio = &p
		body = body[5:]
	}
	return h.OnHeaders(fh, HeaderBlock(body), prio, padLen)
}

func (f *Framer) dispatchPriority(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID == 0 {
		return ErrInvalidStreamID
	}
	if fh.Length != 5 {
		return ErrPriorityWrongLength
	}
	dep := uint32(payload[0])<<24 | uint32(payload[1])<<16 | uint32(payload[2])<<8 | uint32(payload[3])
	p := Priority{
		StreamDep: dep & 0x7fffffff,
		Exclusive: dep&0x80000000 != 0,
		Weight:    payload[4],
	}
	return h.OnPriority(fh, p)
}

func (f *Framer) dispatchRSTStream(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID == 0 {
		return ErrInvalidStreamID
	}
	if fh.Length != 4 {
		return ErrRSTWrongLength
	}
	code := ErrCode(uint32(payload[0])<<24 | uint32(payload[1])<<16 | uint32(payload[2])<<8 | uint32(payload[3]))
	return h.OnRSTStream(fh, code)
}

func (f *Framer) dispatchSettings(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID != 0 {
		return ErrInvalidStreamID
	}
	if fh.Flags&FlagSettingsAck != 0 {
		if fh.Length != 0 {
			return ErrSettingsAck
		}
		return h.OnSettings(fh, SettingsParams{})
	}
	if fh.Length%6 != 0 {
		return ErrSettingsLength
	}
	var s SettingsParams
	for i := 0; i+6 <= len(payload) && s.N < len(s.Pairs); i += 6 {
		s.Pairs[s.N] = SettingPair{
			ID:    SettingID(uint16(payload[i])<<8 | uint16(payload[i+1])),
			Value: uint32(payload[i+2])<<24 | uint32(payload[i+3])<<16 | uint32(payload[i+4])<<8 | uint32(payload[i+5]),
		}
		s.N++
	}
	return h.OnSettings(fh, s)
}

func (f *Framer) dispatchPushPromise(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID == 0 {
		return ErrInvalidStreamID
	}
	body := payload
	var padLen uint8
	if fh.Flags&FlagPushPromisePadded != 0 {
		var err error
		body, padLen, err = bytesx.StripPadding(payload)
		if err != nil {
			return err
		}
	}
	if len(body) < 4 {
		return ErrShortRead
	}
	pid := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
	pid &= 0x7fffffff
	return h.OnPushPromise(fh, pid, HeaderBlock(body[4:]), padLen)
}

func (f *Framer) dispatchPing(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID != 0 {
		return ErrInvalidStreamID
	}
	if fh.Length != 8 {
		return ErrPingWrongLength
	}
	var data [8]byte
	copy(data[:], payload)
	return h.OnPing(fh, data)
}

func (f *Framer) dispatchGoAway(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID != 0 {
		return ErrInvalidStreamID
	}
	if fh.Length < 8 {
		return ErrShortRead
	}
	last := uint32(payload[0])<<24 | uint32(payload[1])<<16 | uint32(payload[2])<<8 | uint32(payload[3])
	last &= 0x7fffffff
	code := ErrCode(uint32(payload[4])<<24 | uint32(payload[5])<<16 | uint32(payload[6])<<8 | uint32(payload[7]))
	return h.OnGoAway(fh, last, code, payload[8:])
}

func (f *Framer) dispatchWindowUpdate(fh FrameHeader, payload []byte, h Handler) error {
	if fh.Length != 4 {
		return ErrWindowWrongLength
	}
	inc := uint32(payload[0])<<24 | uint32(payload[1])<<16 | uint32(payload[2])<<8 | uint32(payload[3])
	inc &= 0x7fffffff
	if inc == 0 {
		return ErrZeroIncrement
	}
	return h.OnWindowUpdate(fh, inc)
}

func (f *Framer) dispatchContinuation(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID == 0 {
		return ErrInvalidStreamID
	}
	return h.OnContinuation(fh, HeaderBlock(payload))
}
