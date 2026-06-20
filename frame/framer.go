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
	OnOrigin(h FrameHeader, origins []string) error
	OnAltSvc(h FrameHeader, entries []AltSvcEntry) error
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

	readBuf    []byte
	readBufPtr *[]byte // pool handle (nil after Close)
	hdrBuf     [FrameHeaderSize]byte
	smallBuf   [16]byte
	// writeBuf is a per-Framer scratch buffer for the WriteHeaders
	// fast path. It must live on the *Framer struct (not the stack)
	// so that escape analysis does not promote it to the heap when
	// io.Writer.Write is called with a sub-slice.
	writeBuf [256]byte
}

// NewFramer constructs a Framer over the given writer and reader.
// Either side may be nil if only the other is needed.
//
// The internal read buffer comes from a shared sync.Pool. Call Close
// when done to return it; otherwise the buffer is GC'd via the pool's
// finalization (slower than reuse). Connection layers SHOULD call
// Close as part of their own shutdown.
func NewFramer(w io.Writer, r io.Reader) *Framer {
	rb := bytesx.GetReadBuf(int(defaultMaxFrameSize) + FrameHeaderSize)
	return &Framer{
		w:                w,
		r:                r,
		maxReadFrameSize: defaultMaxFrameSize,
		readBuf:          *rb,
		readBufPtr:       rb,
	}
}

// Close returns the internal read buffer to the shared pool. Subsequent
// ReadFrame calls will allocate or fetch a fresh buffer if needed.
// Idempotent.
func (f *Framer) Close() {
	if f.readBufPtr == nil {
		return
	}
	*f.readBufPtr = f.readBuf
	bytesx.PutReadBuf(f.readBufPtr)
	f.readBufPtr = nil
	f.readBuf = nil
}

// SetMaxReadFrameSize sets the maximum frame payload length the Framer
// will accept on read AND emit on write. Per RFC 7540 §6.5.2 the
// receiver advertises this via SETTINGS_MAX_FRAME_SIZE; the SENDER
// must independently respect the PEER's advertised value, which lives
// outside the framer (callers track peer settings separately).
func (f *Framer) SetMaxReadFrameSize(n uint32) { f.maxReadFrameSize = n }
// SetMaxHeaderListSize sets the read-side cap on a header block.
func (f *Framer) SetMaxHeaderListSize(n uint32) { f.maxHeaderListSize = n }
// SetReadBuffer overrides the internal read buffer (useful for pooling).
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

// WriteData writes a DATA frame for streamID with the END_STREAM flag
// set when endStream is true.
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

// WriteDataPadded writes a DATA frame with the given padding length.
func (f *Framer) WriteDataPadded(streamID uint32, endStream bool, data []byte, padLen uint8) error {
	if streamID == 0 {
		return ErrInvalidStreamID
	}
	flags := FlagDataPadded
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

// WriteHeaders writes a HEADERS frame per the parameters in p.
//
// Fast path: when p has no padding and no priority, and the entire
// frame (9-byte header + block) fits in f.writeBuf (256 bytes),
// header and block are coalesced into a single io.Writer.Write
// call. This halves the per-frame syscall count on the common
// GET/POST request path; on a benchmark against an in-process H2C
// peer this saves one TCP write round-trip per HEADERS frame
// (~3-5 µs out of ~30 µs total request latency).
//
// The scratch lives on the *Framer struct itself (not the stack) so
// that taking its address — which the runtime does to enforce
// io.Writer's slice non-retention — does not cause it to escape to
// the heap. The cost is +256 bytes per Framer.
//
// Slow path: padded frames, frames with priority, and frames whose
// total length exceeds f.writeBuf fall back to the per-section
// Write path. Error-injection tests (errWriter{n: 9}) still trigger
// the second io.Writer.Write for the payload, just like before.
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
	if totalLen > f.maxReadFrameSize {
		return ErrFrameTooLarge
	}
	// Fast path: no padding, no priority, header+payload fit in f.writeBuf.
	if p.PadLength == 0 && p.Priority == nil && 9+totalLen <= uint32(len(f.writeBuf)) {
		h := FrameHeader{Length: totalLen, Type: FrameHeaders, Flags: flags, StreamID: p.StreamID}
		WriteFrameHeader(f.hdrBuf[:], h)
		copy(f.writeBuf[:9], f.hdrBuf[:9])
		copy(f.writeBuf[9:9+len(p.BlockFragment)], p.BlockFragment)
		if _, err := f.w.Write(f.writeBuf[:9+len(p.BlockFragment)]); err != nil {
			return err
		}
		return nil
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

// WriteContinuation writes a CONTINUATION frame for the given stream.
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

// WritePriority writes a PRIORITY frame.
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

// WriteRSTStream writes an RST_STREAM frame with the given error code.
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

// WriteSettings writes a SETTINGS frame carrying s. Zero allocations:
// pairs are encoded into a stack-resident scratch buffer (max 16 × 6
// bytes = 96 bytes — fits comfortably below the heap-escape threshold
// for a same-package call).
func (f *Framer) WriteSettings(s SettingsParams) error {
	if s.N > 16 {
		return ErrSettingsLength
	}
	var scratch [96]byte
	off := 0
	for i := 0; i < s.N; i++ {
		p := s.Pairs[i]
		scratch[off] = byte(p.ID >> 8)
		scratch[off+1] = byte(p.ID)
		scratch[off+2] = byte(p.Value >> 24)
		scratch[off+3] = byte(p.Value >> 16)
		scratch[off+4] = byte(p.Value >> 8)
		scratch[off+5] = byte(p.Value)
		off += 6
	}
	payload := scratch[:off]
	return f.writeFrame(FrameHeader{Length: uint32(len(payload)), Type: FrameSettings, StreamID: 0}, payload)
}

// WriteSettingsAck writes an empty SETTINGS frame with the ACK flag.
func (f *Framer) WriteSettingsAck() error {
	return f.writeFrame(FrameHeader{Length: 0, Type: FrameSettings, Flags: FlagSettingsAck, StreamID: 0}, nil)
}

// WritePushPromise writes a PUSH_PROMISE frame.
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
	if totalLen > f.maxReadFrameSize {
		return ErrFrameTooLarge
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

// WritePing writes a PING frame, optionally with the ACK flag.
func (f *Framer) WritePing(ack bool, data [8]byte) error {
	flags := Flags(0)
	if ack {
		flags |= FlagPingAck
	}
	return f.writeFrame(FrameHeader{Length: 8, Type: FramePing, Flags: flags, StreamID: 0}, data[:])
}

// WriteGoAway writes a GOAWAY frame. Zero allocations when debug is
// empty (the 8-byte fixed prefix lives in smallBuf); otherwise debug
// is written directly via the underlying io.Writer without copying.
func (f *Framer) WriteGoAway(lastStreamID uint32, code ErrCode, debug []byte) error {
	totalLen := uint32(8 + len(debug))
	if totalLen > f.maxReadFrameSize {
		return ErrFrameTooLarge
	}
	last := lastStreamID & 0x7fffffff
	f.smallBuf[0] = byte(last >> 24)
	f.smallBuf[1] = byte(last >> 16)
	f.smallBuf[2] = byte(last >> 8)
	f.smallBuf[3] = byte(last)
	f.smallBuf[4] = byte(code >> 24)
	f.smallBuf[5] = byte(code >> 16)
	f.smallBuf[6] = byte(code >> 8)
	f.smallBuf[7] = byte(code)
	if err := f.writeHeader(FrameHeader{Length: totalLen, Type: FrameGoAway, StreamID: 0}); err != nil {
		return err
	}
	if _, err := f.w.Write(f.smallBuf[:8]); err != nil {
		return err
	}
	if len(debug) > 0 {
		if _, err := f.w.Write(debug); err != nil {
			return err
		}
	}
	return nil
}

// WriteAltSvc writes an ALTSVC frame (RFC 7838 §4). streamID=0 sends
// server-wide alternative services (entries MUST have non-empty Origin);
// non-zero streamID sends per-request alternatives (entries MUST have
// empty Origin). An empty entries slice clears all alternative services.
func (f *Framer) WriteAltSvc(streamID uint32, entries []AltSvcEntry) error {
	streamID &= 0x7fffffff
	if len(entries) == 0 {
		return f.writeFrame(FrameHeader{Length: 0, Type: FrameAltSvc, StreamID: streamID}, nil)
	}
	buf := make([]byte, 0, len(entries)*32)
	for _, e := range entries {
		if len(e.Origin) > 0xFFFF {
			return ErrFrameTooLarge
		}
		if len(e.AltValue) > 0xFFFFFF {
			return ErrFrameTooLarge
		}
		buf = append(buf, byte(len(e.Origin)>>8), byte(len(e.Origin)))
		buf = append(buf, e.Origin...)
		buf = append(buf, byte(len(e.AltValue)>>16), byte(len(e.AltValue)>>8), byte(len(e.AltValue)))
		buf = append(buf, e.AltValue...)
	}
	if uint32(len(buf)) > f.maxReadFrameSize {
		return ErrFrameTooLarge
	}
	return f.writeFrame(FrameHeader{Length: uint32(len(buf)), Type: FrameAltSvc, StreamID: streamID}, buf)
}

// WriteWindowUpdate writes a WINDOW_UPDATE frame.
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

// ReadFrame reads one frame from the underlying reader and dispatches
// it through h. Honors ctx.Err() at entry — a pre-cancelled ctx returns
// immediately. Cancellation that races a blocked read is the caller's
// responsibility to drive via the underlying transport (e.g. by
// closing the net.Conn or setting a read deadline) — Framer does not
// own the transport's deadline.
func (f *Framer) ReadFrame(ctx context.Context, h Handler) (FrameHeader, error) {
	if f.r == nil {
		return FrameHeader{}, errors.New("poseidon/frame: Framer has no reader")
	}
	if err := ctx.Err(); err != nil {
		return FrameHeader{}, err
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
	case FrameOrigin:
		return fh, f.dispatchOrigin(fh, payload, h)
	case FrameAltSvc:
		return fh, f.dispatchAltSvc(fh, payload, h)
	default:
		// RFC 7540 §5.5: implementations MUST ignore frames they do not
		// understand and continue. Drain the payload (already read) and
		// return without error.
		return fh, nil
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
	if len(payload)/6 > len(SettingsParams{}.Pairs) {
		return ErrSettingsLength
	}
	var s SettingsParams
	for i := 0; i+6 <= len(payload); i += 6 {
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

// dispatchOrigin parses an ORIGIN frame (RFC 8336 §3) and calls OnOrigin.
// ORIGIN frames MUST be sent on stream 0; non-zero stream ID is a
// PROTOCOL_ERROR. Each origin entry is a 2-byte big-endian length
// prefix followed by the origin ASCII string (scheme://host[:port]).
func (f *Framer) dispatchOrigin(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID != 0 {
		return ErrProtocolError
	}
	var origins []string
	for len(payload) >= 2 {
		n := int(payload[0])<<8 | int(payload[1])
		payload = payload[2:]
		if n > len(payload) {
			return ErrProtocolError
		}
		origins = append(origins, string(payload[:n]))
		payload = payload[n:]
	}
	if len(payload) > 0 {
		return ErrProtocolError // trailing bytes — malformed
	}
	return h.OnOrigin(fh, origins)
}

// dispatchAltSvc parses an ALTSVC frame (RFC 7838 §4) and calls OnAltSvc.
// Each entry is: uint16 Origin-Len, Origin bytes, uint24 Alt-Value-Len,
// Alt-Value bytes. An empty payload signals clearing all alternative
// services. Stream-0 frames MUST have non-empty Origin in each entry;
// non-zero-stream frames MUST have empty Origin.
func (f *Framer) dispatchAltSvc(fh FrameHeader, payload []byte, h Handler) error {
	// Empty payload = clear all alt-svc entries (RFC 7838 §4).
	if len(payload) == 0 {
		return h.OnAltSvc(fh, nil)
	}
	var entries []AltSvcEntry
	for len(payload) >= 5 { // minimum: 2 + 0 + 3 + 0
		originLen := int(payload[0])<<8 | int(payload[1])
		if 2+originLen > len(payload) {
			return ErrProtocolError
		}
		origin := string(payload[2 : 2+originLen])
		rest := payload[2+originLen:]
		if len(rest) < 3 {
			return ErrProtocolError
		}
		altLen := int(rest[0])<<16 | int(rest[1])<<8 | int(rest[2])
		if 3+altLen > len(rest) {
			return ErrProtocolError
		}
		altValue := string(rest[3 : 3+altLen])
		entries = append(entries, AltSvcEntry{Origin: origin, AltValue: altValue})
		payload = rest[3+altLen:]
	}
	if len(payload) > 0 {
		return ErrProtocolError // trailing bytes
	}
	return h.OnAltSvc(fh, entries)
}
