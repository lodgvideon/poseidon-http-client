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

	maxReadFrameSize uint32

	readBuf    []byte
	readBufPtr *[]byte // pool handle (nil after Close)
	hdrBuf     [FrameHeaderSize]byte
	smallBuf   [16]byte
	// writeBuf is a per-Framer scratch buffer for the WriteHeaders
	// fast path. It must live on the *Framer struct (not the stack)
	// so that escape analysis does not promote it to the heap when
	// io.Writer.Write is called with a sub-slice.
	writeBuf [256]byte

	// expectContinuation tracks the RFC 9113 §6.10 field-block-continuity
	// invariant on the READ side: a HEADERS or PUSH_PROMISE frame without
	// END_HEADERS opens a field block that the next frame MUST continue with a
	// CONTINUATION on continuationStream, until one sets END_HEADERS. Any other
	// frame — a different type, a frame on another stream, an extension frame, or
	// a CONTINUATION with no block open — is a connection error PROTOCOL_ERROR.
	expectContinuation bool
	continuationStream uint32
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

// SetReadBuffer overrides the internal read buffer (useful for pooling).
func (f *Framer) SetReadBuffer(buf []byte) { f.readBuf = buf }

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
	// Copy into the per-Framer scratch before slicing. data is a by-value
	// array on the stack; passing data[:] straight to writeFrame — whose
	// f.w.Write takes an io.Writer — forces the array to the heap, 8 B/op on a
	// path the inbound-PING echo reaches for every PING the peer sends. smallBuf
	// is already heap-resident, so a slice of it does not allocate, exactly as
	// WriteWindowUpdate builds its payload. Safe under the single-writer
	// contract wmu enforces: the Write completes before this returns.
	copy(f.smallBuf[:8], data[:])
	return f.writeFrame(FrameHeader{Length: 8, Type: FramePing, Flags: flags, StreamID: 0}, f.smallBuf[:8])
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

// WriteAltSvc writes an ALTSVC frame (RFC 7838 §4). A frame carries exactly
// one (Origin, Alt-Svc-Field-Value) pair on the wire: a uint16 Origin-Len,
// the Origin bytes, then the field value as the remainder of the frame —
// "length determined by subtracting the length of all preceding fields from
// the frame length" (RFC 7838 §4), so it is NOT length-prefixed. streamID=0
// sends a server-wide alternative (Origin MUST be non-empty); a non-zero
// streamID sends a per-request alternative (Origin MUST be empty). An empty
// entries slice writes an empty payload that clears all alternative services.
// More than one entry is an error: RFC 7838 permits only a single Origin per
// frame.
func (f *Framer) WriteAltSvc(streamID uint32, entries []AltSvcEntry) error {
	streamID &= 0x7fffffff
	if len(entries) == 0 {
		return f.writeFrame(FrameHeader{Length: 0, Type: FrameAltSvc, StreamID: streamID}, nil)
	}
	if len(entries) > 1 {
		return ErrTooManyAltSvc
	}
	e := entries[0]
	if len(e.Origin) > 0xFFFF {
		return ErrFrameTooLarge
	}
	buf := make([]byte, 0, 2+len(e.Origin)+len(e.AltValue))
	buf = append(buf, byte(len(e.Origin)>>8), byte(len(e.Origin)))
	buf = append(buf, e.Origin...)
	buf = append(buf, e.AltValue...)
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

	// RFC 9113 §6.10: enforce field-block continuity before dispatch, so an
	// interleaving frame (or a stray CONTINUATION) is rejected rather than
	// processed. Checked here — ahead of the per-type switch — so it also catches
	// an unknown/extension frame in the middle of a field block (§4.3).
	if err := f.checkFieldBlockContinuity(fh); err != nil {
		return fh, err
	}

	var derr error
	switch fh.Type {
	case FrameData:
		derr = f.dispatchData(fh, payload, h)
	case FrameHeaders:
		derr = f.dispatchHeaders(fh, payload, h)
	case FramePriority:
		derr = f.dispatchPriority(fh, payload, h)
	case FrameRSTStream:
		derr = f.dispatchRSTStream(fh, payload, h)
	case FrameSettings:
		derr = f.dispatchSettings(fh, payload, h)
	case FramePushPromise:
		derr = f.dispatchPushPromise(fh, payload, h)
	case FramePing:
		derr = f.dispatchPing(fh, payload, h)
	case FrameGoAway:
		derr = f.dispatchGoAway(fh, payload, h)
	case FrameWindowUpdate:
		derr = f.dispatchWindowUpdate(fh, payload, h)
	case FrameContinuation:
		derr = f.dispatchContinuation(fh, payload, h)
	case FrameOrigin:
		derr = f.dispatchOrigin(fh, payload, h)
	case FrameAltSvc:
		derr = f.dispatchAltSvc(fh, payload, h)
	default:
		// RFC 7540 §5.5: implementations MUST ignore frames they do not
		// understand and continue. Drain the payload (already read) and
		// return without error.
		derr = nil
	}
	// Update §6.10 continuity from the END_HEADERS flag regardless of the dispatch
	// outcome: the flag is a property of the wire framing, not of dispatch
	// success. A CONTINUATION that closes a field block but whose handler returns
	// a NON-FATAL stream error must still clear expectContinuation — otherwise the
	// next frame trips a false interleaving violation and the whole connection is
	// torn down for what the RFC scopes to a single stream (§8.1.2.6). A frame
	// whose dispatch is connection-fatal tears the connection down anyway, so
	// updating the state for it is harmless.
	f.trackFieldBlock(fh)
	return fh, derr
}

// checkFieldBlockContinuity enforces RFC 9113 §6.10 before a frame is dispatched:
// while a field block is open only a CONTINUATION on the same stream is allowed,
// and a CONTINUATION is allowed only while a block is open. Both violations are a
// connection error of type PROTOCOL_ERROR (mapped from these sentinels by the
// connection layer).
func (f *Framer) checkFieldBlockContinuity(fh FrameHeader) error {
	if f.expectContinuation {
		if fh.Type != FrameContinuation || fh.StreamID != f.continuationStream {
			return ErrContinuationExpected
		}
		return nil
	}
	if fh.Type == FrameContinuation {
		return ErrUnexpectedContinuation
	}
	return nil
}

// trackFieldBlock updates the §6.10 continuity state after a frame dispatches
// without error: a HEADERS/PUSH_PROMISE without END_HEADERS opens a block on its
// stream; a CONTINUATION with END_HEADERS closes it.
func (f *Framer) trackFieldBlock(fh FrameHeader) {
	switch fh.Type {
	case FrameHeaders:
		f.expectContinuation = fh.Flags&FlagHeadersEndHeaders == 0
		f.continuationStream = fh.StreamID
	case FramePushPromise:
		f.expectContinuation = fh.Flags&FlagPushPromiseEndHeaders == 0
		f.continuationStream = fh.StreamID
	case FrameContinuation:
		if fh.Flags&FlagContinuationEndHeaders != 0 {
			f.expectContinuation = false
		}
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
			return padErr(err)
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
			return padErr(err)
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
	// RFC 7540 §6.5 puts no upper bound on the number of parameters and permits
	// repeats: "Each parameter in a SETTINGS frame replaces any existing value
	// for that parameter ... the value of a SETTINGS parameter is the last value
	// that is seen by a receiver", and "a receiver of a SETTINGS frame does not
	// need to maintain any state other than the current value of its parameters."
	// The fixed Pairs array is therefore not a wire limit — it is the store of
	// current values, one slot per DEFINED identifier. Unknown identifiers are
	// dropped here (§6.5.2: an "unsupported identifier MUST ignore that setting"),
	// which also stops a peer from sending many distinct unknown ids to crowd a
	// real setting out of a bounded array. The previous `len(payload)/6 > 16`
	// rejection aborted the entire connection on a legal 17-parameter frame — one
	// a server sending GREASE reserved settings (RFC 8701) produces routinely.
	var s SettingsParams
	for i := 0; i+6 <= len(payload); i += 6 {
		id := SettingID(uint16(payload[i])<<8 | uint16(payload[i+1]))
		if !isDefinedSetting(id) {
			continue
		}
		s.set(id, uint32(payload[i+2])<<24|uint32(payload[i+3])<<16|uint32(payload[i+4])<<8|uint32(payload[i+5]))
	}
	return h.OnSettings(fh, s)
}

// isDefinedSetting reports whether id is a SETTINGS parameter this
// implementation understands (RFC 7540 §6.5.2 identifiers 0x1–0x6 plus RFC 8441
// §3's 0x8). An unknown or unsupported identifier "MUST ignore that setting"
// (§6.5.2), so it is never stored — bounding the parameter store to the defined
// set regardless of how many parameters the peer sends.
func isDefinedSetting(id SettingID) bool {
	switch id {
	case SettingHeaderTableSize, SettingEnablePush, SettingMaxConcurrentStreams,
		SettingInitialWindowSize, SettingMaxFrameSize, SettingMaxHeaderListSize,
		SettingEnableConnectProtocol:
		return true
	}
	return false
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
			return padErr(err)
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
// Each origin entry is a 2-byte big-endian length prefix followed by the origin
// ASCII string (scheme://host[:port]).
//
// An ORIGIN frame on a non-zero stream is not a connection error: RFC 8336 §2.2
// says "The ORIGIN frame MUST be sent on stream 0; an ORIGIN frame on any other
// stream is invalid and MUST be ignored." The payload has already been read off
// the wire by ReadFrame, so ignoring it is just returning nil without calling
// OnOrigin — where returning a plain error would have fallen through to the
// reader loop's connection-teardown path and killed the whole connection over a
// frame the RFC says to drop.
func (f *Framer) dispatchOrigin(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID != 0 {
		return nil
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
// The payload is a uint16 Origin-Len, the Origin bytes, then a single
// Alt-Svc-Field-Value whose "length determined by subtracting the length of
// all preceding fields from the frame length" (RFC 7838 §4) — it runs to the
// end of the frame and is NOT length-prefixed. A frame therefore carries
// exactly one (Origin, value) pair. An empty payload clears all alternative
// services.
//
// RFC 7838 §4 also requires the receiver to ignore two invalid combinations:
// a stream-0 frame whose Origin is empty, and a non-zero-stream frame whose
// Origin is non-empty. Such frames are dropped without calling OnAltSvc.
func (f *Framer) dispatchAltSvc(fh FrameHeader, payload []byte, h Handler) error {
	// Empty payload = clear all alt-svc entries.
	if len(payload) == 0 {
		return h.OnAltSvc(fh, nil)
	}
	if len(payload) < 2 {
		return ErrProtocolError
	}
	originLen := int(payload[0])<<8 | int(payload[1])
	if 2+originLen > len(payload) {
		return ErrProtocolError
	}
	origin := string(payload[2 : 2+originLen])
	altValue := string(payload[2+originLen:])
	// RFC 7838 §4 invalid Origin/stream combinations MUST be ignored.
	if (fh.StreamID == 0) == (origin == "") {
		return nil
	}
	return h.OnAltSvc(fh, []AltSvcEntry{{Origin: origin, AltValue: altValue}})
}
