package frame

import (
	"github.com/lodgvideon/poseidon-http-client/internal/bufx"
	"github.com/lodgvideon/poseidon-http-client/trace"
)

// The HTTP/2 half of the observation seam described in issue #610.
//
// Both directions of the wire already funnel through this one type — every
// inbound frame through ReadFrame, every outbound one through writeHeader (plus
// the WriteHeaders fast path, which is the single exception and calls traceOut
// itself) — so the observation point was already here. It was just not exposed.
//
// Cost when off is one nil check per frame. Cost when on is a FrameInfo built
// on the stack and passed by value to an interface method, which does not
// allocate: nothing here takes the address of a local, and the only pointer
// FrameInfo carries points at Framer-owned storage that is already on the heap.
// BenchmarkFramer_WriteData_Traced and BenchmarkFramer_ReadFrame_Traced pin that
// at an absolute zero under the bench-gate; TestFramer_Trace_AddsNoAllocations
// pins it as a delta on every frame type.

// SetTracer installs t as the observer for every frame this Framer reads or
// writes, replacing any previous one. Passing nil turns tracing off.
//
// t.TraceFrame is called on whichever goroutine drives the Framer — the
// connection's reader for inbound frames, and the caller holding the write lock
// for outbound ones — so it must not block. See trace.Tracer.
func (f *Framer) SetTracer(t trace.Tracer) {
	f.tracer = t
	if t != nil && f.traceParams == nil {
		// Allocated once per Framer, not per frame, and only when someone is
		// actually watching: FrameInfo.Params must point at storage that already
		// lives on the heap, or handing it to an interface method would push a
		// stack local onto the heap for every SETTINGS frame.
		f.traceParams = new(trace.Params)
	}
}

// traceIn reports a frame that arrived. payload is the frame payload as read
// off the wire, or nil for a frame refused before its payload could be read.
//
// One inbound case is deliberately not reported: a frame whose header arrived
// and whose payload then did not. There is no event to describe — the peer went
// away mid-frame — and the preceding lines plus the error the caller gets
// already say so.
func (f *Framer) traceIn(h FrameHeader, payload []byte) {
	if f.tracer == nil {
		return
	}
	// PUSH_PROMISE is the only detail-bearing type that may be padded, and the
	// pad-length byte sits in front of the promised id (§6.6). Strip it here so
	// fillDetail sees the same logical payload the write side hands it.
	if h.Type == FramePushPromise && h.Flags&FlagPushPromisePadded != 0 && len(payload) > 0 {
		payload = payload[1:]
	}
	f.emit(trace.DirIn, h, payload)
}

// traceOut reports a frame that was written. payload is the frame's logical
// payload — padding already excluded — when the caller has it contiguously, and
// nil when it does not. A nil payload costs only the type-specific detail
// fields; the header line is reported either way.
func (f *Framer) traceOut(h FrameHeader, payload []byte) {
	if f.tracer == nil {
		return
	}
	f.emit(trace.DirOut, h, payload)
}

func (f *Framer) emit(dir trace.Direction, h FrameHeader, payload []byte) {
	info := trace.FrameInfo{
		Proto:     trace.ProtoH2,
		Dir:       dir,
		Type:      uint64(h.Type),
		TypeName:  h.Type.String(),
		Flags:     uint64(h.Flags),
		FlagNames: h.Type.FlagNames(h.Flags),
		StreamID:  uint64(h.StreamID),
		Length:    h.Length,
	}
	f.fillDetail(&info, h, payload)
	f.tracer.TraceFrame(info)
}

// fillDetail decodes the type-specific scalars a human reading a frame log
// needs next to the header — the code on an RST_STREAM, the credit on a
// WINDOW_UPDATE, the parameters of a SETTINGS.
//
// Every read is length-guarded and none of them rejects anything: this runs
// before the dispatch that validates the frame, deliberately, so that a
// malformed frame still produces a line. A frame log whose last entry is
// missing precisely because the frame was bad would omit the one event worth
// seeing.
func (f *Framer) fillDetail(info *trace.FrameInfo, h FrameHeader, payload []byte) {
	if payload == nil && h.Length > 0 {
		// The frame has a payload and this caller does not have it: a refused
		// oversized frame, or a write path that emits its payload in pieces.
		// Nothing is known, so nothing is claimed — which is the distinction
		// trace.Detail exists to carry. A zero-length payload is different, and
		// falls through: an empty non-ACK SETTINGS really does have no parameters.
		return
	}
	//exhaustive:ignore // Only the types carrying scalars beyond the header
	// appear here; every other type is fully described by the header alone.
	switch h.Type {
	case FrameRSTStream:
		if len(payload) >= 4 {
			setErrCode(info, ErrCode(be32(payload)))
		}
	case FrameGoAway:
		if len(payload) >= 8 {
			info.LastStreamID = uint64(bufx.ReadUint31(payload[:4]))
			info.Detail |= trace.DetailLastStreamID
			setErrCode(info, ErrCode(be32(payload[4:])))
		}
	case FrameWindowUpdate:
		if len(payload) >= 4 {
			info.Increment = uint64(bufx.ReadUint31(payload[:4]))
			info.Detail |= trace.DetailIncrement
		}
	case FramePushPromise:
		if len(payload) >= 4 {
			info.PromisedID = uint64(bufx.ReadUint31(payload[:4]))
			info.Detail |= trace.DetailPromisedID
		}
	case FrameSettings:
		if h.Flags&FlagSettingsAck != 0 || f.traceParams == nil {
			return
		}
		p := f.traceParams
		p.Reset()
		for off := 0; off+settingsPairWireSize <= len(payload); off += settingsPairWireSize {
			id := SettingID(be16(payload[off:]))
			p.Add(uint64(id), settingName(id), uint64(be32(payload[off+2:])))
		}
		info.Params = p
		info.Detail |= trace.DetailParams
	}
}

func setErrCode(info *trace.FrameInfo, code ErrCode) {
	info.ErrCode = uint64(code)
	info.ErrCodeName = code.String()
	info.Detail |= trace.DetailErrCode
}

// settingName reports "" rather than UNKNOWN for an unregistered identifier, so
// that a renderer prints the number it does have instead of a word that says
// nothing. trace.Param documents the empty string as exactly this.
func settingName(id SettingID) string {
	if n := id.String(); n != trace.UnknownName {
		return n
	}
	return ""
}

func be16(b []byte) uint16 {
	_ = b[1] // BCE hint
	return uint16(b[0])<<8 | uint16(b[1])
}

func be32(b []byte) uint32 {
	_ = b[3] // BCE hint
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
