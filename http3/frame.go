package http3

import (
	"errors"
	"io"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// Frame types (RFC 9114 §7.2, §11.2.1). The reserved h2-carryover types
// (0x02, 0x06, 0x08, 0x09) are intentionally absent; a stream reader treats them
// as an H3_FRAME_UNEXPECTED connection error (§7.2.8).
const (
	FrameData        uint64 = 0x00
	FrameHeaders     uint64 = 0x01
	FrameCancelPush  uint64 = 0x03
	FrameSettings    uint64 = 0x04
	FramePushPromise uint64 = 0x05
	FrameGoaway      uint64 = 0x07
	FrameMaxPushID   uint64 = 0x0d
)

// SETTINGS identifiers (RFC 9114 §7.2.4.1, RFC 9204 §5).
const (
	SettingQPACKMaxTableCapacity uint64 = 0x01
	SettingMaxFieldSectionSize   uint64 = 0x06
	SettingQPACKBlockedStreams   uint64 = 0x07
)

// ErrH3Settings is returned when a SETTINGS payload repeats an identifier or
// carries a reserved identifier. It maps to the H3_SETTINGS_ERROR error code
// (RFC 9114 §7.2.4.1).
var ErrH3Settings = errors.New("http3: malformed settings")

// ErrH3Frame is returned when a frame payload terminates before the end of its
// fields — for SETTINGS, a setting identifier or value cut off by the frame
// length. It maps to the H3_FRAME_ERROR error code (RFC 9114 §7.1).
var ErrH3Frame = errors.New("http3: malformed frame")

// appendV appends v as a QUIC varint (RFC 9000 §16) without allocating. v must
// be <= bytesx.MaxVarint (2^62-1); larger values would collide with the varint
// length prefix. Stream and push IDs and frame lengths are always in range.
func appendV(dst []byte, v uint64) []byte {
	var b [8]byte
	n := bytesx.WriteVarint(b[:], v)
	return append(dst, b[:n]...)
}

// AppendFrameHeader appends an HTTP/3 frame header — the Type and Length varints
// (RFC 9114 §7.1). The caller then appends or streams the length payload bytes.
func AppendFrameHeader(dst []byte, typ, length uint64) []byte {
	dst = appendV(dst, typ)
	return appendV(dst, length)
}

// AppendData appends a complete DATA frame (RFC 9114 §7.2.1).
func AppendData(dst, data []byte) []byte {
	dst = AppendFrameHeader(dst, FrameData, uint64(len(data)))
	return append(dst, data...)
}

// AppendHeaders appends a complete HEADERS frame wrapping a QPACK-encoded field
// section (RFC 9114 §7.2.2).
func AppendHeaders(dst, fieldSection []byte) []byte {
	dst = AppendFrameHeader(dst, FrameHeaders, uint64(len(fieldSection)))
	return append(dst, fieldSection...)
}

// AppendGoaway appends a GOAWAY frame carrying a stream or push ID
// (RFC 9114 §7.2.6).
func AppendGoaway(dst []byte, id uint64) []byte {
	dst = AppendFrameHeader(dst, FrameGoaway, uint64(bytesx.VarintLen(id)))
	return appendV(dst, id)
}

// AppendMaxPushID appends a MAX_PUSH_ID frame (RFC 9114 §7.2.7).
func AppendMaxPushID(dst []byte, pushID uint64) []byte {
	dst = AppendFrameHeader(dst, FrameMaxPushID, uint64(bytesx.VarintLen(pushID)))
	return appendV(dst, pushID)
}

// AppendCancelPush appends a CANCEL_PUSH frame (RFC 9114 §7.2.3).
func AppendCancelPush(dst []byte, pushID uint64) []byte {
	dst = AppendFrameHeader(dst, FrameCancelPush, uint64(bytesx.VarintLen(pushID)))
	return appendV(dst, pushID)
}

// Setting is one HTTP/3 SETTINGS parameter (identifier + value).
type Setting struct{ ID, Value uint64 }

// AppendSettings appends a complete SETTINGS frame (RFC 9114 §7.2.4).
func AppendSettings(dst []byte, settings []Setting) []byte {
	var n uint64
	for _, s := range settings {
		n += uint64(bytesx.VarintLen(s.ID) + bytesx.VarintLen(s.Value))
	}
	dst = AppendFrameHeader(dst, FrameSettings, n)
	for _, s := range settings {
		dst = appendV(dst, s.ID)
		dst = appendV(dst, s.Value)
	}
	return dst
}

// validateSettings rejects a caller-supplied settings slice this client must not
// put on the wire: a repeated identifier (RFC 9114 §7.2.4, "The same setting
// identifier MUST NOT occur more than once in the SETTINGS frame") or one of the
// reserved HTTP/2-carryover identifiers 0x02–0x05, which §7.2.4.1 says MUST NOT be
// sent. Either makes a conformant peer answer with H3_SETTINGS_ERROR — and this
// client's own ParseSettings rejects both. The MUST NOT is scoped to identifiers
// "defined in [HTTP/2] where there is no corresponding HTTP/3 setting", and the
// HTTP/2 registry starts at 0x01, so the set is exactly 0x02-0x05 -- 0x00 lies
// outside it and is left to the caller. Dial's defaults always pass; the gate
// exists for the exported NewClient, whose caller supplies the slice.
func validateSettings(settings []Setting) error {
	seen := make(map[uint64]struct{}, len(settings))
	for _, s := range settings {
		if s.ID >= 0x02 && s.ID <= 0x05 {
			return ErrH3Settings
		}
		if _, dup := seen[s.ID]; dup {
			return ErrH3Settings
		}
		seen[s.ID] = struct{}{}
	}
	return nil
}

// ParseFrameHeader reads an HTTP/3 frame header from b — the Type and Length
// varints (RFC 9114 §7.1) — returning the type, the payload length, and the
// number of header bytes consumed. If the header is not fully present it returns
// io.ErrUnexpectedEOF (a benign "need more bytes" signal), and a stream reader
// buffers more bytes and retries. It does not validate length against the
// available payload bytes: the caller must bound-check (and cap to a maximum
// frame size) before slicing the payload.
func ParseFrameHeader(b []byte) (typ, length uint64, n int, err error) {
	typ, tn := bytesx.ReadVarint(b)
	if tn == 0 {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	length, ln := bytesx.ReadVarint(b[tn:])
	if ln == 0 {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	return typ, length, tn + ln, nil
}

// ParseSettings decodes a SETTINGS frame payload into its parameters
// (RFC 9114 §7.2.4). A truncated identifier/value pair — a field cut off by the
// frame length — is an ErrH3Frame error (→ H3_FRAME_ERROR, §7.1); a repeated
// identifier or a reserved HTTP/2-carryover identifier (0x02–0x05) is an
// ErrH3Settings error (→ H3_SETTINGS_ERROR, §7.2.4.1).
func ParseSettings(payload []byte) ([]Setting, error) {
	var out []Setting
	seen := make(map[uint64]struct{})
	for len(payload) > 0 {
		id, in := bytesx.ReadVarint(payload)
		if in == 0 {
			return nil, ErrH3Frame // a setting cut off by the frame length (§7.1)
		}
		payload = payload[in:]
		val, vn := bytesx.ReadVarint(payload)
		if vn == 0 {
			return nil, ErrH3Frame // the value cut off by the frame length (§7.1)
		}
		payload = payload[vn:]
		if id >= 0x02 && id <= 0x05 { // reserved h2 settings MUST be rejected (§7.2.4.1)
			return nil, ErrH3Settings
		}
		if _, dup := seen[id]; dup {
			return nil, ErrH3Settings
		}
		seen[id] = struct{}{}
		out = append(out, Setting{id, val})
	}
	return out, nil
}
