package http3

import "strconv"

// FrameTypeName returns the RFC 9114 §11.2.1 registry name of an HTTP/3 frame
// type — "HEADERS", "SETTINGS" — or "UNKNOWN(0x…)" when the type is not one
// this implementation names.
//
// It is a function rather than a String method because the frame-type constants
// in this package are plain uint64 values, which is what a QUIC varint decodes
// to; giving them a named type now would break every caller that stores one.
//
// Two groups get named that no encoder here will ever emit, because a reader
// meets them and a trace of that moment is the whole point:
//
//   - The four h2-carryover types (0x02, 0x06, 0x08, 0x09), which RFC 9114
//     §11.2.1 reserves precisely so an HTTP/2 frame arriving on an HTTP/3 stream
//     is recognised as such. Receiving one is an H3_FRAME_UNEXPECTED connection
//     error (§7.2.8), and "reserved HTTP/2 PING" says why far better than 0x06.
//   - The GREASE types of §7.2.8, 0x1f * N + 0x21, which a conformant peer sends
//     to keep the extension point exercised and which must be ignored.
func FrameTypeName(t uint64) string {
	switch t {
	case FrameData:
		return "DATA"
	case FrameHeaders:
		return "HEADERS"
	case FrameCancelPush:
		return "CANCEL_PUSH"
	case FrameSettings:
		return "SETTINGS"
	case FramePushPromise:
		return "PUSH_PROMISE"
	case FrameGoaway:
		return "GOAWAY"
	case FrameMaxPushID:
		return "MAX_PUSH_ID"
	case 0x02:
		return "RESERVED_H2_PRIORITY"
	case 0x06:
		return "RESERVED_H2_PING"
	case 0x08:
		return "RESERVED_H2_WINDOW_UPDATE"
	case 0x09:
		return "RESERVED_H2_CONTINUATION"
	}
	if isGreaseType(t) {
		return "GREASE(0x" + strconv.FormatUint(t, 16) + ")"
	}
	return "UNKNOWN(0x" + strconv.FormatUint(t, 16) + ")"
}

// isGreaseType reports whether t is one of the reserved 0x1f*N+0x21 values of
// RFC 9114 §7.2.8, which exist only to be ignored.
//
// The subtraction is guarded because t is a full varint from the wire: t < 0x21
// would wrap, and every value below the first GREASE point would be reported as
// one.
func isGreaseType(t uint64) bool {
	return t >= 0x21 && (t-0x21)%0x1f == 0
}

// SettingName returns the name of an HTTP/3 SETTINGS identifier (RFC 9114
// §7.2.4.1, RFC 9204 §5), or "UNKNOWN(0x…)".
func SettingName(id uint64) string {
	switch id {
	case SettingQPACKMaxTableCapacity:
		return "SETTINGS_QPACK_MAX_TABLE_CAPACITY"
	case SettingMaxFieldSectionSize:
		return "SETTINGS_MAX_FIELD_SECTION_SIZE"
	case SettingQPACKBlockedStreams:
		return "SETTINGS_QPACK_BLOCKED_STREAMS"
	}
	if isGreaseType(id) {
		return "GREASE(0x" + strconv.FormatUint(id, 16) + ")"
	}
	return "UNKNOWN(0x" + strconv.FormatUint(id, 16) + ")"
}

// ErrorCodeName returns the RFC 9114 §8.1 name of an HTTP/3 error code, or
// "UNKNOWN(0x…)". It is the exported form of the naming H3ConnError.Error has
// used since that type was introduced.
func ErrorCodeName(code uint64) string {
	if name := h3ErrorCodeName(code); name != "" {
		return name
	}
	return "UNKNOWN(0x" + strconv.FormatUint(code, 16) + ")"
}
