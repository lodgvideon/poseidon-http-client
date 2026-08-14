package http3

import "github.com/lodgvideon/poseidon-http-client/trace"

// FrameTypeName returns the RFC 9114 §11.2.1 name of an HTTP/3 frame type, or
// trace.UnknownName for one this implementation does not define.
//
// It is a function rather than a String method because the frame-type constants
// above are bare uint64: they are compared against varints read straight off
// the wire, and giving them a named type would change the signature of every
// parser that handles one.
//
// Unknown types are ordinary here in a way they are not in HTTP/2: §9 reserves
// the grease pattern 0x1f * N + 0x21, and §7.2.8 requires a receiver to ignore
// any type it does not recognise on a request stream. The reserved
// HTTP/2-carryover types (0x02, 0x06, 0x08, 0x09) are named even though this
// implementation rejects them, because seeing which one a peer sent is the
// whole point of reading a frame log after an H3_FRAME_UNEXPECTED.
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
	default:
		return trace.UnknownName
	}
}

// SettingName returns the name of an HTTP/3 SETTINGS identifier (RFC 9114
// §7.2.4.1, RFC 9204 §5), or trace.UnknownName for one outside those
// registries. §7.2.4.1 requires unknown identifiers to be ignored, so meeting
// one is normal.
func SettingName(id uint64) string {
	switch id {
	case SettingQPACKMaxTableCapacity:
		return "QPACK_MAX_TABLE_CAPACITY"
	case SettingMaxFieldSectionSize:
		return "MAX_FIELD_SECTION_SIZE"
	case SettingQPACKBlockedStreams:
		return "QPACK_BLOCKED_STREAMS"
	default:
		return trace.UnknownName
	}
}
