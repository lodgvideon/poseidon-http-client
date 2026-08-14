package frame

import "strconv"

// Names for the wire vocabulary of RFC 7540 §11 — frame types, flag bits, error
// codes and SETTINGS identifiers. Nothing in this package needed them to move
// bytes, which is why they arrived late: every one of these types was a bare
// integer, so an error message said `code=8` and a frame log was not writable at
// all (#610).
//
// The names are the RFC's own registry entries, not prettified spellings, so a
// log line here can be pasted next to a capture from any other implementation
// and match. Unknown values render as `NAME(0x…)` rather than being swallowed:
// an extension frame type or a GREASE setting is a thing you want to SEE in a
// trace, and "unknown" with no number is the one rendering that helps nobody.
//
// None of this is hot-path code. Every function here allocates a string and is
// reached only from an error path or a tracer.

// unknownName renders an unrecognised wire value as `prefix(0xNN)`.
func unknownName(prefix string, v uint64) string {
	b := make([]byte, 0, len(prefix)+8)
	b = append(b, prefix...)
	b = append(b, "(0x"...)
	b = strconv.AppendUint(b, v, 16)
	return string(append(b, ')'))
}

// String returns the registered name of the frame type — "HEADERS",
// "WINDOW_UPDATE" — or "UNKNOWN(0x…)" for a type this implementation does not
// name. The two extension types this codec speaks are named too: ALTSVC
// (RFC 7838 §4) and ORIGIN (RFC 8336 §3).
//
// A type outside the registry is not an error here. RFC 7540 §5.5 obliges a
// receiver to ignore frames it does not understand, so ReadFrame drops them —
// and a trace of a connection that silently dropped nine frames is exactly when
// you want the numbers.
func (t FrameType) String() string {
	switch t {
	case FrameData:
		return "DATA"
	case FrameHeaders:
		return "HEADERS"
	case FramePriority:
		return "PRIORITY"
	case FrameRSTStream:
		return "RST_STREAM"
	case FrameSettings:
		return "SETTINGS"
	case FramePushPromise:
		return "PUSH_PROMISE"
	case FramePing:
		return "PING"
	case FrameGoAway:
		return "GOAWAY"
	case FrameWindowUpdate:
		return "WINDOW_UPDATE"
	case FrameContinuation:
		return "CONTINUATION"
	case FrameAltSvc:
		return "ALTSVC"
	case FrameOrigin:
		return "ORIGIN"
	}
	return unknownName("UNKNOWN", uint64(t))
}

// StringFor renders the bits set in f using the names they carry on a frame of
// type t: "END_HEADERS|END_STREAM", or "NONE" when no bit is set. Bits with no
// name on that frame type are appended as "0x…" so nothing is lost.
//
// The frame type is a parameter because a flag byte does not mean anything
// without it. 0x1 is END_STREAM on DATA and HEADERS and ACK on SETTINGS and
// PING; 0x4 is END_HEADERS on HEADERS, PUSH_PROMISE and CONTINUATION and
// undefined everywhere else. RFC 7540 §4.1 says as much — "Flags are assigned
// semantics specific to the indicated frame type" — and a renderer that ignores
// it prints "END_STREAM" for a SETTINGS ACK, which reads as a protocol
// violation that never happened.
func (f Flags) StringFor(t FrameType) string {
	var scratch [64]byte
	return string(f.AppendFor(scratch[:0], t))
}

// AppendFor appends what StringFor would return to b and returns the extended
// slice, in the shape of time.Time.AppendFormat and strconv.AppendInt.
//
// It exists because the one caller that runs per frame is a tracer, and a
// tracer that allocates a string for every flag byte it renders adds GC
// pressure to the load test it was switched on to explain. Given a b with
// capacity — a reused line buffer — this allocates nothing.
func (f Flags) AppendFor(b []byte, t FrameType) []byte {
	if f == 0 {
		return append(b, "NONE"...)
	}
	start := len(b)
	rest := f
	for _, fn := range flagNamesFor(t) {
		if rest&fn.bit == 0 {
			continue
		}
		rest &^= fn.bit
		if len(b) > start {
			b = append(b, '|')
		}
		b = append(b, fn.name...)
	}
	if rest != 0 {
		if len(b) > start {
			b = append(b, '|')
		}
		b = append(b, "0x"...)
		b = strconv.AppendUint(b, uint64(rest), 16)
	}
	return b
}

// flagName pairs a flag bit with the name it carries on one frame type.
type flagName struct {
	bit  Flags
	name string
}

// The per-type flag vocabularies of RFC 7540 §6. Package-level so that ranging
// over one costs nothing — a closure over the output buffer, which is the
// obvious way to write AppendFor, forces that buffer to the heap and hands the
// allocation straight back.
var (
	dataFlagNames        = []flagName{{FlagDataEndStream, "END_STREAM"}, {FlagDataPadded, "PADDED"}}
	headersFlagNames     = []flagName{{FlagHeadersEndStream, "END_STREAM"}, {FlagHeadersEndHeaders, "END_HEADERS"}, {FlagHeadersPadded, "PADDED"}, {FlagHeadersPriority, "PRIORITY"}}
	ackFlagNames         = []flagName{{FlagSettingsAck, "ACK"}}
	pushPromiseFlagNames = []flagName{{FlagPushPromiseEndHeaders, "END_HEADERS"}, {FlagPushPromisePadded, "PADDED"}}
	contFlagNames        = []flagName{{FlagContinuationEndHeaders, "END_HEADERS"}}
)

// flagNamesFor returns the flag vocabulary of frame type t, or nil for the
// types that define no flags at all — PRIORITY, RST_STREAM, GOAWAY,
// WINDOW_UPDATE and the two extension types. A bit set on one of those is
// undefined by the RFC, and AppendFor renders it numerically.
func flagNamesFor(t FrameType) []flagName {
	//exhaustive:ignore // The unlisted types define no flags; nil is the answer.
	switch t {
	case FrameData:
		return dataFlagNames
	case FrameHeaders:
		return headersFlagNames
	case FrameSettings, FramePing:
		return ackFlagNames
	case FramePushPromise:
		return pushPromiseFlagNames
	case FrameContinuation:
		return contFlagNames
	}
	return nil
}

// String renders the bits set in f using the names they carry on a HEADERS
// frame, which is the only frame type that defines all four flags this codec
// knows: END_STREAM (0x1), END_HEADERS (0x4), PADDED (0x8), PRIORITY (0x20).
//
// It is the lossy one of the pair, and deliberately so: a bare Flags value does
// not carry the frame type, so 0x1 here always reads END_STREAM even when the
// frame was a SETTINGS ACK. Every caller inside this package knows the type and
// uses StringFor; this exists so that a Flags in a %v somewhere does not print
// as a naked integer.
func (f Flags) String() string { return f.StringFor(FrameHeaders) }

// String renders a frame header the way a wire log reads it:
//
//	HEADERS stream=3 len=54 flags=END_HEADERS|END_STREAM
//
// The flags clause is omitted entirely when no bit is set, rather than printed
// as "flags=NONE" — a log with one such clause per DATA frame is mostly that
// clause.
func (h FrameHeader) String() string {
	b := make([]byte, 0, 64)
	b = append(b, h.Type.String()...)
	b = append(b, " stream="...)
	b = strconv.AppendUint(b, uint64(h.StreamID), 10)
	b = append(b, " len="...)
	b = strconv.AppendUint(b, uint64(h.Length), 10)
	if h.Flags != 0 {
		b = append(b, " flags="...)
		b = append(b, h.Flags.StringFor(h.Type)...)
	}
	return string(b)
}

// String returns the RFC 7540 §7 name of the error code — "PROTOCOL_ERROR",
// "ENHANCE_YOUR_CALM" — or "UNKNOWN_ERROR(0x…)" for a code outside the
// registry.
//
// §7 tells a receiver to treat an unknown code as INTERNAL_ERROR, and this
// deliberately does NOT do that: the substitution is a handling rule, and
// applying it to the rendering would erase the one piece of evidence about what
// the peer actually said.
func (c ErrCode) String() string {
	switch c {
	case ErrCodeNoError:
		return "NO_ERROR"
	case ErrCodeProtocolError:
		return "PROTOCOL_ERROR"
	case ErrCodeInternalError:
		return "INTERNAL_ERROR"
	case ErrCodeFlowControlError:
		return "FLOW_CONTROL_ERROR"
	case ErrCodeSettingsTimeout:
		return "SETTINGS_TIMEOUT"
	case ErrCodeStreamClosed:
		return "STREAM_CLOSED"
	case ErrCodeFrameSizeError:
		return "FRAME_SIZE_ERROR"
	case ErrCodeRefusedStream:
		return "REFUSED_STREAM"
	case ErrCodeCancel:
		return "CANCEL"
	case ErrCodeCompressionError:
		return "COMPRESSION_ERROR"
	case ErrCodeConnectError:
		return "CONNECT_ERROR"
	case ErrCodeEnhanceYourCalm:
		return "ENHANCE_YOUR_CALM"
	case ErrCodeInadequateSecurity:
		return "INADEQUATE_SECURITY"
	case ErrCodeHTTP11Required:
		return "HTTP_1_1_REQUIRED"
	}
	return unknownName("UNKNOWN_ERROR", uint64(c))
}

// String returns the registered name of the SETTINGS parameter —
// "SETTINGS_INITIAL_WINDOW_SIZE" — or "UNKNOWN_SETTING(0x…)".
//
// Unknown identifiers reach here routinely and legitimately: dispatchSettings
// drops them per RFC 7540 §6.5.2, and a peer sending GREASE reserved settings
// (RFC 8701) sends them on every connection. Seeing which ones is the point.
func (s SettingID) String() string {
	switch s {
	case SettingHeaderTableSize:
		return "SETTINGS_HEADER_TABLE_SIZE"
	case SettingEnablePush:
		return "SETTINGS_ENABLE_PUSH"
	case SettingMaxConcurrentStreams:
		return "SETTINGS_MAX_CONCURRENT_STREAMS"
	case SettingInitialWindowSize:
		return "SETTINGS_INITIAL_WINDOW_SIZE"
	case SettingMaxFrameSize:
		return "SETTINGS_MAX_FRAME_SIZE"
	case SettingMaxHeaderListSize:
		return "SETTINGS_MAX_HEADER_LIST_SIZE"
	case SettingEnableConnectProtocol:
		return "SETTINGS_ENABLE_CONNECT_PROTOCOL"
	}
	return unknownName("UNKNOWN_SETTING", uint64(s))
}
