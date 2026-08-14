package frame

import "github.com/lodgvideon/poseidon-http-client/trace"

// Naming for the wire vocabulary of RFC 9113.
//
// Every string returned here is a compile-time constant. That is a requirement,
// not an accident: these are called once per frame from the tracer emit sites
// in framer.go, on paths the bench-gate holds at zero allocations. Anything
// that joined names with strings.Builder would allocate per frame and fail the
// gate, which is why FlagNames is a table of pre-joined constants rather than
// the obvious loop over set bits.

// String returns the RFC 9113 §11.2 name of the frame type, or trace.UnknownName
// for a type this implementation does not define. §5.5 requires an endpoint to
// ignore unknown types, so meeting one is ordinary and the numeric Type is the
// part worth printing.
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
	default:
		return trace.UnknownName
	}
}

// FlagNames renders the flags defined for this frame type, joined by "|", or
// the empty string when none of them is set.
//
// It hangs off FrameType rather than off Flags because a flag bit has no
// meaning on its own: 0x1 is END_STREAM on DATA and HEADERS, and ACK on
// SETTINGS and PING. A Flags.String() would have to pick one of those and be
// wrong about the other half of the connection. Bits the type does not define
// are ignored, as §4.1 requires of a receiver.
func (t FrameType) FlagNames(f Flags) string {
	//exhaustive:ignore // Types with no defined flags — PRIORITY, RST_STREAM,
	// GOAWAY, WINDOW_UPDATE, ALTSVC, ORIGIN — take the default arm, and so does
	// every unknown type.
	switch t {
	case FrameData:
		return dataFlagNames(f)
	case FrameHeaders:
		return headersFlagNames(f)
	case FrameSettings, FramePing:
		if f&FlagSettingsAck != 0 {
			return "ACK"
		}
		return ""
	case FramePushPromise:
		return pushPromiseFlagNames(f)
	case FrameContinuation:
		if f&FlagContinuationEndHeaders != 0 {
			return "END_HEADERS"
		}
		return ""
	default:
		return ""
	}
}

func dataFlagNames(f Flags) string {
	switch f & (FlagDataEndStream | FlagDataPadded) {
	case FlagDataEndStream:
		return "END_STREAM"
	case FlagDataPadded:
		return "PADDED"
	case FlagDataEndStream | FlagDataPadded:
		return "END_STREAM|PADDED"
	default:
		return ""
	}
}

func pushPromiseFlagNames(f Flags) string {
	switch f & (FlagPushPromiseEndHeaders | FlagPushPromisePadded) {
	case FlagPushPromiseEndHeaders:
		return "END_HEADERS"
	case FlagPushPromisePadded:
		return "PADDED"
	case FlagPushPromiseEndHeaders | FlagPushPromisePadded:
		return "END_HEADERS|PADDED"
	default:
		return ""
	}
}

// headersFlagNames enumerates all sixteen combinations of the four flags
// HEADERS defines. Written out rather than assembled so that the result is a
// constant string; TestFrameType_FlagNames_MatchBits rebuilds every arm from
// the bit names and fails on a typo.
func headersFlagNames(f Flags) string {
	const mask = FlagHeadersEndStream | FlagHeadersEndHeaders | FlagHeadersPadded | FlagHeadersPriority
	switch f & mask {
	case 0x00:
		return ""
	case 0x01:
		return "END_STREAM"
	case 0x04:
		return "END_HEADERS"
	case 0x05:
		return "END_STREAM|END_HEADERS"
	case 0x08:
		return "PADDED"
	case 0x09:
		return "END_STREAM|PADDED"
	case 0x0c:
		return "END_HEADERS|PADDED"
	case 0x0d:
		return "END_STREAM|END_HEADERS|PADDED"
	case 0x20:
		return "PRIORITY"
	case 0x21:
		return "END_STREAM|PRIORITY"
	case 0x24:
		return "END_HEADERS|PRIORITY"
	case 0x25:
		return "END_STREAM|END_HEADERS|PRIORITY"
	case 0x28:
		return "PADDED|PRIORITY"
	case 0x29:
		return "END_STREAM|PADDED|PRIORITY"
	case 0x2c:
		return "END_HEADERS|PADDED|PRIORITY"
	default: // 0x2d — all four
		return "END_STREAM|END_HEADERS|PADDED|PRIORITY"
	}
}

// String returns the RFC 9113 §7 name of the error code, or trace.UnknownName
// for a code outside the registry. §7 requires unknown codes to be treated as
// INTERNAL_ERROR rather than rejected, so they reach here in normal operation.
func (e ErrCode) String() string {
	switch e {
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
	default:
		return trace.UnknownName
	}
}

// String returns the RFC 9113 §6.5.2 name of the settings identifier, or
// trace.UnknownName for one outside the registry. §6.5.2 requires a receiver to
// ignore identifiers it does not understand, so unknown ones are ordinary.
func (s SettingID) String() string {
	switch s {
	case SettingHeaderTableSize:
		return "HEADER_TABLE_SIZE"
	case SettingEnablePush:
		return "ENABLE_PUSH"
	case SettingMaxConcurrentStreams:
		return "MAX_CONCURRENT_STREAMS"
	case SettingInitialWindowSize:
		return "INITIAL_WINDOW_SIZE"
	case SettingMaxFrameSize:
		return "MAX_FRAME_SIZE"
	case SettingMaxHeaderListSize:
		return "MAX_HEADER_LIST_SIZE"
	case SettingEnableConnectProtocol:
		return "ENABLE_CONNECT_PROTOCOL"
	default:
		return trace.UnknownName
	}
}
