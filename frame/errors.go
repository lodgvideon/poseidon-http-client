package frame

import "errors"

// ErrCode mirrors RFC 7540 §7.
type ErrCode uint32

const (
	ErrCodeNoError            ErrCode = 0x0
	ErrCodeProtocolError      ErrCode = 0x1
	ErrCodeInternalError      ErrCode = 0x2
	ErrCodeFlowControlError   ErrCode = 0x3
	ErrCodeSettingsTimeout    ErrCode = 0x4
	ErrCodeStreamClosed       ErrCode = 0x5
	ErrCodeFrameSizeError     ErrCode = 0x6
	ErrCodeRefusedStream      ErrCode = 0x7
	ErrCodeCancel             ErrCode = 0x8
	ErrCodeCompressionError   ErrCode = 0x9
	ErrCodeConnectError       ErrCode = 0xA
	ErrCodeEnhanceYourCalm    ErrCode = 0xB
	ErrCodeInadequateSecurity ErrCode = 0xC
	ErrCodeHTTP11Required     ErrCode = 0xD
)

// Sentinel errors. Hot-path code MUST NOT use fmt.Errorf — only these.
var (
	ErrFrameTooLarge       = errors.New("poseidon/frame: frame exceeds SETTINGS_MAX_FRAME_SIZE")
	ErrInvalidStreamID     = errors.New("poseidon/frame: stream id violates RFC 7540 rules")
	ErrInvalidPadding      = errors.New("poseidon/frame: pad length exceeds payload")
	ErrUnknownFrameType    = errors.New("poseidon/frame: unknown frame type")
	ErrSettingsAck         = errors.New("poseidon/frame: SETTINGS ACK with non-empty payload")
	ErrPriorityWrongLength = errors.New("poseidon/frame: PRIORITY frame length != 5")
	ErrRSTWrongLength      = errors.New("poseidon/frame: RST_STREAM frame length != 4")
	ErrPingWrongLength     = errors.New("poseidon/frame: PING frame length != 8")
	ErrWindowWrongLength   = errors.New("poseidon/frame: WINDOW_UPDATE frame length != 4")
	ErrSettingsLength      = errors.New("poseidon/frame: SETTINGS frame length not multiple of 6")
	ErrShortRead           = errors.New("poseidon/frame: short read on header or payload")
	ErrZeroIncrement       = errors.New("poseidon/frame: WINDOW_UPDATE with zero increment")
	ErrProtocolError       = errors.New("poseidon/frame: protocol error")
	ErrTooManyAltSvc       = errors.New("poseidon/frame: ALTSVC frame carries more than one origin")
	// ErrContinuationExpected is returned when a field block is open (a HEADERS or
	// PUSH_PROMISE without END_HEADERS) and the next frame is not a CONTINUATION on
	// the same stream (RFC 9113 §6.10 — a connection error of type PROTOCOL_ERROR).
	ErrContinuationExpected = errors.New("poseidon/frame: expected CONTINUATION on the open field block")
	// ErrUnexpectedContinuation is returned when a CONTINUATION frame arrives with
	// no field block open (RFC 9113 §6.10 — a connection error of type PROTOCOL_ERROR).
	ErrUnexpectedContinuation = errors.New("poseidon/frame: CONTINUATION with no open field block")
)
