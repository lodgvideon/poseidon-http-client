package quic

import "errors"

// Transport error codes (RFC 9000 §20.1), carried in a CONNECTION_CLOSE frame
// of type 0x1c.
const (
	ErrCodeNoError                 uint64 = 0x00 // NO_ERROR
	ErrCodeInternalError           uint64 = 0x01 // INTERNAL_ERROR
	ErrCodeConnectionRefused       uint64 = 0x02 // CONNECTION_REFUSED
	ErrCodeFlowControlError        uint64 = 0x03 // FLOW_CONTROL_ERROR
	ErrCodeStreamLimitError        uint64 = 0x04 // STREAM_LIMIT_ERROR
	ErrCodeStreamStateError        uint64 = 0x05 // STREAM_STATE_ERROR
	ErrCodeFinalSizeError          uint64 = 0x06 // FINAL_SIZE_ERROR
	ErrCodeFrameEncodingError      uint64 = 0x07 // FRAME_ENCODING_ERROR
	ErrCodeTransportParameterError uint64 = 0x08 // TRANSPORT_PARAMETER_ERROR
	ErrCodeConnectionIDLimitError  uint64 = 0x09 // CONNECTION_ID_LIMIT_ERROR
	ErrCodeProtocolViolation       uint64 = 0x0a // PROTOCOL_VIOLATION
	ErrCodeInvalidToken            uint64 = 0x0b // INVALID_TOKEN
	ErrCodeApplicationError        uint64 = 0x0c // APPLICATION_ERROR
	ErrCodeCryptoBufferExceeded    uint64 = 0x0d // CRYPTO_BUFFER_EXCEEDED
	ErrCodeKeyUpdateError          uint64 = 0x0e // KEY_UPDATE_ERROR
	ErrCodeAEADLimitReached        uint64 = 0x0f // AEAD_LIMIT_REACHED
	ErrCodeNoViablePath            uint64 = 0x10 // NO_VIABLE_PATH
	// CRYPTO_ERROR is the range 0x0100-0x01ff (0x0100 + the TLS alert).
	ErrCodeCryptoBase uint64 = 0x0100
)

// ErrFrameEncoding is returned by the frame parser when the input is malformed:
// a truncated field, a varint that runs past the buffer, a length that exceeds
// the remaining bytes, or an unknown frame type. It maps to the
// FRAME_ENCODING_ERROR (0x07) transport error (RFC 9000 §12.4).
var ErrFrameEncoding = errors.New("quic: frame encoding error")

// ErrPacketEncoding is returned by the packet-header parser when the input is
// too short or malformed: a truncated header, a connection ID length above 20,
// or a Length field that runs past the datagram (RFC 9000 §17).
var ErrPacketEncoding = errors.New("quic: packet encoding error")
