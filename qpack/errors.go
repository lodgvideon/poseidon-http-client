package qpack

import "errors"

// QPACK error codes (RFC 9204 §6). These are HTTP/3 connection-error codes,
// signalled via a QUIC CONNECTION_CLOSE frame when field-section coding fails.
const (
	// ErrCodeDecompressionFailed is QPACK_DECOMPRESSION_FAILED.
	ErrCodeDecompressionFailed uint64 = 0x0200
	// ErrCodeEncoderStreamError is QPACK_ENCODER_STREAM_ERROR.
	ErrCodeEncoderStreamError uint64 = 0x0201
	// ErrCodeDecoderStreamError is QPACK_DECODER_STREAM_ERROR.
	ErrCodeDecoderStreamError uint64 = 0x0202
)

// ErrDecompressionFailed is returned by the Decoder when a field section cannot
// be decoded: a malformed or truncated representation, a static-table index out
// of range, or a reference to the dynamic table (which this static-only profile
// does not support — the client advertises SETTINGS_QPACK_MAX_TABLE_CAPACITY=0,
// so a conformant peer never emits one). It corresponds to the
// QPACK_DECOMPRESSION_FAILED (0x0200) connection error.
var ErrDecompressionFailed = errors.New("qpack: decompression failed")
