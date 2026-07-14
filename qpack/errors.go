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
// of range, or a dynamic-table reference that cannot be resolved (an entry not
// present, an out-of-range index, or a Required Insert Count the table has not
// reached). It corresponds to the QPACK_DECOMPRESSION_FAILED (0x0200)
// connection error.
var ErrDecompressionFailed = errors.New("qpack: decompression failed")

// ErrEncoderStream is returned when applying an encoder-stream instruction
// (RFC 9204 §4.3) fails: a malformed instruction, an unresolvable name
// reference, a Set Dynamic Table Capacity above the advertised maximum, or an
// insertion too large for the table. It corresponds to the
// QPACK_ENCODER_STREAM_ERROR (0x0201) connection error.
var ErrEncoderStream = errors.New("qpack: encoder stream error")

// ErrDecoderStream is returned when applying a decoder-stream instruction
// (RFC 9204 §4.4) the peer's decoder sent fails: a malformed instruction, an
// Insert Count Increment of zero, or an acknowledgment that would advance the
// encoder's Known Received Count past the entries it has actually inserted. It
// corresponds to the QPACK_DECODER_STREAM_ERROR (0x0202) connection error.
var ErrDecoderStream = errors.New("qpack: decoder stream error")
