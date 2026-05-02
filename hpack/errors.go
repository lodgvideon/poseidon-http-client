package hpack

import "errors"

// Sentinel errors. Hot-path code MUST NOT use fmt.Errorf — only these.
var (
	// ErrTruncated is returned when input ends mid-field (RFC 7541 §5).
	ErrTruncated = errors.New("poseidon/hpack: truncated input")
	// ErrIntegerOverflow is returned when an N-bit prefix integer exceeds 2^32-1.
	ErrIntegerOverflow = errors.New("poseidon/hpack: integer overflow")
	// ErrInvalidIndex is returned when an index references neither static nor dynamic table.
	ErrInvalidIndex = errors.New("poseidon/hpack: invalid table index")
	// ErrInvalidHuffman is returned when Huffman-coded input is malformed.
	ErrInvalidHuffman = errors.New("poseidon/hpack: invalid Huffman code")
	// ErrTableSizeUpdate is returned when a "Dynamic Table Size Update" exceeds the SETTINGS limit.
	ErrTableSizeUpdate = errors.New("poseidon/hpack: dynamic table size update exceeds limit")
	// ErrHeaderListTooLarge is returned when an incoming header list exceeds SETTINGS_MAX_HEADER_LIST_SIZE.
	ErrHeaderListTooLarge = errors.New("poseidon/hpack: header list exceeds max size")
	// ErrInvalidPrefix is returned when a representation prefix byte is malformed.
	ErrInvalidPrefix = errors.New("poseidon/hpack: invalid representation prefix")
)
