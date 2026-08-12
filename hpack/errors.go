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
	// ErrNotStreaming is returned by Feed or Finish when no streaming decode is
	// open — Begin was not called, or Finish already closed it.
	//
	// It exists because both used to report this through ErrInvalidPrefix, a
	// wire-format sentinel meaning the peer sent a malformed representation byte.
	// The two say opposite things about who is at fault: one is the caller's
	// sequencing mistake, entirely local, and the other is a peer's bytes and a
	// connection error under RFC 7541 §5. Anyone mapping sentinels to RFC
	// sections — the way conn's dispatch does — was told a local bug came off
	// the wire.
	ErrNotStreaming = errors.New("poseidon/hpack: no streaming decode in progress")
)
