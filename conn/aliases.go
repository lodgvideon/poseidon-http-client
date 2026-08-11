package conn

import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
)

// Re-exports of low-level types so the client package can avoid importing
// frame and hpack directly, keeping conn as the single dependency surface.

type (
	// ErrCode is an HTTP/2 error code (RFC 7540 §7).
	ErrCode = frame.ErrCode

	// HeaderField is a decoded HPACK name/value pair.
	HeaderField = header.Field

	// IndexingMode selects a header field's HPACK literal representation, and
	// therefore whether it enters the dynamic table.
	IndexingMode = header.IndexingMode
)

const (
	// ErrCodeRefusedStream is the HTTP/2 error code indicating the peer
	// refused to accept the stream (used in GOAWAY / RST_STREAM context).
	ErrCodeRefusedStream = frame.ErrCodeRefusedStream

	// IndexIncremental indexes the field into the dynamic table (the default).
	IndexIncremental = header.IndexIncremental

	// IndexWithout keeps a per-request-varying field out of the dynamic table
	// without claiming any security meaning.
	IndexWithout = header.IndexWithout

	// IndexNever marks a field never-indexed, which intermediaries must honour.
	IndexNever = header.IndexNever
)
