package conn

import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Re-exports of low-level types so the client package can avoid importing
// frame and hpack directly, keeping conn as the single dependency surface.

type (
	// ErrCode is an HTTP/2 error code (RFC 7540 §7).
	ErrCode = frame.ErrCode

	// HeaderField is a decoded HPACK name/value pair.
	HeaderField = hpack.HeaderField
)

const (
	// ErrCodeRefusedStream is the HTTP/2 error code indicating the peer
	// refused to accept the stream (used in GOAWAY / RST_STREAM context).
	ErrCodeRefusedStream = frame.ErrCodeRefusedStream
)
