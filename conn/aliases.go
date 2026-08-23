package conn

import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
)

// Re-exports of low-level types so the client package can avoid importing
// frame and hpack directly, keeping conn as the single dependency surface.
//
// That was the stated intent from the start and it had drifted: `client` named
// frame.Priority in four places including the public Request.Priority field, and
// `grpc` named frame.ErrCode and four of its constants — so an HTTP/1.1 or
// HTTP/3 caller had to import the HTTP/2 frame codec to spell a parameter type.
// The bindings below close both, and ci.yml keeps the edge from coming back
// (#714). A Go type alias is an identical type, so nothing here forces a caller
// to migrate.

type (
	// ErrCode is an HTTP/2 error code (RFC 7540 §7).
	ErrCode = frame.ErrCode

	// Priority describes a PRIORITY field block (RFC 7540 §6.3). It is an alias
	// so a caller can name a stream's priority — including through
	// client.Request.Priority, which HTTP/1.1 and HTTP/3 accept and ignore —
	// without importing the HTTP/2 frame codec.
	Priority = frame.Priority

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

	// ErrCodeInternalError is the HTTP/2 error code for an unexpected internal
	// condition (RFC 7540 §7).
	ErrCodeInternalError = frame.ErrCodeInternalError

	// ErrCodeCancel indicates the stream is no longer needed (RFC 7540 §7).
	ErrCodeCancel = frame.ErrCodeCancel

	// ErrCodeEnhanceYourCalm indicates the peer is generating excessive load
	// (RFC 7540 §7).
	ErrCodeEnhanceYourCalm = frame.ErrCodeEnhanceYourCalm

	// ErrCodeInadequateSecurity indicates the transport does not meet the
	// peer's security requirements (RFC 7540 §7).
	ErrCodeInadequateSecurity = frame.ErrCodeInadequateSecurity

	// IndexIncremental indexes the field into the dynamic table (the default).
	IndexIncremental = header.IndexIncremental

	// IndexWithout keeps a per-request-varying field out of the dynamic table
	// without claiming any security meaning.
	IndexWithout = header.IndexWithout

	// IndexNever marks a field never-indexed, which intermediaries must honour.
	IndexNever = header.IndexNever
)
