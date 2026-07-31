package grpc

import (
	"strconv"
	"strings"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Code is a gRPC status code, as carried in the grpc-status trailer.
type Code uint32

// The canonical gRPC status codes.
const (
	// OK means the RPC completed successfully.
	OK Code = 0
	// Canceled means the operation was cancelled, typically by the caller.
	Canceled Code = 1
	// Unknown covers errors that carry no more specific code.
	Unknown Code = 2
	// InvalidArgument means the client supplied a malformed argument.
	InvalidArgument Code = 3
	// DeadlineExceeded means the deadline expired before the RPC completed.
	DeadlineExceeded Code = 4
	// NotFound means a requested entity was not found.
	NotFound Code = 5
	// AlreadyExists means the entity a client tried to create already exists.
	AlreadyExists Code = 6
	// PermissionDenied means the caller is not authorized for the operation.
	PermissionDenied Code = 7
	// ResourceExhausted means a quota or per-connection resource is exhausted.
	ResourceExhausted Code = 8
	// FailedPrecondition means the system is not in the state the operation needs.
	FailedPrecondition Code = 9
	// Aborted means the operation was aborted, typically by a concurrency conflict.
	Aborted Code = 10
	// OutOfRange means the operation was attempted past the valid range.
	OutOfRange Code = 11
	// Unimplemented means the method is not supported by the server.
	Unimplemented Code = 12
	// Internal means an invariant the system relies on was broken.
	Internal Code = 13
	// Unavailable means the service is currently unavailable; retry may succeed.
	Unavailable Code = 14
	// DataLoss means unrecoverable data loss or corruption.
	DataLoss Code = 15
	// Unauthenticated means the request lacks valid credentials.
	Unauthenticated Code = 16
)

// codeNames indexes the canonical names by code value.
var codeNames = [...]string{
	"OK", "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED",
	"NOT_FOUND", "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED",
	"FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED",
	"INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED",
}

// String returns the canonical name of the code, or CODE(n) when n is outside
// the range the gRPC specification defines.
func (c Code) String() string {
	if int(c) < len(codeNames) {
		return codeNames[c]
	}
	return "CODE(" + strconv.FormatUint(uint64(c), 10) + ")"
}

// Status is the terminal outcome of an RPC: the grpc-status code plus the
// decoded grpc-message. A Status with code OK is the success case and is not
// treated as an error by Err.
type Status struct {
	// Code is the grpc-status value.
	Code Code
	// Message is the grpc-message value, percent-decoded.
	Message string
}

// Error implements the error interface so a non-OK Status can be returned
// directly from Recv.
func (s *Status) Error() string {
	if s.Message == "" {
		return "grpc: " + s.Code.String()
	}
	return "grpc: " + s.Code.String() + ": " + s.Message
}

// Err returns s as an error, or nil when the code is OK. The returned error is
// a *Status, so callers can recover the code with errors.As.
func (s Status) Err() error {
	if s.Code == OK {
		return nil
	}
	cp := s
	return &cp
}

// statusFromHTTP maps an HTTP response status to a gRPC code, per the
// "HTTP-Status to gRPC-Status" table in the gRPC over HTTP/2 specification.
// It is used only when the server ends the stream without a grpc-status.
func statusFromHTTP(httpStatus int) Code {
	switch httpStatus {
	case 400:
		return Internal
	case 401:
		return Unauthenticated
	case 403:
		return PermissionDenied
	case 404:
		return Unimplemented
	case 429, 502, 503, 504:
		return Unavailable
	default:
		return Unknown
	}
}

// statusFromRST maps an HTTP/2 RST_STREAM error code to a gRPC code. A reset
// stream never carries trailers, so this is the only status the caller gets.
func statusFromRST(code frame.ErrCode) Code {
	switch code {
	case frame.ErrCodeRefusedStream:
		return Unavailable
	case frame.ErrCodeCancel:
		return Canceled
	case frame.ErrCodeEnhanceYourCalm:
		return ResourceExhausted
	case frame.ErrCodeInadequateSecurity:
		return PermissionDenied
	default:
		return Internal
	}
}

// parseStatusCode parses a grpc-status field value. A value that is not a
// decimal number is treated as Unknown, which is what the specification
// requires of a client facing a malformed trailer.
func parseStatusCode(v string) Code {
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return Unknown
	}
	return Code(n)
}

// decodeMessage percent-decodes a grpc-message field value. Per the
// specification the encoding is deliberately lenient on the read side: a
// stray or truncated escape is passed through verbatim rather than failing
// the RPC, because the message is diagnostic text, not protocol state.
func decodeMessage(v string) string {
	if !strings.ContainsRune(v, '%') {
		return v
	}
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); {
		if v[i] == '%' && i+2 < len(v) {
			hi, ok1 := unhex(v[i+1])
			lo, ok2 := unhex(v[i+2])
			if ok1 && ok2 {
				b.WriteByte(hi<<4 | lo)
				i += 3
				continue
			}
		}
		b.WriteByte(v[i])
		i++
	}
	return b.String()
}

// unhex converts one hexadecimal digit to its value.
func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
