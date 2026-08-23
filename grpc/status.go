package grpc

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/lodgvideon/poseidon-http-client/conn"
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
	// Compared in Code's own (uint32) domain, not via int: on a 32-bit target
	// int(c) for a peer-supplied 4294967295 is -1, which passes a signed guard
	// and then indexes the array out of range.
	if c < Code(len(codeNames)) {
		return codeNames[c]
	}
	return "CODE(" + strconv.FormatUint(uint64(c), 10) + ")"
}

// Status is the terminal outcome of an RPC: the grpc-status code plus the
// decoded grpc-message. A Status with code OK is the success case and is not
// treated as an error by Err.
//
// Error takes a pointer receiver and Err a value receiver deliberately. Error
// on the pointer keeps a Status value from satisfying error, so the only way
// to turn one into an error is Err — the single place that returns a nil
// error for code OK. Err takes a value so it can be called on one (a Status
// returned from a function is not addressable) and copies before taking the
// address, so the returned error never aliases the caller's struct. Unifying
// the receivers either way gives up one of those three properties.
//
//nolint:recvcheck // the split receiver is the design; see above
type Status struct {
	// Code is the grpc-status value.
	Code Code
	// Message is the grpc-message value, percent-decoded.
	Message string

	// cause is the transport error a connection-level failure was mapped from.
	// It is unexported because a Status built from the wire has none: the peer
	// sends a code and a message, not a Go error. Set only by
	// statusFromTransport, and reachable through Unwrap so a caller that still
	// wants to ask errors.Is(err, conn.ErrConnClosed) can.
	cause error
}

// Unwrap returns the transport error this Status was mapped from, or nil for a
// Status the peer sent. It is what keeps the mapping additive: a connection
// death now answers errors.As(&Status) AND the errors.Is check a caller may
// already have written against the conn-level sentinel.

func (s *Status) Unwrap() error { return s.cause }

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
// It is used only when the server ends the stream without a grpc-status —
// which is why 200 maps to UNKNOWN through the default arm rather than to OK:
// a truly successful response would have carried a grpc-status.
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
func statusFromRST(code conn.ErrCode) Code {
	switch code {
	case conn.ErrCodeRefusedStream:
		return Unavailable
	case conn.ErrCodeCancel:
		return Canceled
	case conn.ErrCodeEnhanceYourCalm:
		return ResourceExhausted
	case conn.ErrCodeInadequateSecurity:
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
	if !strings.ContainsRune(v, '%') && !hasControlByte(v) {
		return v
	}
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); {
		if v[i] == '%' && i+2 < len(v) {
			hi, ok1 := unhex(v[i+1])
			lo, ok2 := unhex(v[i+2])
			if ok1 && ok2 {
				if c := hi<<4 | lo; !controlByte(c) {
					b.WriteByte(c)
				}
				i += 3
				continue
			}
		}
		if !controlByte(v[i]) {
			b.WriteByte(v[i])
		}
		i++
	}
	return b.String()
}

// hasControlByte reports whether v carries a control byte verbatim. conn
// rejects those in a decoded response field, so on the live path this is always
// false — the scan is here so the guarantee decodeMessage offers holds by
// construction rather than by trusting a check in another package.
func hasControlByte(v string) bool {
	for i := 0; i < len(v); i++ {
		if controlByte(v[i]) {
			return true
		}
	}
	return false
}

// controlByte reports whether c is a C0 control or DEL. conn rejects these in a
// raw field value, but percent-decoding runs after that check and would put
// them back: a peer that sends "%0A" followed by a plausible timestamp forges a
// line in the caller's log, and "%1B" delivers ANSI escapes to whatever
// terminal prints the error. The specification requires the decode not to fail
// the RPC, so they are dropped rather than rejected.
func controlByte(c byte) bool { return c < 0x20 || c == 0x7f }

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

// statusFromTransport maps a transport-level failure to the *Status family, so a
// caller has ONE way to classify a failed RPC.
//
// A peer resetting the stream already became a *Status; a connection dying under
// it — GOAWAY, conn.ErrConnClosed, a cancelled context — leaked the transport
// error verbatim. Which of the two a caller got depended on whether conn
// delivered the failure as an event or as an error from Recv, an implementation
// detail of the transport that no caller should have to know. Retry
// classification needed errors.As(*Status) AND errors.Is(conn.ErrConnClosed) to
// be complete, and nothing said so.
//
// The context codes are separated from Unavailable deliberately: a deadline the
// CALLER set is not the server being unavailable, and conflating them makes a
// retry policy retry a request whose deadline has already passed.
//
// The original error stays reachable through Unwrap, so this adds a family
// rather than replacing one.
func statusFromTransport(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return &Status{Code: Canceled, Message: err.Error(), cause: err}
	case errors.Is(err, context.DeadlineExceeded):
		return &Status{Code: DeadlineExceeded, Message: err.Error(), cause: err}
	}
	// Everything else the transport can hand back — the connection closed, the
	// peer went away, the stream handle went stale — means this RPC did not
	// complete and another connection might serve it. That is Unavailable in
	// grpc's model.
	return &Status{Code: Unavailable, Message: err.Error(), cause: err}
}
