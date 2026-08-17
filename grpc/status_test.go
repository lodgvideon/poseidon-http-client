package grpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestCode_String(t *testing.T) {
	named := Unauthenticated.String()
	outOfRange := Code(99).String()

	require.Equalf(t, "UNAUTHENTICATED", named, "Unauthenticated = %q", named)
	require.Equalf(t, "CODE(99)", outOfRange, "Code(99) = %q", outOfRange)
}

func TestStatus_ErrIsNilForOK(t *testing.T) {
	okErr := (Status{Code: OK}).Err()
	err := Status{Code: NotFound, Message: "no such user"}.Err()

	require.NoErrorf(t, okErr, "OK.Err() = %v, want nil", okErr)
	var st *Status
	require.Truef(t, errors.As(err, &st), "Err() = %T, want *Status", err)
	require.Equalf(t, NotFound, st.Code, "code = %v", st.Code)
	require.Equalf(t, "grpc: NOT_FOUND: no such user", err.Error(), "Error() = %q", err.Error())
}

// TestStatusFromHTTP pins the mapping table from the gRPC over HTTP/2
// specification. It is the only path to a status when a server fails before it
// can produce a grpc-status trailer, which is what a proxy in the middle does.
func TestStatusFromHTTP(t *testing.T) {
	cases := map[int]Code{
		400: Internal,
		401: Unauthenticated,
		403: PermissionDenied,
		404: Unimplemented,
		429: Unavailable,
		502: Unavailable,
		503: Unavailable,
		504: Unavailable,
		418: Unknown,
		500: Unknown,
	}

	for httpStatus, want := range cases {
		got := statusFromHTTP(httpStatus)

		assert.Equalf(t, want, got, "statusFromHTTP(%d) = %v, want %v", httpStatus, got, want)
	}
}

func TestStatusFromRST(t *testing.T) {
	cases := map[frame.ErrCode]Code{
		frame.ErrCodeRefusedStream:      Unavailable,
		frame.ErrCodeCancel:             Canceled,
		frame.ErrCodeEnhanceYourCalm:    ResourceExhausted,
		frame.ErrCodeInadequateSecurity: PermissionDenied,
		frame.ErrCodeProtocolError:      Internal,
		frame.ErrCodeNoError:            Internal,
	}

	for rst, want := range cases {
		got := statusFromRST(rst)

		assert.Equalf(t, want, got, "statusFromRST(%d) = %v, want %v", rst, got, want)
	}
}

func TestParseStatusCode_MalformedIsUnknown(t *testing.T) {
	garbage := parseStatusCode("not-a-number")
	empty := parseStatusCode("")
	valid := parseStatusCode("14")

	require.Equalf(t, Unknown, garbage, "parseStatusCode(garbage) = %v, want UNKNOWN", garbage)
	require.Equalf(t, Unknown, empty, "parseStatusCode(empty) = %v, want UNKNOWN", empty)
	require.Equalf(t, Unavailable, valid, "parseStatusCode(14) = %v", valid)
}

func TestDecodeMessage(t *testing.T) {
	cases := map[string]string{
		"plain":         "plain",
		"a%20b":         "a b",
		"100%25":        "100%",
		"%D0%BF%D1%80":  "пр",
		"trailing%":     "trailing%",
		"short%2":       "short%2",
		"bad%zz":        "bad%zz",
		"mixed%20a%zzb": "mixed a%zzb",
	}

	for in, want := range cases {
		got := decodeMessage(in)

		assert.Equalf(t, want, got, "decodeMessage(%q) = %q, want %q", in, got, want)
	}
}
