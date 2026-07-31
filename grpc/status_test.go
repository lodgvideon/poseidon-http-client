package grpc

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestCode_String(t *testing.T) {
	if got := Unauthenticated.String(); got != "UNAUTHENTICATED" {
		t.Fatalf("Unauthenticated = %q", got)
	}
	if got := Code(99).String(); got != "CODE(99)" {
		t.Fatalf("Code(99) = %q", got)
	}
}

func TestStatus_ErrIsNilForOK(t *testing.T) {
	if err := (Status{Code: OK}).Err(); err != nil {
		t.Fatalf("OK.Err() = %v, want nil", err)
	}
	err := Status{Code: NotFound, Message: "no such user"}.Err()
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Err() = %T, want *Status", err)
	}
	if st.Code != NotFound {
		t.Fatalf("code = %v", st.Code)
	}
	if err.Error() != "grpc: NOT_FOUND: no such user" {
		t.Fatalf("Error() = %q", err.Error())
	}
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
		if got := statusFromHTTP(httpStatus); got != want {
			t.Errorf("statusFromHTTP(%d) = %v, want %v", httpStatus, got, want)
		}
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
		if got := statusFromRST(rst); got != want {
			t.Errorf("statusFromRST(%d) = %v, want %v", rst, got, want)
		}
	}
}

func TestParseStatusCode_MalformedIsUnknown(t *testing.T) {
	if got := parseStatusCode("not-a-number"); got != Unknown {
		t.Fatalf("parseStatusCode(garbage) = %v, want UNKNOWN", got)
	}
	if got := parseStatusCode(""); got != Unknown {
		t.Fatalf("parseStatusCode(empty) = %v, want UNKNOWN", got)
	}
	if got := parseStatusCode("14"); got != Unavailable {
		t.Fatalf("parseStatusCode(14) = %v", got)
	}
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
		if got := decodeMessage(in); got != want {
			t.Errorf("decodeMessage(%q) = %q, want %q", in, got, want)
		}
	}
}
