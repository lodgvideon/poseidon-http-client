package conn

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestSentinelsAreDistinct(t *testing.T) {
	all := []error{
		ErrALPNFailed,
		ErrTooManyStreams,
		ErrConnClosed,
		ErrStreamClosed,
		ErrFlowControlExhausted,
		ErrUnexpectedPushPromise,
	}
	for i, a := range all {
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Fatalf("sentinels %d and %d collide", i, j)
			}
		}
	}
}

func TestConnError_ErrorAndUnwrap(t *testing.T) {
	e := &ConnError{Code: frame.ErrCodeProtocolError, Reason: "bad preface", Last: 0}
	if e.Error() == "" {
		t.Fatalf("Error() empty")
	}
	if !errors.Is(e, e) {
		t.Fatalf("errors.Is self failed")
	}
}

func TestStreamError_ErrorString(t *testing.T) {
	e := &StreamError{StreamID: 3, Code: frame.ErrCodeCancel}
	got := e.Error()
	if got == "" || !contains(got, "stream 3") {
		t.Fatalf("unexpected: %q", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub ||
		(len(s) > len(sub) && (indexOf(s, sub) >= 0)))
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
