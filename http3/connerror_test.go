package http3

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Every connection-level HTTP/3 violation used to collapse into the bare
// ErrH3Control sentinel: the RFC 9114 §8.1 code went on the wire and nowhere
// else, so a pool, a retry policy, a metric or a test could not tell a peer's
// framing bug from a local QPACK failure. The HTTP/2 engine types the same thing
// (conn.ConnError) and this package already typed the stream-level case
// (StreamResetError); only the connection-level case was untyped.

// TestH3ConnError_MatchesTheSentinel is the compatibility half. Code written
// against errors.Is(err, ErrH3Control) must keep working.
func TestH3ConnError_MatchesTheSentinel(t *testing.T) {
	err := error(&H3ConnError{Code: H3FrameError})
	if !errors.Is(err, ErrH3Control) {
		t.Error("a typed connection error no longer matches ErrH3Control; every existing " +
			"errors.Is check against the sentinel would stop firing")
	}
	// Wrapped, as a caller several layers up sees it.
	if !errors.Is(fmt.Errorf("do: %w", err), ErrH3Control) {
		t.Error("the match does not survive wrapping")
	}
}

// TestH3ConnError_CarriesTheCode is the point of the change: the code the peer
// was told about is the code the caller can read.
func TestH3ConnError_CarriesTheCode(t *testing.T) {
	for _, code := range []uint64{
		H3FrameError, H3SettingsErrorCode, H3ClosedCriticalStream,
		H3MissingSettings, H3IDError, 0x0200, // QPACK_DECOMPRESSION_FAILED
	} {
		err := error(&H3ConnError{Code: code})
		var ce *H3ConnError
		if !errors.As(err, &ce) {
			t.Fatalf("code %#x: errors.As failed", code)
		}
		if ce.Code != code {
			t.Errorf("code %#x survived as %#x", code, ce.Code)
		}
	}
}

// TestH3ConnError_MessageNamesKnownCodes keeps the diagnosis readable: a bare
// hex code in a log is the thing this change exists to improve on.
func TestH3ConnError_MessageNamesKnownCodes(t *testing.T) {
	if got := (&H3ConnError{Code: H3FrameError}).Error(); !strings.Contains(got, "H3_FRAME_ERROR") {
		t.Errorf("Error() = %q, want it to name H3_FRAME_ERROR", got)
	}
	// An unknown code still renders, in hex, rather than being swallowed.
	if got := (&H3ConnError{Code: 0xdead}).Error(); !strings.Contains(got, "0xdead") {
		t.Errorf("Error() = %q, want the raw code for an unknown value", got)
	}
}

// TestH3ConnError_DistinguishesCauses is the failure the issue describes, stated
// as a test: two different violations must not be the same error any more.
func TestH3ConnError_DistinguishesCauses(t *testing.T) {
	peerFraming := error(&H3ConnError{Code: H3FrameError})
	localQPACK := error(&H3ConnError{Code: 0x0200}) // QPACK_DECOMPRESSION_FAILED

	var a, b *H3ConnError
	if !errors.As(peerFraming, &a) || !errors.As(localQPACK, &b) {
		t.Fatal("errors.As failed")
	}
	if a.Code == b.Code {
		t.Fatal("the two causes carry the same code")
	}
	// Both still answer to the sentinel, so a caller that only wants "connection
	// died" is unaffected by the added detail.
	if !errors.Is(peerFraming, ErrH3Control) || !errors.Is(localQPACK, ErrH3Control) {
		t.Error("a caller matching the sentinel stopped seeing one of them")
	}
}
