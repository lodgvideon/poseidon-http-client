package http3

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	wrapped := fmt.Errorf("do: %w", err)

	assert.ErrorIs(t, err, ErrH3Control,
		"a typed connection error no longer matches ErrH3Control; every existing "+
			"errors.Is check against the sentinel would stop firing")
	// Wrapped, as a caller several layers up sees it.
	assert.ErrorIs(t, wrapped, ErrH3Control, "the match does not survive wrapping")
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
		ok := errors.As(err, &ce)

		require.Truef(t, ok, "code %#x: errors.As failed", code)
		assert.Equalf(t, code, ce.Code, "code %#x survived as %#x", code, ce.Code)
	}
}

// TestH3ConnError_MessageNamesKnownCodes keeps the diagnosis readable: a bare
// hex code in a log is the thing this change exists to improve on.
func TestH3ConnError_MessageNamesKnownCodes(t *testing.T) {
	known := &H3ConnError{Code: H3FrameError}
	unknown := &H3ConnError{Code: 0xdead}

	gotKnown, gotUnknown := known.Error(), unknown.Error()

	assert.Containsf(t, gotKnown, "H3_FRAME_ERROR",
		"Error() = %q, want it to name H3_FRAME_ERROR", gotKnown)
	// An unknown code still renders, in hex, rather than being swallowed.
	assert.Containsf(t, gotUnknown, "0xdead",
		"Error() = %q, want the raw code for an unknown value", gotUnknown)
}

// TestH3ConnError_DistinguishesCauses is the failure the issue describes, stated
// as a test: two different violations must not be the same error any more.
func TestH3ConnError_DistinguishesCauses(t *testing.T) {
	peerFraming := error(&H3ConnError{Code: H3FrameError})
	localQPACK := error(&H3ConnError{Code: 0x0200}) // QPACK_DECOMPRESSION_FAILED

	var a, b *H3ConnError
	okA, okB := errors.As(peerFraming, &a), errors.As(localQPACK, &b)

	require.True(t, okA, "errors.As failed on the peer-framing error")
	require.True(t, okB, "errors.As failed on the local QPACK error")
	assert.NotEqual(t, a.Code, b.Code, "the two causes carry the same code")
	// Both still answer to the sentinel, so a caller that only wants "connection
	// died" is unaffected by the added detail.
	assert.ErrorIs(t, peerFraming, ErrH3Control, "a caller matching the sentinel stopped seeing the peer-framing error")
	assert.ErrorIs(t, localQPACK, ErrH3Control, "a caller matching the sentinel stopped seeing the local QPACK error")
}

// TestH3ConnError_RealPathCarriesTheCode drives an actual connection-level
// violation and reads the code off the RETURNED error.
//
// The other tests in this file construct an H3ConnError themselves, so they pass
// whether or not connError ever produces one — reverting connError to the bare
// sentinel left every one of them green. This is the gate that fails: it asserts
// the code the client put on the wire is the code the caller can read, which is
// the entire claim.
func TestH3ConnError_RealPathCarriesTheCode(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
		code  uint64
	}{
		{"SETTINGS on a request stream", AppendFrameHeader(nil, FrameSettings, 0), H3FrameUnexpected},
		{"reserved frame type", AppendFrameHeader(nil, 0x02, 0), H3FrameUnexpected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{tc.frame}, fin: true}}
			client, cerr := NewClientFake(conn, nil)
			require.NoError(t, cerr, "NewClientFake over the fake transport")

			_, _, err := client.Do(context.Background(),
				&Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

			var ce *H3ConnError
			require.Truef(t, errors.As(err, &ce),
				"err = %v (%T), want a *H3ConnError — the code is on the wire "+
					"(close code %#x) but not in the error the caller gets",
				err, err, conn.closeCode)
			assert.Equalf(t, tc.code, ce.Code, "error carries %#x, want %#x", ce.Code, tc.code)
			assert.Equalf(t, conn.closeCode, ce.Code,
				"error carries %#x but %#x went on the wire; the two must agree",
				ce.Code, conn.closeCode)
		})
	}
}
