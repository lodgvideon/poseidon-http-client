package conn

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/stretchr/testify/require"
)

func TestSentinelsAreDistinct(t *testing.T) {
	all := []error{
		ErrALPNFailed,
		ErrTooManyStreams,
		ErrConnClosed,
		ErrStreamClosed,
		ErrUnexpectedPushPromise,
	}

	for i, a := range all {
		for j, b := range all {
			if i == j {
				continue
			}

			require.Falsef(t, errors.Is(a, b),
				"sentinels %d (%v) and %d (%v) collide; errors.Is cannot tell them apart, "+
					"so a caller classifying one would match the other", i, a, j, b)
		}
	}
}

func TestConnError_ErrorAndUnwrap(t *testing.T) {
	e := &ConnError{Code: frame.ErrCodeProtocolError, Reason: "bad preface", Last: 0}

	got := e.Error()

	require.NotEmptyf(t, got, "Error() empty; a connection-fatal error with no text tells "+
		"the caller nothing about why the connection died")
	require.Truef(t, errors.Is(e, e), "errors.Is self failed")
}

func TestStreamError_ErrorString(t *testing.T) {
	e := &StreamError{StreamID: 3, Code: frame.ErrCodeCancel}

	got := e.Error()

	require.Containsf(t, got, "stream 3",
		"unexpected: %q; the id is the only thing that tells the caller which of its "+
			"concurrent streams was reset", got)
}
