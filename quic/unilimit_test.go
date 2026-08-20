package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConformance_RFC9000_Sec46_FrameOverUniStreamLimit checks that a received
// frame referencing a server-initiated unidirectional stream whose ID exceeds the
// initial_max_streams_uni the client advertised is a STREAM_LIMIT_ERROR, even for
// frames other than STREAM (RESET_STREAM, STREAM_DATA_BLOCKED), while an in-limit
// stream is handled normally (RFC 9000 §4.6).
func TestConformance_RFC9000_Sec46_FrameOverUniStreamLimit(t *testing.T) {
	// The client advertised room for 2 server uni streams (IDs 3 and 7); ID 11 is
	// the third, past the limit.
	newConn := func() *Conn { return &Conn{localMaxStreamsUni: 2} }

	errResetOver := (&connFrameHandler{c: newConn()}).OnResetStream(11, 0, 0)
	errBlockedOver := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(11, 0)
	// Within the limit, an uncreated server-uni stream's RESET_STREAM/STREAM_DATA_BLOCKED
	// is handled without a limit error.
	errResetIn := (&connFrameHandler{c: newConn()}).OnResetStream(3, 0, 0)
	errBlockedIn := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(3, 0)
	code, ok := closeCodeFor(ErrTooManyUniStreams)

	assert.Truef(t, errResetOver == ErrTooManyUniStreams,
		"RESET_STREAM on an over-limit server-uni stream = %v, want ErrTooManyUniStreams", errResetOver)
	assert.Truef(t, errBlockedOver == ErrTooManyUniStreams,
		"STREAM_DATA_BLOCKED on an over-limit server-uni stream = %v, want ErrTooManyUniStreams", errBlockedOver)
	assert.NoErrorf(t, errResetIn,
		"RESET_STREAM on an in-limit server-uni stream = %v, want nil", errResetIn)
	assert.NoErrorf(t, errBlockedIn,
		"STREAM_DATA_BLOCKED on an in-limit server-uni stream = %v, want nil", errBlockedIn)
	// ErrTooManyUniStreams maps to the STREAM_LIMIT_ERROR transport code.
	assert.Truef(t, ok && code == ErrCodeStreamLimitError,
		"closeCodeFor(ErrTooManyUniStreams) = (%#x, %v), want (%#x, true)", code, ok, ErrCodeStreamLimitError)
}

// TestConformance_RFC9000_Sec46_ZeroUniStreamLimitForbidsAll covers the one
// equivalence class the §4.6 limit had no case for: zero.
//
// RFC 9000 §18.2 permits an endpoint to advertise initial_max_streams_uni = 0,
// and doing so forbids EVERY peer-initiated unidirectional stream — there is no
// legal first one. The tests above use a limit of 2 and exercise the stream one
// past it and one comfortably inside; the two neighbouring off-by-one mutants are
// both caught, so the boundary is otherwise well pinned. Zero is different in
// kind: it is the only value where the smallest possible peer-uni stream ID must
// already be refused, and a mutant that treats a zero limit as a limit of one
// left the whole suite green. Under it a client advertising no unidirectional
// streams silently accepts server-uni stream 3. #854.
//
// Stream 3 is the FIRST server-initiated unidirectional stream, so this is the
// boundary case, not a value chosen from the middle of the range.
func TestConformance_RFC9000_Sec46_ZeroUniStreamLimitForbidsAll(t *testing.T) {
	newConn := func() *Conn { return &Conn{localMaxStreamsUni: 0} }

	errStream := (&connFrameHandler{c: newConn()}).OnStream(3, 0, false, []byte("x"))
	errReset := (&connFrameHandler{c: newConn()}).OnResetStream(3, 0, 0)
	errBlocked := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(3, 0)
	// Control: the same frames on a CLIENT-initiated bidi stream are unaffected by
	// the uni limit, so the arms above cannot be passing because the fixture
	// refuses everything.
	errBidiControl := (&connFrameHandler{c: &Conn{localMaxStreamsUni: 0, nextBidiStreamID: 4}}).
		OnStreamDataBlocked(0, 0)

	assert.Truef(t, errStream == ErrTooManyUniStreams,
		"STREAM on server-uni stream 3 with initial_max_streams_uni = 0 = %v, want "+
			"ErrTooManyUniStreams — a zero limit forbids every peer-initiated uni stream, "+
			"and the first one is not a special case", errStream)
	assert.Truef(t, errReset == ErrTooManyUniStreams,
		"RESET_STREAM on server-uni stream 3 with a zero limit = %v, want ErrTooManyUniStreams",
		errReset)
	assert.Truef(t, errBlocked == ErrTooManyUniStreams,
		"STREAM_DATA_BLOCKED on server-uni stream 3 with a zero limit = %v, want "+
			"ErrTooManyUniStreams", errBlocked)
	assert.NoErrorf(t, errBidiControl,
		"control: STREAM_DATA_BLOCKED on a created client-bidi stream = %v, want nil — "+
			"the uni limit must not bleed into other stream types, or the arms above "+
			"prove nothing", errBidiControl)
}
