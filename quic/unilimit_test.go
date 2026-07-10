package quic

import "testing"

// TestConformance_RFC9000_Sec46_FrameOverUniStreamLimit checks that a received
// frame referencing a server-initiated unidirectional stream whose ID exceeds the
// initial_max_streams_uni the client advertised is a STREAM_LIMIT_ERROR, even for
// frames other than STREAM (RESET_STREAM, STREAM_DATA_BLOCKED), while an in-limit
// stream is handled normally (RFC 9000 §4.6).
func TestConformance_RFC9000_Sec46_FrameOverUniStreamLimit(t *testing.T) {
	// The client advertised room for 2 server uni streams (IDs 3 and 7); ID 11 is
	// the third, past the limit.
	newConn := func() *Conn { return &Conn{localMaxStreamsUni: 2} }

	if err := (&connFrameHandler{c: newConn()}).OnResetStream(11, 0, 0); err != ErrTooManyUniStreams {
		t.Fatalf("RESET_STREAM on an over-limit server-uni stream = %v, want ErrTooManyUniStreams", err)
	}
	if err := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(11, 0); err != ErrTooManyUniStreams {
		t.Fatalf("STREAM_DATA_BLOCKED on an over-limit server-uni stream = %v, want ErrTooManyUniStreams", err)
	}

	// Within the limit, an uncreated server-uni stream's RESET_STREAM/STREAM_DATA_BLOCKED
	// is handled without a limit error.
	if err := (&connFrameHandler{c: newConn()}).OnResetStream(3, 0, 0); err != nil {
		t.Fatalf("RESET_STREAM on an in-limit server-uni stream = %v, want nil", err)
	}
	if err := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(3, 0); err != nil {
		t.Fatalf("STREAM_DATA_BLOCKED on an in-limit server-uni stream = %v, want nil", err)
	}

	// ErrTooManyUniStreams maps to the STREAM_LIMIT_ERROR transport code.
	if code, ok := closeCodeFor(ErrTooManyUniStreams); !ok || code != ErrCodeStreamLimitError {
		t.Fatalf("closeCodeFor(ErrTooManyUniStreams) = (%#x, %v), want (%#x, true)", code, ok, ErrCodeStreamLimitError)
	}
}
