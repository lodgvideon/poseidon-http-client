package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec19_StreamStateErrors checks that a frame violating a
// stream's directionality is a STREAM_STATE_ERROR: a STREAM (§19.8) or
// RESET_STREAM (§19.4) on a send-only client-initiated unidirectional stream
// (id&0x3 == 0x2), and a STOP_SENDING (§19.5) on a receive-only server-initiated
// unidirectional stream (id&0x3 == 0x3).
func TestConformance_RFC9000_Sec19_StreamStateErrors(t *testing.T) {
	t.Run("stream-on-send-only", func(t *testing.T) {
		h := &connFrameHandler{c: &Conn{}}

		err := h.OnStream(2, 0, false, []byte("x"))

		assert.Truef(t, err == ErrStreamState,
			"OnStream on a send-only stream = %v, want ErrStreamState", err)
	})
	t.Run("reset-on-send-only", func(t *testing.T) {
		h := &connFrameHandler{c: &Conn{}}

		err := h.OnResetStream(2, 0, 0)

		assert.Truef(t, err == ErrStreamState,
			"OnResetStream on a send-only stream = %v, want ErrStreamState", err)
	})
	t.Run("stop-sending-on-recv-only", func(t *testing.T) {
		h := &connFrameHandler{c: &Conn{}}

		err := h.OnStopSending(3, 0)

		assert.Truef(t, err == ErrStreamState,
			"OnStopSending on a receive-only stream = %v, want ErrStreamState", err)
	})
}

// TestConformance_RFC9000_Sec19_StreamStateValidDirections checks that the same
// frames on a stream with the matching direction are accepted (no false positive):
// a STREAM on a receive-capable server-uni stream, and a STOP_SENDING on a
// send-capable client-uni stream.
func TestConformance_RFC9000_Sec19_StreamStateValidDirections(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 10}
	rc := &Conn{localMaxStreamsUni: 10}
	s, err := rc.acceptPeerUniStream(3)
	require.NoError(t, err, "accepting the peer's unidirectional stream 3")

	// STREAM on a server-initiated unidirectional stream (receive-only for us): ok.
	errStream := (&connFrameHandler{c: c}).OnStream(3, 0, false, []byte("x"))
	// STOP_SENDING on a client-initiated unidirectional stream we have created
	// (send-only for us, so a send side exists): ok, not a STREAM_STATE_ERROR.
	errStopSending := (&connFrameHandler{c: &Conn{openedUni: 1}}).OnStopSending(2, 0)
	// RESET_STREAM on a server-initiated unidirectional stream (receive-only for
	// us): ok — the server may reset its own send side, which is our receive side.
	errReset := (&connFrameHandler{c: rc}).OnResetStream(3, 7, 0)

	assert.NoErrorf(t, errStream,
		"OnStream on a receive-capable server-uni stream = %v, want nil", errStream)
	assert.NoErrorf(t, errStopSending,
		"OnStopSending on a created send-capable client-uni stream = %v, want nil", errStopSending)
	assert.NoErrorf(t, errReset,
		"OnResetStream on a receive-capable server-uni stream = %v, want nil", errReset)
	assert.True(t, s.recvReset,
		"a valid RESET_STREAM on a server-uni stream should mark its receive side reset")
}

// TestConformance_RFC9000_Sec19_StreamStateNotCreated checks that a STREAM (§19.8)
// or STOP_SENDING (§19.5) for a locally initiated stream that has not yet been
// created is a STREAM_STATE_ERROR, while the same frame for an ID at or below our
// high-water mark — a stream we created and have since closed — is ignored.
func TestConformance_RFC9000_Sec19_StreamStateNotCreated(t *testing.T) {
	// Cursors: client bidi opened through ID 0 (next is 4); client uni opened
	// through ID 2 (next is 6).
	newConn := func() *Conn { return &Conn{nextBidiStreamID: 4, openedUni: 1} }

	t.Run("stream-on-not-created-bidi", func(t *testing.T) {
		h := &connFrameHandler{c: newConn()}

		err := h.OnStream(4, 0, false, []byte("x"))

		assert.Truef(t, err == ErrStreamState,
			"OnStream on a not-yet-created client-bidi stream = %v, want ErrStreamState", err)
	})
	t.Run("stream-on-closed-bidi-ignored", func(t *testing.T) {
		h := &connFrameHandler{c: newConn()}

		err := h.OnStream(0, 0, false, []byte("x"))

		assert.NoErrorf(t, err,
			"OnStream on a created-then-closed client-bidi stream = %v, want nil", err)
	})
	t.Run("stop-sending-on-not-created-bidi", func(t *testing.T) {
		h := &connFrameHandler{c: newConn()}

		err := h.OnStopSending(4, 0)

		assert.Truef(t, err == ErrStreamState,
			"OnStopSending on a not-yet-created client-bidi stream = %v, want ErrStreamState", err)
	})
	t.Run("stop-sending-on-not-created-uni", func(t *testing.T) {
		h := &connFrameHandler{c: newConn()}

		err := h.OnStopSending(6, 0)

		assert.Truef(t, err == ErrStreamState,
			"OnStopSending on a not-yet-created client-uni stream = %v, want ErrStreamState", err)
	})
	t.Run("stop-sending-on-closed-bidi-ignored", func(t *testing.T) {
		h := &connFrameHandler{c: newConn()}

		err := h.OnStopSending(0, 0)

		assert.NoErrorf(t, err,
			"OnStopSending on a created-then-closed client-bidi stream = %v, want nil", err)
	})
}

// TestConformance_RFC9000_Sec1910_MaxStreamDataStreamState checks the two §19.10
// MUSTs for a received MAX_STREAM_DATA: one for a receive-only stream (a
// server-initiated unidirectional stream, which the client has no send side on) and
// one for a locally initiated stream not yet created are both STREAM_STATE_ERROR,
// while one for a created (or since-closed) stream is honored or ignored.
func TestConformance_RFC9000_Sec1910_MaxStreamDataStreamState(t *testing.T) {
	t.Run("recv-only", func(t *testing.T) {
		c := &Conn{localMaxStreamsUni: 10}
		_, err := c.acceptPeerUniStream(3)
		require.NoError(t, err, "accepting the peer's unidirectional stream 3")

		errMSD := (&connFrameHandler{c: c}).OnMaxStreamData(3, 1000)

		assert.Truef(t, errMSD == ErrStreamState,
			"MAX_STREAM_DATA on a receive-only server-uni stream = %v, want ErrStreamState", errMSD)
	})
	t.Run("not-created-bidi", func(t *testing.T) {
		c := &Conn{nextBidiStreamID: 4}

		err := (&connFrameHandler{c: c}).OnMaxStreamData(4, 1000)

		assert.Truef(t, err == ErrStreamState,
			"MAX_STREAM_DATA on a not-yet-created client-bidi stream = %v, want ErrStreamState", err)
	})
	t.Run("closed-ignored-and-open-honored", func(t *testing.T) {
		c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 4}}
		s, err := c.OpenStream() // stream 0
		require.NoError(t, err, "OpenStream")

		errOpen := (&connFrameHandler{c: c}).OnMaxStreamData(s.ID(), 7777)
		grantedMax := s.sendMax
		delete(c.streams, s.ID()) // retire it; the ID stays below the high-water mark
		errClosed := (&connFrameHandler{c: c}).OnMaxStreamData(s.ID(), 9999)

		assert.NoErrorf(t, errOpen, "MAX_STREAM_DATA on an open stream = %v, want nil", errOpen)
		assert.EqualValuesf(t, 7777, grantedMax,
			"sendMax = %d, want 7777 (the grant must still be honored)", grantedMax)
		assert.NoErrorf(t, errClosed,
			"MAX_STREAM_DATA on a created-then-closed stream = %v, want nil (ignored)", errClosed)
	})
}

// TestConn_StreamStateError_ClosesWithCode checks that ErrStreamState maps to the
// STREAM_STATE_ERROR transport code (RFC 9000 §20.1).
func TestConn_StreamStateError_ClosesWithCode(t *testing.T) {
	code, ok := closeCodeFor(ErrStreamState)

	assert.Truef(t, ok && code == ErrCodeStreamStateError,
		"closeCodeFor(ErrStreamState) = (%#x, %v), want (%#x, true)", code, ok, ErrCodeStreamStateError)
}
