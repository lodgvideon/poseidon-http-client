package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRetireConn returns a Conn with one bidi stream credit and receive flow
// control enabled, plus an open stream and a frame handler, for exercising
// stream-map retirement.
func newRetireConn(t *testing.T) (*Conn, *Stream, *connFrameHandler) {
	t.Helper()
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	require.NoError(t, err, "OpenStream on the retirement fixture")
	return c, s, &connFrameHandler{c: c}
}

// TestConn_StreamRetire_GracefulBothDirs checks that a stream is dropped from the
// routing map once the FIN has been sent and the whole response received.
func TestConn_StreamRetire_GracefulBothDirs(t *testing.T) {
	c, s, h := newRetireConn(t)
	s.finSent = true // request FIN sent

	err := h.OnStream(s.ID(), 0, true, []byte("resp"))

	require.NoError(t, err, "the response STREAM frame with FIN")
	_, stillRouted := c.streams[s.ID()]
	assert.False(t, stillRouted, "stream should be retired once both directions are done")
}

// TestConn_StreamRetire_KeptUntilFinSent checks that a stream whose response is
// complete but whose FIN has not been sent stays in the map.
func TestConn_StreamRetire_KeptUntilFinSent(t *testing.T) {
	c, s, h := newRetireConn(t)

	err := h.OnStream(s.ID(), 0, true, []byte("resp")) // recv complete, finSent false

	require.NoError(t, err, "the response STREAM frame with FIN")
	_, stillRouted := c.streams[s.ID()]
	assert.True(t, stillRouted, "stream must remain until its send side also finishes")
}

// TestConn_StreamRetire_OnPeerReset checks that a stream is retired when the peer
// resets the receive side after our FIN was sent.
func TestConn_StreamRetire_OnPeerReset(t *testing.T) {
	c, s, h := newRetireConn(t)
	s.finSent = true

	err := h.OnResetStream(s.ID(), 0, 100) // RESET_STREAM, final size 100

	require.NoError(t, err, "the peer's RESET_STREAM")
	_, stillRouted := c.streams[s.ID()]
	assert.False(t, stillRouted, "stream should be retired after FIN sent + peer RESET_STREAM")
}

// TestConn_StreamRetire_EvictsResetStream checks that a stream reset on the send
// side before its FIN is retired once its receive side is also terminal, so a
// long-lived connection does not accumulate reset streams. Reset scrubs the
// stream's STREAM data from the retransmit sources first, so §13.3 still holds
// even though the stream is no longer in the routing map (see
// TestConformance_RFC9000_Sec133_NoRetransmitAfterResetAndEvict).
func TestConn_StreamRetire_EvictsResetStream(t *testing.T) {
	c, s, h := newRetireConn(t)
	s.sendReset = true // send side aborted, finSent stays false

	err := h.OnResetStream(s.ID(), 0, 0)

	require.NoError(t, err, "the peer's RESET_STREAM")
	_, stillRouted := c.streams[s.ID()]
	assert.False(t, stillRouted,
		"a stream terminal on both sides (send reset + recv reset) must be retired")
}

// TestConn_StreamRetire_LateFrameIgnored checks that a STREAM frame arriving for a
// retired (client-initiated) stream is ignored, not treated as a protocol error.
func TestConn_StreamRetire_LateFrameIgnored(t *testing.T) {
	_, s, h := newRetireConn(t)
	s.finSent = true
	require.NoError(t, h.OnStream(s.ID(), 0, true, []byte("resp")),
		"the response that retires the stream")

	err := h.OnStream(s.ID(), 0, false, []byte("late retransmit"))

	assert.NoErrorf(t, err, "late frame for a retired stream = %v, want nil (ignored)", err)
}
