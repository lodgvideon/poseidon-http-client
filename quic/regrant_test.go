package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A credit grant is queued once and never retransmitted, and the limit it carries
// is applied to local state the moment it is queued. Lose that packet and the two
// sides disagree: we believe the peer may send up to the new limit, the peer is
// still held at the old one. Nothing recovers on its own, because the next grant
// is only produced by consuming more data and no more data is coming.
//
// The way out is the peer's own DATA_BLOCKED / STREAM_DATA_BLOCKED. These tests
// pin that we answer it with the CURRENT limit, and that we stay quiet when the
// peer is not actually behind.
//
// The loss is modelled by truncating pendingCtrl after the grant is queued, which
// is exactly what a dropped datagram leaves behind: local state advanced, nothing
// on the wire.

// grantedStream returns a connection and a stream that has consumed a full window,
// so a MAX_STREAM_DATA has been queued and s.recvMax has moved past the initial
// limit, together with the limit the peer is still stuck at.
func grantedStream(t *testing.T) (*Conn, *Stream, uint64) {
	t.Helper()
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	require.NoError(t, err, "open the stream the grant is earned on")
	h := &connFrameHandler{c: c}
	require.NoError(t, h.OnStream(s.ID(), 0, false, make([]byte, int(DefaultStreamRecvWindow))),
		"deliver a full window so consuming it earns a grant")
	require.Equalf(t, int(DefaultStreamRecvWindow), len(s.Recv()),
		"the fixture must consume a full window, or no grant is queued")
	require.Greaterf(t, s.recvMax, DefaultStreamRecvWindow,
		"no grant was queued: recvMax = %d", s.recvMax)
	staleLimit := DefaultStreamRecvWindow // what the peer still believes, the grant having been lost
	c.pendingCtrl = c.pendingCtrl[:0]     // the grant's packet is lost
	return c, s, staleLimit
}

// loseAPacket drives a real loss episode in the application space: four
// ack-eliciting packets, the largest acknowledged, so the packet-threshold rule
// declares pn 0 lost (RFC 9002 §6.1.1) and detectLost runs everything a genuine
// loss runs.
func loseAPacket(t *testing.T, c *Conn) {
	t.Helper()
	base := time.Unix(500, 0)
	c.now = func() time.Time { return base }
	for pn := uint64(0); pn <= 3; pn++ {
		c.sent[spaceApp].onSent(pn, base, true, streamFrame(0, pn, "x"))
	}
	c.sent[spaceApp].ack(c, 3, 3)
	c.detectLost(spaceApp)
	require.NotContains(t, c.sent[spaceApp].packets, uint64(0),
		"the setup did not actually declare a packet lost")
}

// queuedCtrl decodes whatever is sitting in pendingCtrl.
func queuedCtrl(t *testing.T, c *Conn) ctrlCollector {
	t.Helper()
	var col ctrlCollector
	require.NoError(t, ParseFrames(c.pendingCtrl, &col), "the queued control frames must decode")
	return col
}

// TestRegrant_StreamDataBlockedResendsCurrentLimit is the deadlock gate: a peer
// stuck at a stale limit says so, and must be told the current one.
func TestRegrant_StreamDataBlockedResendsCurrentLimit(t *testing.T) {
	c, s, stale := grantedStream(t)
	h := &connFrameHandler{c: c}

	blockedErr := h.OnStreamDataBlocked(s.ID(), stale)

	require.NoErrorf(t, blockedErr, "OnStreamDataBlocked: %v", blockedErr)
	col := queuedCtrl(t, c)
	got, ok := col.streamData[s.ID()]
	require.True(t, ok, "STREAM_DATA_BLOCKED at a stale limit queued no MAX_STREAM_DATA — the "+
		"peer stays blocked at a limit we already moved past, and the transfer "+
		"deadlocks: no data arrives, so nothing is consumed, so no further grant "+
		"is ever produced")
	assert.Equalf(t, s.recvMax, got,
		"re-sent MAX_STREAM_DATA = %d, want the CURRENT limit %d — resending "+
			"the value the peer already has leaves it exactly as stuck", got, s.recvMax)
}

// TestRegrant_StreamDataBlockedAtCurrentLimitIsQuiet is the bound. A peer blocked
// at the limit it already knows is legitimately blocked, not out of sync, and
// re-stating the limit would only add a frame per frame it sends.
func TestRegrant_StreamDataBlockedAtCurrentLimitIsQuiet(t *testing.T) {
	c, s, _ := grantedStream(t)
	h := &connFrameHandler{c: c}

	for _, at := range []uint64{s.recvMax, s.recvMax + 1} {
		c.pendingCtrl = c.pendingCtrl[:0]

		blockedErr := h.OnStreamDataBlocked(s.ID(), at)

		require.NoErrorf(t, blockedErr, "OnStreamDataBlocked(%d): %v", at, blockedErr)
		assert.Emptyf(t, c.pendingCtrl,
			"STREAM_DATA_BLOCKED at %d (current limit %d) queued %d bytes, want none",
			at, s.recvMax, len(c.pendingCtrl))
	}
}

// TestRegrant_StreamDataBlockedAfterFinIsQuiet mirrors onStreamConsumed, which
// stops granting once the final size is known: no further data can arrive, so
// credit would buy the peer nothing.
func TestRegrant_StreamDataBlockedAfterFinIsQuiet(t *testing.T) {
	c, s, stale := grantedStream(t)
	h := &connFrameHandler{c: c}
	s.recv.fin = true

	blockedErr := h.OnStreamDataBlocked(s.ID(), stale)

	require.NoErrorf(t, blockedErr, "OnStreamDataBlocked: %v", blockedErr)
	assert.Emptyf(t, c.pendingCtrl,
		"queued %d bytes of credit for a stream whose final size is known, want none",
		len(c.pendingCtrl))
}

// TestRegrant_DataBlockedResendsCurrentLimit is the connection-level half. The
// connection limit deadlocks the whole connection rather than one stream, so it
// matters more, and it is reached by any response past half the connection window.
func TestRegrant_DataBlockedResendsCurrentLimit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 4}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	require.NoError(t, err, "open the stream the connection grant is earned on")
	h := &connFrameHandler{c: c}
	win := int(DefaultStreamRecvWindow)
	// Two windows of consumption crosses the connection half-window and grants.
	for off := 0; off < 4; off++ {
		require.NoErrorf(t, h.OnStream(s.ID(), uint64(off*win), false, make([]byte, win)),
			"OnStream at window %d", off)
		require.Equalf(t, win, len(s.Recv()), "window %d: consumed the wrong number of bytes", off)
	}
	require.Greaterf(t, c.connRecvMax, DefaultConnRecvWindow,
		"no connection grant was queued: connRecvMax = %d", c.connRecvMax)
	stale := DefaultConnRecvWindow
	c.pendingCtrl = c.pendingCtrl[:0] // the grant's packet is lost

	staleErr := h.OnDataBlocked(stale)
	col := queuedCtrl(t, c)
	c.pendingCtrl = c.pendingCtrl[:0]
	currentErr := h.OnDataBlocked(c.connRecvMax)

	require.NoErrorf(t, staleErr, "OnDataBlocked: %v", staleErr)
	require.True(t, col.dataSet, "DATA_BLOCKED at a stale limit queued no MAX_DATA — the peer stays "+
		"blocked connection-wide and every stream on it stalls")
	assert.Equalf(t, c.connRecvMax, col.data,
		"re-sent MAX_DATA = %d, want the CURRENT limit %d", col.data, c.connRecvMax)
	// And the bound, on the same connection.
	require.NoErrorf(t, currentErr, "OnDataBlocked at the current limit: %v", currentErr)
	assert.Emptyf(t, c.pendingCtrl,
		"DATA_BLOCKED at the current limit queued %d bytes, want none", len(c.pendingCtrl))
}

// TestRegrant_LossEpisodeResendsCurrentLimit is the gate on the half that does not
// depend on the peer. Answering DATA_BLOCKED is not enough on its own: that frame
// crosses the same lossy path, and a peer sends it once per limit, so it is
// dropped as readily as the grant it was meant to rescue. Measured, not argued —
// with only the blocked-frame answer in place, a 1 MiB response through 10% loss
// still deadlocked on the second run of thirty.
// It drives detectLost rather than calling regrantAfterLoss, so that removing
// the call from the loss path fails it. The first version called the function
// directly and passed with the wiring deleted — a gate on a function nothing
// reaches is not a gate.
func TestRegrant_LossEpisodeResendsCurrentLimit(t *testing.T) {
	c, s, _ := grantedStream(t)

	loseAPacket(t, c)

	col := queuedCtrl(t, c)
	got, ok := col.streamData[s.ID()]
	require.True(t, ok, "a loss episode queued no MAX_STREAM_DATA — a grant is sent once and "+
		"never retransmitted, so the packet that carried it taking a drop leaves "+
		"the peer permanently behind (RFC 9000 §13.3)")
	assert.Equalf(t, s.recvMax, got,
		"re-sent MAX_STREAM_DATA = %d, want the CURRENT limit %d", got, s.recvMax)
}

// TestRegrant_LossEpisodeRepeatsUntilDelivered pins that the re-grant itself is
// retried. A re-grant is as droppable as the original, so one attempt would only
// narrow the window in which the deadlock happens rather than close it.
func TestRegrant_LossEpisodeRepeatsUntilDelivered(t *testing.T) {
	c, s, _ := grantedStream(t)

	for episode := 1; episode <= 3; episode++ {
		c.pendingCtrl = c.pendingCtrl[:0] // the previous re-grant was lost too
		c.sent[spaceApp] = sentSpace{}

		loseAPacket(t, c)

		col := queuedCtrl(t, c)
		got, ok := col.streamData[s.ID()]
		require.Truef(t, ok && got == s.recvMax,
			"loss episode %d re-sent %d (present=%v), want the current limit %d — "+
				"the retry must survive its own losses", episode, got, ok, s.recvMax)
	}
}

// TestRegrant_LossEpisodeQuietForUngrantedStreams is the cost bound. A connection
// carrying many small responses must not emit a frame per stream on every loss
// episode; only streams that actually received a grant are at risk.
func TestRegrant_LossEpisodeQuietForUngrantedStreams(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 8}, connRecvMax: DefaultConnRecvWindow}
	h := &connFrameHandler{c: c}
	for i := 0; i < 8; i++ {
		s, err := c.OpenStream()
		require.NoError(t, err, "open one of the eight small-response streams")
		// Well under the half-window that earns a grant.
		require.NoError(t, h.OnStream(s.ID(), 0, false, make([]byte, 1024)),
			"deliver a sub-threshold amount on the stream")
		s.Recv()
	}
	c.pendingCtrl = c.pendingCtrl[:0]

	loseAPacket(t, c)

	assert.Emptyf(t, c.pendingCtrl,
		"a loss episode queued %d bytes for streams that never got a grant, want none",
		len(c.pendingCtrl))
}

// TestRegrant_LossEpisodeDropsFinishedStreams pins that the tracking set does not
// grow without bound: a stream whose final size is known can never need credit
// again and must leave.
func TestRegrant_LossEpisodeDropsFinishedStreams(t *testing.T) {
	c, s, _ := grantedStream(t)
	require.Lenf(t, c.grantedStreams, 1, "granted set holds %d streams, want 1", len(c.grantedStreams))
	s.recv.fin = true

	loseAPacket(t, c)

	assert.NotContains(t, c.grantedStreams, s.ID(),
		"a stream whose final size is known stayed in the granted set — the set "+
			"would grow for the life of the connection")
	var col ctrlCollector
	require.NoError(t, ParseFrames(c.pendingCtrl, &col), "the queued control frames must decode")
	assert.NotContains(t, col.streamData, s.ID(),
		"credit was re-sent for a stream that can receive no more data")
}

// TestRegrant_UnknownStreamIsQuiet covers the lookup miss: a STREAM_DATA_BLOCKED
// for a peer-initiated stream we have already retired must not panic or invent a
// grant for a stream that is gone.
func TestRegrant_UnknownStreamIsQuiet(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1},
		localMaxStreamsUni: 4, connRecvMax: DefaultConnRecvWindow}
	h := &connFrameHandler{c: c}

	// A server-initiated unidirectional id (0x3) we have never seen: valid to
	// receive, absent from the registry.
	blockedErr := h.OnStreamDataBlocked(0x3, 0)

	require.NoErrorf(t, blockedErr, "OnStreamDataBlocked for an unknown stream: %v", blockedErr)
	assert.Emptyf(t, c.pendingCtrl,
		"queued %d bytes for a stream that does not exist, want none", len(c.pendingCtrl))
}
