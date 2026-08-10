package quic

import (
	"testing"
	"time"
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
	if err != nil {
		t.Fatal(err)
	}
	h := &connFrameHandler{c: c}
	if err := h.OnStream(s.ID(), 0, false, make([]byte, int(DefaultStreamRecvWindow))); err != nil {
		t.Fatal(err)
	}
	if got := len(s.Recv()); got != int(DefaultStreamRecvWindow) {
		t.Fatalf("consumed %d bytes, want %d", got, DefaultStreamRecvWindow)
	}
	if s.recvMax <= DefaultStreamRecvWindow {
		t.Fatalf("no grant was queued: recvMax = %d", s.recvMax)
	}
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
	if _, still := c.sent[spaceApp].packets[0]; still {
		t.Fatal("the setup did not actually declare a packet lost")
	}
}

// queuedCtrl decodes whatever is sitting in pendingCtrl.
func queuedCtrl(t *testing.T, c *Conn) ctrlCollector {
	t.Helper()
	var col ctrlCollector
	if err := ParseFrames(c.pendingCtrl, &col); err != nil {
		t.Fatal(err)
	}
	return col
}

// TestRegrant_StreamDataBlockedResendsCurrentLimit is the deadlock gate: a peer
// stuck at a stale limit says so, and must be told the current one.
func TestRegrant_StreamDataBlockedResendsCurrentLimit(t *testing.T) {
	c, s, stale := grantedStream(t)
	h := &connFrameHandler{c: c}

	if err := h.OnStreamDataBlocked(s.ID(), stale); err != nil {
		t.Fatalf("OnStreamDataBlocked: %v", err)
	}
	col := queuedCtrl(t, c)
	got, ok := col.streamData[s.ID()]
	if !ok {
		t.Fatal("STREAM_DATA_BLOCKED at a stale limit queued no MAX_STREAM_DATA — the " +
			"peer stays blocked at a limit we already moved past, and the transfer " +
			"deadlocks: no data arrives, so nothing is consumed, so no further grant " +
			"is ever produced")
	}
	if got != s.recvMax {
		t.Errorf("re-sent MAX_STREAM_DATA = %d, want the CURRENT limit %d — resending "+
			"the value the peer already has leaves it exactly as stuck", got, s.recvMax)
	}
}

// TestRegrant_StreamDataBlockedAtCurrentLimitIsQuiet is the bound. A peer blocked
// at the limit it already knows is legitimately blocked, not out of sync, and
// re-stating the limit would only add a frame per frame it sends.
func TestRegrant_StreamDataBlockedAtCurrentLimitIsQuiet(t *testing.T) {
	c, s, _ := grantedStream(t)
	h := &connFrameHandler{c: c}

	for _, at := range []uint64{s.recvMax, s.recvMax + 1} {
		c.pendingCtrl = c.pendingCtrl[:0]
		if err := h.OnStreamDataBlocked(s.ID(), at); err != nil {
			t.Fatalf("OnStreamDataBlocked(%d): %v", at, err)
		}
		if len(c.pendingCtrl) != 0 {
			t.Errorf("STREAM_DATA_BLOCKED at %d (current limit %d) queued %d bytes, want none",
				at, s.recvMax, len(c.pendingCtrl))
		}
	}
}

// TestRegrant_StreamDataBlockedAfterFinIsQuiet mirrors onStreamConsumed, which
// stops granting once the final size is known: no further data can arrive, so
// credit would buy the peer nothing.
func TestRegrant_StreamDataBlockedAfterFinIsQuiet(t *testing.T) {
	c, s, stale := grantedStream(t)
	h := &connFrameHandler{c: c}
	s.recv.fin = true

	if err := h.OnStreamDataBlocked(s.ID(), stale); err != nil {
		t.Fatalf("OnStreamDataBlocked: %v", err)
	}
	if len(c.pendingCtrl) != 0 {
		t.Errorf("queued %d bytes of credit for a stream whose final size is known, want none",
			len(c.pendingCtrl))
	}
}

// TestRegrant_DataBlockedResendsCurrentLimit is the connection-level half. The
// connection limit deadlocks the whole connection rather than one stream, so it
// matters more, and it is reached by any response past half the connection window.
func TestRegrant_DataBlockedResendsCurrentLimit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 4}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	h := &connFrameHandler{c: c}
	win := int(DefaultStreamRecvWindow)
	// Two windows of consumption crosses the connection half-window and grants.
	for off := 0; off < 4; off++ {
		if err := h.OnStream(s.ID(), uint64(off*win), false, make([]byte, win)); err != nil {
			t.Fatalf("OnStream at window %d: %v", off, err)
		}
		if got := len(s.Recv()); got != win {
			t.Fatalf("window %d: consumed %d bytes, want %d", off, got, win)
		}
	}
	if c.connRecvMax <= DefaultConnRecvWindow {
		t.Fatalf("no connection grant was queued: connRecvMax = %d", c.connRecvMax)
	}
	stale := DefaultConnRecvWindow
	c.pendingCtrl = c.pendingCtrl[:0] // the grant's packet is lost

	if err := h.OnDataBlocked(stale); err != nil {
		t.Fatalf("OnDataBlocked: %v", err)
	}
	col := queuedCtrl(t, c)
	if !col.dataSet {
		t.Fatal("DATA_BLOCKED at a stale limit queued no MAX_DATA — the peer stays " +
			"blocked connection-wide and every stream on it stalls")
	}
	if col.data != c.connRecvMax {
		t.Errorf("re-sent MAX_DATA = %d, want the CURRENT limit %d", col.data, c.connRecvMax)
	}

	// And the bound, on the same connection.
	c.pendingCtrl = c.pendingCtrl[:0]
	if err := h.OnDataBlocked(c.connRecvMax); err != nil {
		t.Fatalf("OnDataBlocked at the current limit: %v", err)
	}
	if len(c.pendingCtrl) != 0 {
		t.Errorf("DATA_BLOCKED at the current limit queued %d bytes, want none", len(c.pendingCtrl))
	}
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
	if !ok {
		t.Fatal("a loss episode queued no MAX_STREAM_DATA — a grant is sent once and " +
			"never retransmitted, so the packet that carried it taking a drop leaves " +
			"the peer permanently behind (RFC 9000 §13.3)")
	}
	if got != s.recvMax {
		t.Errorf("re-sent MAX_STREAM_DATA = %d, want the CURRENT limit %d", got, s.recvMax)
	}
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
		if got, ok := col.streamData[s.ID()]; !ok || got != s.recvMax {
			t.Fatalf("loss episode %d re-sent %d (present=%v), want the current limit %d — "+
				"the retry must survive its own losses", episode, got, ok, s.recvMax)
		}
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
		if err != nil {
			t.Fatal(err)
		}
		// Well under the half-window that earns a grant.
		if err := h.OnStream(s.ID(), 0, false, make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}
		s.Recv()
	}
	c.pendingCtrl = c.pendingCtrl[:0]

	loseAPacket(t, c)

	if len(c.pendingCtrl) != 0 {
		t.Errorf("a loss episode queued %d bytes for streams that never got a grant, want none",
			len(c.pendingCtrl))
	}
}

// TestRegrant_LossEpisodeDropsFinishedStreams pins that the tracking set does not
// grow without bound: a stream whose final size is known can never need credit
// again and must leave.
func TestRegrant_LossEpisodeDropsFinishedStreams(t *testing.T) {
	c, s, _ := grantedStream(t)
	if len(c.grantedStreams) != 1 {
		t.Fatalf("granted set holds %d streams, want 1", len(c.grantedStreams))
	}
	s.recv.fin = true

	loseAPacket(t, c)

	if _, still := c.grantedStreams[s.ID()]; still {
		t.Error("a stream whose final size is known stayed in the granted set — the set " +
			"would grow for the life of the connection")
	}
	var col ctrlCollector
	if err := ParseFrames(c.pendingCtrl, &col); err != nil {
		t.Fatal(err)
	}
	if _, sent := col.streamData[s.ID()]; sent {
		t.Error("credit was re-sent for a stream that can receive no more data")
	}
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
	if err := h.OnStreamDataBlocked(0x3, 0); err != nil {
		t.Fatalf("OnStreamDataBlocked for an unknown stream: %v", err)
	}
	if len(c.pendingCtrl) != 0 {
		t.Errorf("queued %d bytes for a stream that does not exist, want none", len(c.pendingCtrl))
	}
}
