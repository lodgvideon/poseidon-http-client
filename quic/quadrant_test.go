package quic

import "testing"

// Stream-ID quadrants are relative to the endpoint reading them (RFC 9000 §2.1):
// the low bit names the initiator, the second bit names directionality. Which
// quadrant is "send-only for us" therefore flips between a client Conn and a
// server one.
//
// Five frame handlers used to write the client's answer as a literal — 0x2 for
// send-only, 0x3 for receive-only. On a server that is exactly inverted, so a
// conformant peer resetting its own unidirectional stream was answered with a
// STREAM_STATE_ERROR and the connection torn down.
//
// These tests run every quadrant against both roles. A literal cannot pass them:
// whichever constant it picks, one of the two roles fails.

// quadrants names the four stream-ID classes by their low two bits.
var quadrants = []struct {
	name string
	id   uint64
}{
	{"client bidi", 0x0},
	{"server bidi", 0x1},
	{"client uni", 0x2},
	{"server uni", 0x3},
}

// TestStreamRole_SendOnlyAndRecvOnly pins the two predicates directly, for both
// roles, over all four quadrants. Bidirectional streams are neither: both
// endpoints send on them.
func TestStreamRole_SendOnlyAndRecvOnly(t *testing.T) {
	cases := []struct {
		role     string
		isServer bool
		sendOnly uint64 // the one quadrant that is send-only for this role
		recvOnly uint64
	}{
		{"client", false, 0x2, 0x3},
		{"server", true, 0x3, 0x2},
	}
	for _, tc := range cases {
		c := &Conn{isServer: tc.isServer}
		for _, q := range quadrants {
			wantSend := q.id == tc.sendOnly
			wantRecv := q.id == tc.recvOnly
			if got := c.sendOnlyStream(q.id); got != wantSend {
				t.Errorf("%s: sendOnlyStream(%s, id 0x%x) = %v, want %v",
					tc.role, q.name, q.id, got, wantSend)
			}
			if got := c.recvOnlyStream(q.id); got != wantRecv {
				t.Errorf("%s: recvOnlyStream(%s, id 0x%x) = %v, want %v",
					tc.role, q.name, q.id, got, wantRecv)
			}
		}
	}
}

// TestConformance_RFC9000_Sec194_ResetStreamQuadrantIsRoleAware is the bug in
// its original shape: a server receiving RESET_STREAM for the client's own
// unidirectional stream. The client is that stream's sender, so the frame is
// legal and must be accepted; rejecting it kills the connection over conformant
// behaviour.
func TestConformance_RFC9000_Sec194_ResetStreamQuadrantIsRoleAware(t *testing.T) {
	cases := []struct {
		role     string
		isServer bool
		legal    uint64 // a uni stream the PEER initiated: it may reset it
		illegal  uint64 // a uni stream WE initiated: the peer cannot reset it
	}{
		{"client", false, 0x3, 0x2},
		{"server", true, 0x2, 0x3},
	}
	for _, tc := range cases {
		c := &Conn{isServer: tc.isServer, localMaxStreamsUni: 100,
			// High-water marks so the ids below count as already created: this test
			// is about the quadrant check, not about never-opened streams.
			openedUni: 100, nextBidiStreamID: 1 << 20}
		h := &connFrameHandler{c: c}

		if err := h.OnResetStream(tc.legal, 0, 0); err == ErrStreamState {
			t.Errorf("%s: RESET_STREAM on the peer's own uni stream (0x%x) rejected as "+
				"STREAM_STATE_ERROR — the peer is its sender and the frame is legal",
				tc.role, tc.legal)
		}
		if err := h.OnResetStream(tc.illegal, 0, 0); err != ErrStreamState {
			t.Errorf("%s: RESET_STREAM on our own uni stream (0x%x) = %v, want ErrStreamState "+
				"— the peer has no send side there (RFC 9000 §19.4)", tc.role, tc.illegal, err)
		}
	}
}

// TestConformance_RFC9000_Sec195_StopSendingQuadrantIsRoleAware is the mirror:
// STOP_SENDING is addressed to a sender, so it is illegal on a stream we only
// receive on and legal on one we send on.
func TestConformance_RFC9000_Sec195_StopSendingQuadrantIsRoleAware(t *testing.T) {
	cases := []struct {
		role     string
		isServer bool
		legal    uint64 // our own uni stream: we send, the peer may ask us to stop
		illegal  uint64 // the peer's uni stream: we have no send side
	}{
		{"client", false, 0x2, 0x3},
		{"server", true, 0x3, 0x2},
	}
	for _, tc := range cases {
		c := &Conn{isServer: tc.isServer, localMaxStreamsUni: 100,
			// High-water marks so the ids below count as already created: this test
			// is about the quadrant check, not about never-opened streams.
			openedUni: 100, nextBidiStreamID: 1 << 20}
		h := &connFrameHandler{c: c}

		if err := h.OnStopSending(tc.legal, 0); err == ErrStreamState {
			t.Errorf("%s: STOP_SENDING on our own uni stream (0x%x) rejected — we are its "+
				"sender, so the request is legal", tc.role, tc.legal)
		}
		if err := h.OnStopSending(tc.illegal, 0); err != ErrStreamState {
			t.Errorf("%s: STOP_SENDING on a receive-only stream (0x%x) = %v, want "+
				"ErrStreamState (RFC 9000 §19.5)", tc.role, tc.illegal, err)
		}
	}
}

// TestConformance_RFC9000_Sec1910_MaxStreamDataQuadrantIsRoleAware covers the
// flow-control frame with the same shape: credit is addressed to a sender.
func TestConformance_RFC9000_Sec1910_MaxStreamDataQuadrantIsRoleAware(t *testing.T) {
	cases := []struct {
		role     string
		isServer bool
		legal    uint64
		illegal  uint64
	}{
		{"client", false, 0x2, 0x3},
		{"server", true, 0x3, 0x2},
	}
	for _, tc := range cases {
		c := &Conn{isServer: tc.isServer, localMaxStreamsUni: 100,
			// High-water marks so the ids below count as already created: this test
			// is about the quadrant check, not about never-opened streams.
			openedUni: 100, nextBidiStreamID: 1 << 20}
		h := &connFrameHandler{c: c}

		if err := h.OnMaxStreamData(tc.legal, 1024); err == ErrStreamState {
			t.Errorf("%s: MAX_STREAM_DATA on our own uni stream (0x%x) rejected — we send "+
				"there and the credit applies to us", tc.role, tc.legal)
		}
		if err := h.OnMaxStreamData(tc.illegal, 1024); err != ErrStreamState {
			t.Errorf("%s: MAX_STREAM_DATA on a receive-only stream (0x%x) = %v, want "+
				"ErrStreamState (RFC 9000 §19.10)", tc.role, tc.illegal, err)
		}
	}
}

// TestConformance_RFC9000_Sec1913_StreamDataBlockedQuadrantIsRoleAware covers the
// last handler: STREAM_DATA_BLOCKED comes from a sender.
func TestConformance_RFC9000_Sec1913_StreamDataBlockedQuadrantIsRoleAware(t *testing.T) {
	cases := []struct {
		role     string
		isServer bool
		legal    uint64
		illegal  uint64
	}{
		{"client", false, 0x3, 0x2},
		{"server", true, 0x2, 0x3},
	}
	for _, tc := range cases {
		c := &Conn{isServer: tc.isServer, localMaxStreamsUni: 100,
			// High-water marks so the ids below count as already created: this test
			// is about the quadrant check, not about never-opened streams.
			openedUni: 100, nextBidiStreamID: 1 << 20}
		h := &connFrameHandler{c: c}

		if err := h.OnStreamDataBlocked(tc.legal, 0); err == ErrStreamState {
			t.Errorf("%s: STREAM_DATA_BLOCKED on the peer's own uni stream (0x%x) rejected "+
				"— the peer is its sender", tc.role, tc.legal)
		}
		if err := h.OnStreamDataBlocked(tc.illegal, 0); err != ErrStreamState {
			t.Errorf("%s: STREAM_DATA_BLOCKED on a stream only we send on (0x%x) = %v, want "+
				"ErrStreamState (RFC 9000 §19.13)", tc.role, tc.illegal, err)
		}
	}
}

// TestConformance_RFC9000_Sec46_UniStreamLimitIsRoleAware pins the fifth site.
// The limit applies to streams the PEER opens, which is the other quadrant on a
// server.
func TestConformance_RFC9000_Sec46_UniStreamLimitIsRoleAware(t *testing.T) {
	const limit = 4
	cases := []struct {
		role     string
		isServer bool
		peerUni  uint64 // low bits of the peer's unidirectional quadrant
		ourUni   uint64
	}{
		{"client", false, 0x3, 0x2},
		{"server", true, 0x2, 0x3},
	}
	for _, tc := range cases {
		c := &Conn{isServer: tc.isServer, localMaxStreamsUni: limit}

		within := (limit-1)<<2 | tc.peerUni
		beyond := limit<<2 | tc.peerUni
		if c.exceedsUniStreamLimit(within) {
			t.Errorf("%s: stream index %d of the peer's uni quadrant reported over a limit of %d",
				tc.role, limit-1, limit)
		}
		if !c.exceedsUniStreamLimit(beyond) {
			t.Errorf("%s: stream index %d of the peer's uni quadrant not reported over a "+
				"limit of %d (RFC 9000 §4.6)", tc.role, limit, limit)
		}
		// Our own quadrant is not governed by what we advertised to the peer.
		if c.exceedsUniStreamLimit(limit<<2 | tc.ourUni) {
			t.Errorf("%s: our own uni stream measured against the limit we advertised for "+
				"the peer's streams", tc.role)
		}
	}
}
