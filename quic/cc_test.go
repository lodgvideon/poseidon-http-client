package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ccTestConn() *Conn {
	base := time.Unix(1000, 0)
	return &Conn{cwnd: kInitialWindow, ssthresh: ^uint64(0), now: func() time.Time { return base }}
}

// TestConformance_RFC9002_Sec731_SlowStart: in slow start (cwnd < ssthresh) an
// acknowledgement grows the window by the acknowledged byte count and frees those
// bytes from the in-flight total (RFC 9002 §7.3.1).
func TestConformance_RFC9002_Sec731_SlowStart(t *testing.T) {
	c := ccTestConn()
	c.bytesInFlight = 3600

	c.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200}, true)

	assert.EqualValuesf(t, kInitialWindow+1200, c.cwnd,
		"cwnd = %d, want %d (slow start += acked)", c.cwnd, kInitialWindow+1200)
	assert.EqualValuesf(t, 2400, c.bytesInFlight, "bytesInFlight = %d, want 2400", c.bytesInFlight)
}

// TestConformance_RFC9002_Sec733_CongestionAvoidance verifies the byte-accumulator
// congestion-avoidance growth (RFC 9002 §7.3.3: ~one max_datagram_size per window
// of bytes acknowledged) and, critically, that it does not freeze at a large
// window where the literal per-ack `mds*acked/cwnd` truncates to zero.
func TestConformance_RFC9002_Sec733_CongestionAvoidance(t *testing.T) {
	c := ccTestConn()
	c.cwnd, c.ssthresh = 4800, 4800 // cwnd >= ssthresh → congestion avoidance

	for i := 0; i < 4; i++ { // one full window (4×1200) acknowledged
		c.bytesInFlight = 4800
		c.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200}, true)
	}

	assert.EqualValuesf(t, 4800+maxDatagramSize, c.cwnd,
		"cwnd = %d, want %d (+1 mds per window acked)", c.cwnd, 4800+maxDatagramSize)

	// No-freeze: above mds² (~1.44 MB) the truncating formula would add 0.
	big := ccTestConn()
	big.cwnd, big.ssthresh = 2<<20, 1<<20
	start := big.cwnd

	for acked := uint64(0); acked < start; acked += 1200 {
		big.bytesInFlight = big.cwnd // congestion-limited: window kept full
		big.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200}, true)
	}

	assert.Greaterf(t, big.cwnd, start,
		"cwnd = %d did not grow past %d — avoidance froze at a high window", big.cwnd, start)
}

// TestConformance_RFC9002_Sec731_HalveOncePerRecovery verifies that a loss halves
// the window once per recovery episode: further losses of packets sent within the
// same episode do not re-halve, but a loss of a packet sent after the episode
// started does (RFC 9002 §7.3.1).
func TestConformance_RFC9002_Sec731_HalveOncePerRecovery(t *testing.T) {
	base := time.Unix(2000, 0)
	c := &Conn{cwnd: 20000, ssthresh: ^uint64(0), now: func() time.Time { return base }}
	sent := base.Add(-100 * time.Millisecond)

	c.onCongestionEvent(sent)
	firstCwnd, firstSsthresh := c.cwnd, c.ssthresh
	c.onCongestionEvent(sent.Add(-50 * time.Millisecond)) // same episode (before recoveryStart)
	sameEpisodeCwnd := c.cwnd
	c.onCongestionEvent(base.Add(50 * time.Millisecond)) // new episode (after recoveryStart)

	assert.EqualValuesf(t, 10000, firstCwnd,
		"first loss: cwnd=%d ssthresh=%d, want 10000/10000", firstCwnd, firstSsthresh)
	assert.EqualValuesf(t, 10000, firstSsthresh,
		"first loss: cwnd=%d ssthresh=%d, want 10000/10000", firstCwnd, firstSsthresh)
	assert.EqualValuesf(t, 10000, sameEpisodeCwnd,
		"second loss, same episode: cwnd=%d, want 10000 (halve once)", sameEpisodeCwnd)
	assert.EqualValuesf(t, 5000, c.cwnd, "new episode: cwnd=%d, want 5000", c.cwnd)
}

// TestConformance_RFC9002_Sec78_AppLimitedNoGrowth checks that acknowledging a
// packet sent while the sender was application-limited (bytes in flight below the
// window) does not grow the congestion window, though it still frees the acked
// bytes (RFC 9002 §7.8).
func TestConformance_RFC9002_Sec78_AppLimitedNoGrowth(t *testing.T) {
	c := ccTestConn()
	c.bytesInFlight = 3600 // window far from full

	c.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200}, false)

	assert.EqualValuesf(t, kInitialWindow, c.cwnd,
		"cwnd = %d, want %d (an app-limited ack must not grow the window)", c.cwnd, kInitialWindow)
	assert.EqualValuesf(t, 2400, c.bytesInFlight,
		"bytesInFlight = %d, want 2400 (acked bytes still freed)", c.bytesInFlight)
}

// TestConformance_RFC9002_Sec78_CwndLimited checks the congestion-limited test:
// a flight below half the window is application-limited, a full (or over-full)
// window is congestion-limited, and in slow start a more-than-half-full window
// counts as limited so growth is not stalled by unbatched acks (§7.8).
func TestConformance_RFC9002_Sec78_CwndLimited(t *testing.T) {
	c := ccTestConn() // cwnd = kInitialWindow (12000), ssthresh = ∞ (slow start)

	nearlyEmpty := c.cwndLimited(1200)
	full := c.cwndLimited(kInitialWindow)
	overHalfInSlowStart := c.cwndLimited(kInitialWindow/2 + 1)
	// Out of slow start the half-full relaxation does not apply.
	c.ssthresh = 6000
	halfInAvoidance := c.cwndLimited(kInitialWindow / 2)

	assert.False(t, nearlyEmpty, "a nearly empty window must be application-limited")
	assert.True(t, full, "a full window must be congestion-limited")
	assert.True(t, overHalfInSlowStart,
		"a more-than-half-full window in slow start must be congestion-limited")
	assert.False(t, halfInAvoidance,
		"a half-full window in congestion avoidance must be application-limited")
}

// TestConformance_RFC9002_Sec78_FullWindowAckGrowsCwnd drives a full-window,
// multi-range ACK through the frame parser and connFrameHandler to pin the §7.8
// wiring: the bytes in flight are captured once before the frame removes anything
// and reused across every range, so each acknowledged packet of a congestion-
// limited flight grows cwnd. A stale re-read after the first range would grow the
// window for only part of the flight.
func TestConformance_RFC9002_Sec78_FullWindowAckGrowsCwnd(t *testing.T) {
	base := time.Unix(3000, 0)
	c := &Conn{cwnd: 12000, ssthresh: ^uint64(0), now: func() time.Time { return base }}
	sent := base.Add(-100 * time.Millisecond)
	for pn := uint64(0); pn < 10; pn++ { // ten 1200-byte packets fill the window
		c.sent[spaceApp].onSent(pn, sent, true, nil)
		c.onPacketSent(spaceApp, pn, true, 1200)
	}
	c.sendPN[spaceApp] = 10 // sent packets 0..9 (§13.1 never-sent gate)
	require.EqualValuesf(t, 12000, c.bytesInFlight,
		"bytesInFlight = %d, want 12000 (full window)", c.bytesInFlight)
	// One ACK frame acknowledging 6–9 (First ACK Range) and 0–4 (an additional
	// range), leaving packet 5 in flight: nine packets acknowledged.
	ack := AppendAck(nil, 9, 0, 3, []AckRange{{Gap: 0, Length: 4}})
	fh := &connFrameHandler{c: c, space: spaceApp}

	err := ParseFrames(ack, fh)

	require.NoError(t, err, "ParseFrames")
	// Slow start adds each acknowledged packet: 9 × 1200. A stale re-read of bytes
	// in flight in the additional range would classify it application-limited and
	// grow by only 4 × 1200.
	assert.EqualValuesf(t, 12000+9*1200, c.cwnd,
		"cwnd = %d, want %d (a full-window multi-range ack grows per packet)", c.cwnd, 12000+9*1200)
}

// TestCC_GateClampedByWindow checks the send gate: grantable clamps to the space
// left in the congestion window and reports blockCong (no frame) when it is full.
func TestCC_GateClampedByWindow(t *testing.T) {
	c := &Conn{cwnd: 5000, bytesInFlight: 5000, connMax: 1 << 30}
	s := &Stream{conn: c, sendMax: 1 << 30}

	fullN, fullB := s.grantable(2000)
	c.bytesInFlight = 4000 // 1000 bytes of window left
	partN, partB := s.grantable(2000)

	assert.Zerof(t, fullN, "full window: got (%d, %v), want (0, blockCong)", fullN, fullB)
	assert.Equalf(t, blockCong, fullB, "full window: got (%d, %v), want (0, blockCong)", fullN, fullB)
	assert.EqualValuesf(t, 1000, partN, "partial window: got (%d, %v), want (1000, blockNone)", partN, partB)
	assert.Equalf(t, blockNone, partB, "partial window: got (%d, %v), want (1000, blockNone)", partN, partB)
}

// TestCC_PureAckNotCounted checks that non-ack-eliciting packets (pure ACKs) do
// not count toward bytes_in_flight (RFC 9002 §2 "in flight").
func TestCC_PureAckNotCounted(t *testing.T) {
	c := ccTestConn()

	c.sent[spaceApp].onSent(5, time.Unix(1000, 0), false, nil)
	c.onPacketSent(spaceApp, 5, false, 44)
	pureAckInFlight := c.bytesInFlight
	// An ack-eliciting packet is counted.
	c.sent[spaceApp].onSent(6, time.Unix(1000, 0), true, nil)
	c.onPacketSent(spaceApp, 6, true, 1200)

	assert.Zerof(t, pureAckInFlight,
		"pure-ACK packet must not be in flight, bytesInFlight=%d", pureAckInFlight)
	assert.EqualValuesf(t, 1200, c.bytesInFlight,
		"ack-eliciting packet not counted, bytesInFlight=%d, want 1200", c.bytesInFlight)
}

// TestConformance_RFC9001_Sec49_DiscardSpaceUncountsInFlight checks that
// discarding a packet-number space on key discard removes its unacknowledged
// bytes from the congestion controller (RFC 9002 §6.4) and clears its sent state,
// so a lost handshake-space ACK cannot leave a permanent phantom floor in
// bytes_in_flight (which could otherwise block all app-space sends).
func TestConformance_RFC9001_Sec49_DiscardSpaceUncountsInFlight(t *testing.T) {
	c := ccTestConn()
	c.sent[spaceInitial].onSent(0, time.Unix(1000, 0), true, nil)
	c.onPacketSent(spaceInitial, 0, true, 1200)
	c.sent[spaceHandshake].onSent(0, time.Unix(1000, 0), true, nil)
	c.onPacketSent(spaceHandshake, 0, true, 90)
	require.EqualValuesf(t, 1290, c.bytesInFlight, "bytesInFlight = %d, want 1290", c.bytesInFlight)
	require.True(t, c.hasInFlight(), "hasInFlight should be true with unacked packets in flight")

	c.discardSpace(spaceInitial)
	c.discardSpace(spaceHandshake)

	assert.Zerof(t, c.bytesInFlight,
		"after discard bytesInFlight = %d, want 0 (no phantom floor)", c.bytesInFlight)
	assert.False(t, c.hasInFlight(), "hasInFlight should be false after discarding both spaces")
	assert.Empty(t, c.sent[spaceInitial].packets, "sent maps should be empty after discard")
	assert.Empty(t, c.sent[spaceHandshake].packets, "sent maps should be empty after discard")
	assert.Truef(t, c.keys.Initial == nil, "discarded-space openers should be nil")
	assert.Truef(t, c.keys.Handshake == nil, "discarded-space openers should be nil")
}

// TestCC_DisabledSentinel checks that a hand-built Conn (cwnd == 0) leaves the
// send path unthrottled and the CC hooks inert, preserving pre-CC test behavior.
func TestCC_DisabledSentinel(t *testing.T) {
	c := &Conn{connMax: 1 << 30} // cwnd == 0
	s := &Stream{conn: c, sendMax: 1 << 30}

	n, b := s.grantable(2000)
	c.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200}, true)

	assert.Equalf(t, blockNone, b, "cwnd==0 must not gate: got (%d, %v)", n, b)
	assert.NotZerof(t, n, "cwnd==0 must not gate: got (%d, %v)", n, b)
	assert.Zerof(t, c.cwnd, "cwnd must stay 0 (disabled), got %d", c.cwnd)
}
