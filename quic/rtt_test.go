package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const ms = time.Millisecond

func TestRTTStats_FirstSample(t *testing.T) {
	var r rttStats

	r.update(100*ms, 0)

	assert.Equalf(t, 100*ms, r.minRTT, "first sample = %+v, want minRTT=100ms", r)
	assert.Equalf(t, 100*ms, r.smoothedRTT, "first sample = %+v, want smoothed=100ms", r)
	assert.Equalf(t, 50*ms, r.rttvar, "first sample = %+v, want rttvar=50ms", r)
}

// TestRTTStats_Subsequent checks the §5.3 smoothing: rttvar = 3/4·rttvar +
// 1/4·|srtt-adjusted|, smoothed = 7/8·srtt + 1/8·adjusted.
func TestRTTStats_Subsequent(t *testing.T) {
	var r rttStats
	r.update(100*ms, 0)

	r.update(120*ms, 0)

	assert.Equalf(t, 100*ms, r.minRTT, "minRTT = %v, want 100ms", r.minRTT)
	assert.Equalf(t, (7*100*ms+120*ms)/8, r.smoothedRTT, // 102.5ms
		"smoothedRTT = %v, want %v", r.smoothedRTT, (7*100*ms+120*ms)/8)
	assert.Equalf(t, (3*50*ms+20*ms)/4, r.rttvar, // 42.5ms
		"rttvar = %v, want %v", r.rttvar, (3*50*ms+20*ms)/4)
}

// TestRTTStats_AckDelay checks that the peer's ack delay is subtracted, but not
// below min_rtt (§5.3).
func TestRTTStats_AckDelay(t *testing.T) {
	var r rttStats
	r.update(100*ms, 0)

	r.update(120*ms, 10*ms) // 120 >= min(100)+10, so adjusted = 110ms

	assert.Equalf(t, (7*100*ms+110*ms)/8, r.smoothedRTT,
		"smoothedRTT = %v, want %v (ack delay subtracted)", r.smoothedRTT, (7*100*ms+110*ms)/8)
}

func TestSentSpace_Ack(t *testing.T) {
	base := time.Unix(100, 0)
	var s sentSpace
	s.onSent(5, base, true, nil)
	s.onSent(6, base.Add(10*ms), true, nil)

	sendTime, ok := s.ack(nil, 5, 6)
	leftInFlight := len(s.packets)
	// A duplicate ACK of an already-largest packet yields no new RTT sample.
	s.onSent(6, base.Add(10*ms), true, nil)
	_, dupOK := s.ack(nil, 6, 6)

	require.Truef(t, ok, "ack = (%v, %v), want the largest's (pn 6) send time", sendTime, ok)
	assert.Truef(t, sendTime.Equal(base.Add(10*ms)),
		"ack sample time = %v, want the largest's (pn 6) send time %v", sendTime, base.Add(10*ms))
	assert.Zerof(t, leftInFlight, "acked packets not removed: %d left", leftInFlight)
	assert.False(t, dupOK, "duplicate largest ack must not resample RTT")
}

// TestConnFrameHandler_OnAck_UpdatesRTT drives the ACK path end to end: record
// sends, advance an injected clock, feed an ACK, and check the RTT sample and
// that the acked packets are cleared.
func TestConnFrameHandler_OnAck_UpdatesRTT(t *testing.T) {
	base := time.Unix(200, 0)
	tm := base
	c := &Conn{now: func() time.Time { return tm }}
	c.sent[spaceApp].onSent(10, base, true, nil)
	c.sent[spaceApp].onSent(11, base, true, nil)
	c.sendPN[spaceApp] = 12 // sent through packet 11 (§13.1 never-sent gate)
	tm = base.Add(30 * ms)
	h := &connFrameHandler{c: c, space: spaceApp}

	ackErr := h.OnAck(11, 0, 1) // acks [10, 11]

	require.NoError(t, ackErr, "OnAck over packets that were actually sent")
	assert.Truef(t, c.rtt.haveSample, "rtt = %+v, want a sample recorded", c.rtt)
	assert.Equalf(t, 30*ms, c.rtt.latestRTT, "rtt = %+v, want latestRTT=30ms", c.rtt)
	assert.Emptyf(t, c.sent[spaceApp].packets,
		"acked packets not cleared: %d left", len(c.sent[spaceApp].packets))
}

// TestConnFrameHandler_OnAckRange_Gap verifies an ACK with a gap acknowledges
// the two ranges and leaves the gapped packet in flight (RFC 9000 §19.3).
func TestConnFrameHandler_OnAckRange_Gap(t *testing.T) {
	base := time.Unix(300, 0)
	c := &Conn{now: func() time.Time { return base }}
	for pn := uint64(8); pn <= 11; pn++ {
		c.sent[spaceApp].onSent(pn, base, true, nil)
	}
	c.sendPN[spaceApp] = 12 // sent through packet 11 (§13.1 never-sent gate)
	h := &connFrameHandler{c: c, space: spaceApp}

	// Largest 11, first range 0 (just 11); then gap 0 (skip 10), length 1 (8..9).
	ackErr := h.OnAck(11, 0, 0)
	rangeErr := h.OnAckRange(0, 1)

	require.NoError(t, ackErr, "OnAck for the first range")
	require.NoError(t, rangeErr, "OnAckRange for the second range")
	left := c.sent[spaceApp].packets
	assert.Lenf(t, left, 1, "in-flight = %d, want 1 (the gapped pn 10)", len(left))
	assert.Containsf(t, left, uint64(10), "pn 10 should remain in flight, got %v", left)
}

// TestSentSpace_Ack_LargestNotInFlightYieldsNoSample pins the haveLargest guard
// in sentSpace.ack, which nothing in the suite exercised.
//
// An RTT sample is taken from the SEND TIME of the largest acknowledged packet
// (RFC 9002 §5.1). When that packet number is not in our sent map the send time
// is the zero time.Time, and the caller computes now - 0001-01-01 as the round
// trip: roughly two millennia fed into rttStats.update, poisoning minRTT,
// smoothedRTT, rttvar, the PTO period and the persistent-congestion duration for
// the rest of the connection. Dropping haveLargest from the hasRTT expression
// left the whole suite green — TestSentSpace_Ack covers the neighbouring case (a
// DUPLICATE largest yields no sample) and that one is caught, so it is
// specifically the absent-largest arm that was missing. #847.
//
// pn 7 outside the map is not contrived: detectLost deletes a packet it declares
// lost, and a loss declaration can be spurious, so a later ACK naming it is
// exactly what a reordered path produces.
func TestSentSpace_Ack_LargestNotInFlightYieldsNoSample(t *testing.T) {
	base := time.Unix(400, 0)
	var s sentSpace
	s.onSent(5, base, true, nil)
	s.onSent(6, base.Add(10*ms), true, nil)
	require.NotContains(t, s.packets, uint64(7),
		"the fixture's premise is that pn 7 is NOT in the sent map")

	sendTime, hasRTT := s.ack(nil, 5, 7) // largest = 7, never in our map

	assert.Falsef(t, hasRTT,
		"ack reported an RTT sample for a largest packet we do not hold; its send time "+
			"is the zero Time, so the caller measures now minus year 1 and poisons "+
			"min_rtt, smoothed_rtt, the PTO period and persistent congestion")
	assert.Truef(t, sendTime.IsZero(),
		"send time = %v, want the zero Time — the guard, not the value, is what must "+
			"stop the caller using it", sendTime)
	assert.Emptyf(t, s.packets,
		"packets in [5,7] must still be removed from flight, got %d left", len(s.packets))
}

// TestConn_OnAck_LargestNotInFlightLeavesRTTUntouched is the same property
// through the frame handler, so the §13.1 never-sent gate is shown NOT to be the
// thing rejecting it: sendPN is 8, which makes an ACK of packet 7 legitimate.
// Two mechanisms could produce "no RTT sample" here and this pins the intended
// one. #847.
func TestConn_OnAck_LargestNotInFlightLeavesRTTUntouched(t *testing.T) {
	base := time.Unix(500, 0)
	tm := base
	c := &Conn{now: func() time.Time { return tm }}
	c.sendPN[spaceApp] = 8 // we did send packets 0..7, so §13.1 admits this ACK
	c.sent[spaceApp].onSent(5, base, true, nil)
	c.sent[spaceApp].onSent(6, base, true, nil)
	// pn 7 was declared lost by an earlier episode and dropped from the map.
	c.sent[spaceApp].largestAckedPN, c.sent[spaceApp].haveLargestAcked = 4, true
	tm = base.Add(30 * ms)
	h := &connFrameHandler{c: c, space: spaceApp}

	err := h.OnAck(7, 0, 2) // acks [5, 7]; 7 is newly largest but not in flight

	require.NoErrorf(t, err, "OnAck of packets we did send must not be a §13.1 violation: %v", err)
	assert.Falsef(t, c.rtt.haveSample,
		"an RTT sample was taken from a packet not in flight: latestRTT = %v, "+
			"smoothed = %v — a sample of that size never leaves the estimator",
		c.rtt.latestRTT, c.rtt.smoothedRTT)
	assert.Zerof(t, c.rtt.latestRTT,
		"latestRTT = %v, want 0 (untouched)", c.rtt.latestRTT)
}
