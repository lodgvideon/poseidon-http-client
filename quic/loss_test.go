package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cryptoCollector captures the CRYPTO frame decoded off a retransmitted packet.
type cryptoCollector struct {
	nopFrameHandler
	offset uint64
	data   []byte
	seen   bool
}

func (h *cryptoCollector) OnCrypto(offset uint64, data []byte) error {
	h.offset = offset
	h.data = append([]byte(nil), data...)
	h.seen = true
	return nil
}

func streamFrame(streamID, offset uint64, data string) *retransFrame {
	return &retransFrame{kind: retransStream, streamID: streamID, offset: offset, data: []byte(data)}
}

// TestConformance_RFC9002_Sec611_PacketThresholdLoss: with pn 3 acknowledged,
// pn 0 is lost (3 >= 0+3) but pn 1 and 2 are not (§6.1.1). Time threshold inert
// (all sent "now").
func TestConformance_RFC9002_Sec611_PacketThresholdLoss(t *testing.T) {
	base := time.Unix(500, 0)
	c := &Conn{now: func() time.Time { return base }}
	for pn := uint64(0); pn <= 3; pn++ {
		c.sent[spaceApp].onSent(pn, base, true, streamFrame(0, pn, "x"))
	}
	c.sent[spaceApp].ack(c, 3, 3) // acknowledge the largest; sets largestAckedPN=3

	c.detectLost(spaceApp)

	require.Lenf(t, c.retransQueue[spaceApp], 1,
		"retransQueue = %+v, want just pn 0", c.retransQueue[spaceApp])
	assert.Zerof(t, c.retransQueue[spaceApp][0].offset,
		"retransQueue = %+v, want just pn 0", c.retransQueue[spaceApp])
	assert.NotContains(t, c.sent[spaceApp].packets, uint64(0), "pn 0 should be removed from flight")
	assert.Contains(t, c.sent[spaceApp].packets, uint64(1),
		"pn 1 is within the packet threshold and must stay in flight")
}

// TestConformance_RFC9002_Sec612_TimeThresholdLoss: an old packet is lost by the
// time threshold even though the packet-number gap is below the threshold
// (§6.1.2).
func TestConformance_RFC9002_Sec612_TimeThresholdLoss(t *testing.T) {
	base := time.Unix(600, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.update(20*ms, 0) // lossDelay = 20ms*9/8 = 22.5ms
	c.sent[spaceApp].onSent(0, base.Add(-50*ms), true, streamFrame(0, 0, "old"))
	c.sent[spaceApp].onSent(1, base.Add(-50*ms), true, nil)
	c.sent[spaceApp].ack(c, 1, 1) // largestAckedPN=1; removes pn 1 (mirrors real order)

	c.detectLost(spaceApp)

	assert.Lenf(t, c.retransQueue[spaceApp], 1,
		"pn 0 should be lost by the time threshold; queue = %+v", c.retransQueue[spaceApp])
}

func TestConn_DetectLost_NoLossWithinThresholds(t *testing.T) {
	base := time.Unix(700, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.update(20*ms, 0)
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "x")) // sent "now"
	c.sent[spaceApp].onSent(1, base, true, nil)
	c.sent[spaceApp].ack(c, 1, 1)

	c.detectLost(spaceApp)

	assert.Emptyf(t, c.retransQueue[spaceApp],
		"no loss expected within both thresholds, got %+v", c.retransQueue[spaceApp])
	assert.Contains(t, c.sent[spaceApp].packets, uint64(0), "pn 0 should remain in flight")
}

// TestConn_DetectLost_PrunesAckedElicitWithoutLoss: a connection that never
// loses a packet still prunes recorded acknowledgement times down to the
// in-flight window, so ackedElicit cannot grow without bound over a long clean
// run (the load-generator target case). Before the fix pruneAcked ran only after
// the no-loss early return, so a lossless ACK left the slice growing forever.
func TestConn_DetectLost_PrunesAckedElicitWithoutLoss(t *testing.T) {
	base := time.Unix(800, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.update(20*ms, 0)
	// pn 5 sent "now" stays in flight (the oldest unacked); pn 6 is acknowledged.
	c.sent[spaceApp].onSent(5, base, true, streamFrame(0, 5, "x"))
	c.sent[spaceApp].onSent(6, base, true, nil)
	c.sent[spaceApp].ack(c, 6, 6) // largestAckedPN=6; pn 5 stays in flight, no loss
	// Acknowledgement times from a long lossless run: those at or before the oldest
	// in-flight packet (base) must be dropped, newer ones kept.
	c.sent[spaceApp].ackedElicit = []time.Time{
		base.Add(-time.Second), // older → pruned
		base,                   // equal → pruned (prune keeps strictly-after)
		base.Add(time.Second),  // newer → kept
	}

	c.detectLost(spaceApp)

	assert.Emptyf(t, c.retransQueue[spaceApp],
		"no loss expected within thresholds; retransQueue has %d frames", len(c.retransQueue[spaceApp]))
	got := c.sent[spaceApp].ackedElicit
	require.Lenf(t, got, 1,
		"ackedElicit = %v, want only base+1s (pruned to the in-flight window)", got)
	assert.Truef(t, got[0].Equal(base.Add(time.Second)),
		"ackedElicit = %v, want only base+1s (pruned to the in-flight window)", got)
}

func TestRTTStats_LossDelay(t *testing.T) {
	var r rttStats

	zeroRTT := r.lossDelay()
	r.update(40*ms, 0)
	sampled := r.lossDelay()

	assert.Equalf(t, kGranularity, zeroRTT, "zero-RTT lossDelay = %v, want %v (floor)", zeroRTT, kGranularity)
	assert.Equalf(t, 40*ms*9/8, sampled, "lossDelay = %v, want %v", sampled, 40*ms*9/8)
}

// TestConn_Retransmit_CryptoResendsBytesAtOffset checks a lost Initial CRYPTO
// frame is resent at its original offset, and that the retransmit datagram is
// padded to the RFC 9000 §14.1 1200-byte minimum.
func TestConn_Retransmit_CryptoResendsBytesAtOffset(t *testing.T) {
	dcid := []byte("losstst0")
	ck, _ := InitialKeys(dcid)
	sealer, err := NewSealer(ck)
	require.NoError(t, err, "NewSealer for the Initial keys")
	opener, err := NewOpener(ck)
	require.NoError(t, err, "NewOpener to decrypt the retransmit")
	pc := &capturePC{}
	c := &Conn{pc: pc, dcid: dcid, initialSealer: sealer}
	c.retransQueue[spaceInitial] = []retransFrame{{kind: retransCrypto, offset: 100, data: []byte("clienthello-bytes")}}

	err = c.flush()

	require.NoError(t, err, "flush the queued CRYPTO retransmit")
	require.Lenf(t, pc.pkts, 1, "expected 1 retransmit datagram, got %d", len(pc.pkts))
	assert.GreaterOrEqualf(t, len(pc.pkts[0]), InitialDatagramMinSize,
		"Initial retransmit datagram = %d bytes, want >= %d (§14.1)", len(pc.pkts[0]), InitialDatagramMinSize)
	hdr, err := ParseHeader(pc.pkts[0], 0)
	require.NoError(t, err, "ParseHeader on the retransmit")
	_, _, payload, err := opener.Open(pc.pkts[0], hdr.PNOffset, 0)
	require.NoError(t, err, "Open the retransmit")
	var col cryptoCollector
	require.NoError(t, ParseFrames(payload, &col), "ParseFrames on the retransmit payload")
	assert.Truef(t, col.seen && col.offset == 100 && string(col.data) == "clienthello-bytes",
		"retransmitted CRYPTO = {off:%d data:%q}, want off 100 with original bytes", col.offset, col.data)
}

// TestConn_Retransmit_StreamResendsBytesAtOffsetAndFin checks a lost STREAM
// frame is resent at its original offset with FIN, without re-advancing the send
// accounting.
func TestConn_Retransmit_StreamResendsBytesAtOffsetAndFin(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 1<<20, 1<<20)
	_, err := s.Send([]byte("request body"), true)
	require.NoError(t, err, "the original Send whose packet is then declared lost")
	beforeOffset, beforeConn := s.sendOffset, c.connSent
	pc.pkts = nil                 // drop the original send; capture only the retransmit
	c.sent[spaceApp].ack(c, 3, 3) // fake ACK of a higher pn -> largestAckedPN=3
	c.detectLost(spaceApp)        // pn 0 lost by packet threshold

	err = c.flush()

	require.NoError(t, err, "flush the retransmit")
	require.Lenf(t, pc.pkts, 1, "expected 1 retransmit datagram, got %d", len(pc.pkts))
	h := collectFrames(t, c, op, pc)
	require.Lenf(t, h.streams, 1, "expected 1 resent STREAM frame, got %d", len(h.streams))
	f := h.streams[0]
	assert.Truef(t, f.offset == 0 && f.fin && string(f.data) == "request body",
		"resent STREAM = {off:%d fin:%v data:%q}, want off 0 fin body", f.offset, f.fin, f.data)
	assert.Truef(t, s.sendOffset == beforeOffset && c.connSent == beforeConn,
		"retransmit re-advanced accounting: sendOffset %d->%d connSent %d->%d",
		beforeOffset, s.sendOffset, beforeConn, c.connSent)
}

func TestConn_Retransmit_AckedPacketNotResent(t *testing.T) {
	base := time.Unix(900, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "x"))
	c.sent[spaceApp].ack(c, 0, 5) // acknowledges pn 0 (removed) and sets largestAckedPN=5

	c.detectLost(spaceApp)

	assert.Emptyf(t, c.retransQueue[spaceApp],
		"an acknowledged packet must never be queued for resend, got %+v", c.retransQueue[spaceApp])
}

// TestConn_AckOnlyPacketNotRetransmittable: a lost ACK-only packet (no
// retransmittable frames) is removed from flight but queues nothing (§13.3).
func TestConn_AckOnlyPacketNotRetransmittable(t *testing.T) {
	base := time.Unix(1000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.sent[spaceApp].onSent(0, base, false, nil)
	c.sent[spaceApp].ack(c, 3, 3)

	c.detectLost(spaceApp)

	assert.Empty(t, c.retransQueue[spaceApp], "a lost ACK-only packet must not be retransmitted")
	assert.NotContains(t, c.sent[spaceApp].packets, uint64(0),
		"the lost packet should still be removed from flight")
}

// TestConformance_RFC9002_Sec61_OnlyPacketsBeforeAnAckedOneAreLost pins
// detectLost's eligibility precondition, which nothing in the suite exercised.
//
// RFC 9002 §6.1 makes it the FIRST of the two conditions a packet must meet to be
// declared lost: "The packet is unacknowledged, in flight, and was sent prior to
// an acknowledged packet." Without it the time threshold alone fires on any
// merely-old packet — including one the peer has had no opportunity to
// acknowledge — so an unlucky RTT sample turns every packet still in flight into
// a spurious retransmission and a congestion-window cut.
//
// TestConformance_RFC9002_Sec612_EarliestLossTime is the only other fixture with
// a packet above largestAckedPN, and it sends that packet at "now", so the time
// threshold does not fire for it either way. Deleting the guard left the whole
// suite green. #840.
//
// The fixture makes the guard the only thing that can spare pn 5: it is above the
// largest acknowledged AND older than the loss delay, so the §6.1.2 time
// threshold would otherwise take it.
func TestConformance_RFC9002_Sec61_OnlyPacketsBeforeAnAckedOneAreLost(t *testing.T) {
	base := time.Unix(900, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.update(20*ms, 0) // lossDelay = 20ms*9/8 = 22.5ms
	c.sent[spaceApp].onSent(0, base.Add(-50*ms), true, streamFrame(0, 0, "old"))
	c.sent[spaceApp].onSent(1, base.Add(-50*ms), true, nil)
	c.sent[spaceApp].ack(c, 1, 1) // largestAckedPN = 1
	// Above the largest acknowledged, and 50ms old — past the 22.5ms threshold.
	c.sent[spaceApp].onSent(5, base.Add(-50*ms), true, streamFrame(0, 5, "newer"))
	require.Contains(t, c.sent[spaceApp].packets, uint64(5),
		"the fixture must have pn 5 in flight, or there is nothing for the guard to spare")

	c.detectLost(spaceApp)

	assert.Containsf(t, c.sent[spaceApp].packets, uint64(5),
		"pn 5 left flight: it is ABOVE the largest acknowledged, so §6.1's first "+
			"condition is unmet and no threshold may declare it lost — the peer has "+
			"not had the chance to acknowledge it")
	require.Lenf(t, c.retransQueue[spaceApp], 1,
		"retransQueue = %+v, want only pn 0 — pn 5 must not be retransmitted",
		c.retransQueue[spaceApp])
	assert.Zerof(t, c.retransQueue[spaceApp][0].offset,
		"the queued frame is offset %d, want pn 0's offset 0",
		c.retransQueue[spaceApp][0].offset)
	assert.NotContains(t, c.sent[spaceApp].packets, uint64(0),
		"pn 0 is below the largest acknowledged and past the time threshold: it must be lost")
}
