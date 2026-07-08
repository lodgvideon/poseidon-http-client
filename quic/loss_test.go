package quic

import (
	"testing"
	"time"
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

func streamFrame(streamID, offset uint64, data string) []retransFrame {
	return []retransFrame{{kind: retransStream, streamID: streamID, offset: offset, data: []byte(data)}}
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
	c.sent[spaceApp].ack(3, 3) // acknowledge the largest; sets largestAckedPN=3
	c.detectLost(spaceApp)

	if len(c.retransQueue[spaceApp]) != 1 || c.retransQueue[spaceApp][0].offset != 0 {
		t.Fatalf("retransQueue = %+v, want just pn 0", c.retransQueue[spaceApp])
	}
	if _, ok := c.sent[spaceApp].packets[0]; ok {
		t.Fatal("pn 0 should be removed from flight")
	}
	if _, ok := c.sent[spaceApp].packets[1]; !ok {
		t.Fatal("pn 1 is within the packet threshold and must stay in flight")
	}
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
	c.sent[spaceApp].ack(1, 1) // largestAckedPN=1; removes pn 1 (mirrors real order)
	c.detectLost(spaceApp)

	if len(c.retransQueue[spaceApp]) != 1 {
		t.Fatalf("pn 0 should be lost by the time threshold; queue = %+v", c.retransQueue[spaceApp])
	}
}

func TestConn_DetectLost_NoLossWithinThresholds(t *testing.T) {
	base := time.Unix(700, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.update(20*ms, 0)
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "x")) // sent "now"
	c.sent[spaceApp].onSent(1, base, true, nil)
	c.sent[spaceApp].ack(1, 1)
	c.detectLost(spaceApp)

	if len(c.retransQueue[spaceApp]) != 0 {
		t.Fatalf("no loss expected within both thresholds, got %+v", c.retransQueue[spaceApp])
	}
	if _, ok := c.sent[spaceApp].packets[0]; !ok {
		t.Fatal("pn 0 should remain in flight")
	}
}

func TestRTTStats_LossDelay(t *testing.T) {
	var r rttStats
	if r.lossDelay() != kGranularity {
		t.Fatalf("zero-RTT lossDelay = %v, want %v (floor)", r.lossDelay(), kGranularity)
	}
	r.update(40*ms, 0)
	if want := 40 * ms * 9 / 8; r.lossDelay() != want {
		t.Fatalf("lossDelay = %v, want %v", r.lossDelay(), want)
	}
}

// TestConn_Retransmit_CryptoResendsBytesAtOffset checks a lost Initial CRYPTO
// frame is resent at its original offset, and that the retransmit datagram is
// padded to the RFC 9000 §14.1 1200-byte minimum.
func TestConn_Retransmit_CryptoResendsBytesAtOffset(t *testing.T) {
	dcid := []byte("losstst0")
	ck, _ := InitialKeys(dcid)
	sealer, err := NewSealer(ck)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(ck)
	if err != nil {
		t.Fatal(err)
	}
	pc := &capturePC{}
	c := &Conn{pc: pc, dcid: dcid, initialSealer: sealer}
	c.retransQueue[spaceInitial] = []retransFrame{{kind: retransCrypto, offset: 100, data: []byte("clienthello-bytes")}}

	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) != 1 {
		t.Fatalf("expected 1 retransmit datagram, got %d", len(pc.pkts))
	}
	if len(pc.pkts[0]) < InitialDatagramMinSize {
		t.Fatalf("Initial retransmit datagram = %d bytes, want >= %d (§14.1)", len(pc.pkts[0]), InitialDatagramMinSize)
	}
	hdr, err := ParseHeader(pc.pkts[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _, payload, err := opener.Open(pc.pkts[0], hdr.PNOffset, 0)
	if err != nil {
		t.Fatal(err)
	}
	var col cryptoCollector
	if err := ParseFrames(payload, &col); err != nil {
		t.Fatal(err)
	}
	if !col.seen || col.offset != 100 || string(col.data) != "clienthello-bytes" {
		t.Fatalf("retransmitted CRYPTO = {off:%d data:%q}, want off 100 with original bytes", col.offset, col.data)
	}
}

// TestConn_Retransmit_StreamResendsBytesAtOffsetAndFin checks a lost STREAM
// frame is resent at its original offset with FIN, without re-advancing the send
// accounting.
func TestConn_Retransmit_StreamResendsBytesAtOffsetAndFin(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 1<<20, 1<<20)
	if _, err := s.Send([]byte("request body"), true); err != nil {
		t.Fatal(err)
	}
	beforeOffset, beforeConn := s.sendOffset, c.connSent

	pc.pkts = nil              // drop the original send; capture only the retransmit
	c.sent[spaceApp].ack(3, 3) // fake ACK of a higher pn -> largestAckedPN=3
	c.detectLost(spaceApp)     // pn 0 lost by packet threshold
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) != 1 {
		t.Fatalf("expected 1 retransmit datagram, got %d", len(pc.pkts))
	}
	h := collectFrames(t, c, op, pc)
	if len(h.streams) != 1 {
		t.Fatalf("expected 1 resent STREAM frame, got %d", len(h.streams))
	}
	f := h.streams[0]
	if f.offset != 0 || !f.fin || string(f.data) != "request body" {
		t.Fatalf("resent STREAM = {off:%d fin:%v data:%q}, want off 0 fin body", f.offset, f.fin, f.data)
	}
	if s.sendOffset != beforeOffset || c.connSent != beforeConn {
		t.Fatalf("retransmit re-advanced accounting: sendOffset %d->%d connSent %d->%d",
			beforeOffset, s.sendOffset, beforeConn, c.connSent)
	}
}

func TestConn_Retransmit_AckedPacketNotResent(t *testing.T) {
	base := time.Unix(900, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "x"))
	c.sent[spaceApp].ack(0, 5) // acknowledges pn 0 (removed) and sets largestAckedPN=5
	c.detectLost(spaceApp)
	if len(c.retransQueue[spaceApp]) != 0 {
		t.Fatalf("an acknowledged packet must never be queued for resend, got %+v", c.retransQueue[spaceApp])
	}
}

// TestConn_AckOnlyPacketNotRetransmittable: a lost ACK-only packet (no
// retransmittable frames) is removed from flight but queues nothing (§13.3).
func TestConn_AckOnlyPacketNotRetransmittable(t *testing.T) {
	base := time.Unix(1000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.sent[spaceApp].onSent(0, base, false, nil)
	c.sent[spaceApp].ack(3, 3)
	c.detectLost(spaceApp)
	if len(c.retransQueue[spaceApp]) != 0 {
		t.Fatal("a lost ACK-only packet must not be retransmitted")
	}
	if _, ok := c.sent[spaceApp].packets[0]; ok {
		t.Fatal("the lost packet should still be removed from flight")
	}
}
