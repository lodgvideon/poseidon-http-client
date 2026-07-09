package quic

import (
	"testing"
	"time"
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
	c.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200})
	if c.cwnd != kInitialWindow+1200 {
		t.Fatalf("cwnd = %d, want %d (slow start += acked)", c.cwnd, kInitialWindow+1200)
	}
	if c.bytesInFlight != 2400 {
		t.Fatalf("bytesInFlight = %d, want 2400", c.bytesInFlight)
	}
}

// TestConformance_RFC9002_Sec733_CongestionAvoidance verifies the byte-accumulator
// congestion-avoidance growth (RFC 9002 §7.3.3: ~one max_datagram_size per window
// of bytes acknowledged) and, critically, that it does not freeze at a large
// window where the literal per-ack `mds*acked/cwnd` truncates to zero.
func TestConformance_RFC9002_Sec733_CongestionAvoidance(t *testing.T) {
	c := ccTestConn()
	c.cwnd, c.ssthresh = 4800, 4800 // cwnd >= ssthresh → congestion avoidance
	for i := 0; i < 4; i++ {        // one full window (4×1200) acknowledged
		c.bytesInFlight = 4800
		c.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200})
	}
	if c.cwnd != 4800+maxDatagramSize {
		t.Fatalf("cwnd = %d, want %d (+1 mds per window acked)", c.cwnd, 4800+maxDatagramSize)
	}

	// No-freeze: above mds² (~1.44 MB) the truncating formula would add 0.
	big := ccTestConn()
	big.cwnd, big.ssthresh = 2<<20, 1<<20
	start := big.cwnd
	for acked := uint64(0); acked < start; acked += 1200 {
		big.bytesInFlight = 1200
		big.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200})
	}
	if big.cwnd <= start {
		t.Fatalf("cwnd = %d did not grow past %d — avoidance froze at a high window", big.cwnd, start)
	}
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
	if c.cwnd != 10000 || c.ssthresh != 10000 {
		t.Fatalf("first loss: cwnd=%d ssthresh=%d, want 10000/10000", c.cwnd, c.ssthresh)
	}
	c.onCongestionEvent(sent.Add(-50 * time.Millisecond)) // same episode (before recoveryStart)
	if c.cwnd != 10000 {
		t.Fatalf("second loss, same episode: cwnd=%d, want 10000 (halve once)", c.cwnd)
	}
	c.onCongestionEvent(base.Add(50 * time.Millisecond)) // new episode (after recoveryStart)
	if c.cwnd != 5000 {
		t.Fatalf("new episode: cwnd=%d, want 5000", c.cwnd)
	}
}

// TestCC_GateClampedByWindow checks the send gate: grantable clamps to the space
// left in the congestion window and reports blockCong (no frame) when it is full.
func TestCC_GateClampedByWindow(t *testing.T) {
	c := &Conn{cwnd: 5000, bytesInFlight: 5000, connMax: 1 << 30}
	s := &Stream{conn: c, sendMax: 1 << 30}
	if n, b := s.grantable(2000); n != 0 || b != blockCong {
		t.Fatalf("full window: got (%d, %v), want (0, blockCong)", n, b)
	}
	c.bytesInFlight = 4000 // 1000 bytes of window left
	if n, b := s.grantable(2000); n != 1000 || b != blockNone {
		t.Fatalf("partial window: got (%d, %v), want (1000, blockNone)", n, b)
	}
}

// TestCC_PureAckNotCounted checks that non-ack-eliciting packets (pure ACKs) do
// not count toward bytes_in_flight (RFC 9002 §2 "in flight").
func TestCC_PureAckNotCounted(t *testing.T) {
	c := ccTestConn()
	c.sent[spaceApp].onSent(5, time.Unix(1000, 0), false, nil)
	c.onPacketSent(spaceApp, 5, false, 44)
	if c.bytesInFlight != 0 {
		t.Fatalf("pure-ACK packet must not be in flight, bytesInFlight=%d", c.bytesInFlight)
	}
	// An ack-eliciting packet is counted.
	c.sent[spaceApp].onSent(6, time.Unix(1000, 0), true, nil)
	c.onPacketSent(spaceApp, 6, true, 1200)
	if c.bytesInFlight != 1200 {
		t.Fatalf("ack-eliciting packet not counted, bytesInFlight=%d, want 1200", c.bytesInFlight)
	}
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
	if c.bytesInFlight != 1290 {
		t.Fatalf("bytesInFlight = %d, want 1290", c.bytesInFlight)
	}
	if !c.hasInFlight() {
		t.Fatal("hasInFlight should be true with unacked packets in flight")
	}

	c.discardSpace(spaceInitial)
	c.discardSpace(spaceHandshake)

	if c.bytesInFlight != 0 {
		t.Fatalf("after discard bytesInFlight = %d, want 0 (no phantom floor)", c.bytesInFlight)
	}
	if c.hasInFlight() {
		t.Fatal("hasInFlight should be false after discarding both spaces")
	}
	if len(c.sent[spaceInitial].packets) != 0 || len(c.sent[spaceHandshake].packets) != 0 {
		t.Fatal("sent maps should be empty after discard")
	}
	if c.keys.Initial != nil || c.keys.Handshake != nil {
		t.Fatal("discarded-space openers should be nil")
	}
}

// TestCC_DisabledSentinel checks that a hand-built Conn (cwnd == 0) leaves the
// send path unthrottled and the CC hooks inert, preserving pre-CC test behavior.
func TestCC_DisabledSentinel(t *testing.T) {
	c := &Conn{connMax: 1 << 30} // cwnd == 0
	s := &Stream{conn: c, sendMax: 1 << 30}
	n, b := s.grantable(2000)
	if b != blockNone || n == 0 {
		t.Fatalf("cwnd==0 must not gate: got (%d, %v)", n, b)
	}
	c.onPacketAcked(sentPacket{timeSent: time.Unix(1000, 0), ackEliciting: true, size: 1200})
	if c.cwnd != 0 {
		t.Fatalf("cwnd must stay 0 (disabled), got %d", c.cwnd)
	}
}
