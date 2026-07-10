package quic

import (
	"testing"
	"time"
)

// TestConformance_RFC9002_Sec6221_PTOArmedWithEmptyFlight checks that before the
// handshake completes the probe timer is armed even with no ack-eliciting packet
// in flight, and that this no longer holds once the handshake completes (RFC 9002
// §6.2.2.1).
func TestConformance_RFC9002_Sec6221_PTOArmedWithEmptyFlight(t *testing.T) {
	base := time.Unix(4000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.smoothedRTT, c.rtt.rttvar, c.rtt.haveSample = 40*time.Millisecond, 5*time.Millisecond, true

	if !c.handshakeAntiDeadlock() {
		t.Fatal("handshakeAntiDeadlock should hold before the handshake completes")
	}
	dl, isLoss := c.lossDetectionDeadline()
	if isLoss {
		t.Fatal("no time-threshold loss is pending")
	}
	if want := base.Add(c.ptoPeriod()); !dl.Equal(want) {
		t.Fatalf("deadline = %v, want the PTO %v (armed with an empty flight)", dl, want)
	}

	c.handshakeComplete = true
	if c.handshakeAntiDeadlock() {
		t.Fatal("handshakeAntiDeadlock should be false once the handshake completes")
	}
}

// TestConformance_RFC9002_Sec6221_HandshakeProbeSendsPing checks that a PTO during
// the handshake with an empty flight sends a Handshake-space PING probe (not an
// application-space probe), to unblock an anti-amplification-limited server
// (RFC 9002 §6.2.2.1).
func TestConformance_RFC9002_Sec6221_HandshakeProbeSendsPing(t *testing.T) {
	dcid := []byte("hspto000")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	pc := &closePC{}
	// Handshake keys present, handshake not complete, nothing in flight.
	c := &Conn{pc: pc, dcid: dcid, handshakeSealer: sealer, now: func() time.Time { return time.Unix(4, 0) }}

	c.onPTO()
	if !c.handshakeProbe {
		t.Fatal("a PTO during the handshake with empty flight should mark a handshake probe")
	}
	if c.probePending {
		t.Fatal("the application-space probe must not be used during the handshake")
	}

	if err := c.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1 handshake PING probe", len(pc.writes))
	}
	if c.handshakeProbe {
		t.Fatal("handshakeProbe should be cleared once the PING is sent")
	}

	hdr, err := ParseHeader(pc.writes[0], len(c.dcid))
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != PacketHandshake {
		t.Fatalf("probe packet type = %v, want Handshake (§6.2.2.1)", hdr.Type)
	}
	_, _, payload, err := opener.Open(pc.writes[0], hdr.PNOffset, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var h pingCapture
	if err := ParseFrames(payload, &h); err != nil {
		t.Fatal(err)
	}
	if !h.got {
		t.Fatal("the handshake probe packet does not carry a PING")
	}
}

// TestConformance_RFC9002_Sec6221_InitialProbePadded checks that when the client
// holds only Initial keys, the anti-deadlock probe is an Initial PING in a
// datagram padded to at least 1200 bytes (RFC 9002 §6.2.2.1, RFC 9000 §14.1).
func TestConformance_RFC9002_Sec6221_InitialProbePadded(t *testing.T) {
	dcid := []byte("inpto000")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	pc := &closePC{}
	// Only Initial keys (no Handshake keys yet), handshake not complete.
	c := &Conn{pc: pc, dcid: dcid, initialSealer: sealer, now: func() time.Time { return time.Unix(4, 0) }}

	c.onPTO()
	if !c.handshakeProbe {
		t.Fatal("a PTO during the handshake with empty flight should mark a handshake probe")
	}
	if err := c.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1 Initial PING probe", len(pc.writes))
	}
	if len(pc.writes[0]) < InitialDatagramMinSize {
		t.Fatalf("Initial probe datagram = %d bytes, want ≥ %d (§14.1)", len(pc.writes[0]), InitialDatagramMinSize)
	}

	hdr, err := ParseHeader(pc.writes[0], len(c.dcid))
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Type != PacketInitial {
		t.Fatalf("probe packet type = %v, want Initial", hdr.Type)
	}
	_, _, payload, err := opener.Open(pc.writes[0], hdr.PNOffset, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var h pingCapture
	if err := ParseFrames(payload, &h); err != nil {
		t.Fatal(err)
	}
	if !h.got {
		t.Fatal("the Initial probe packet does not carry a PING")
	}
}
