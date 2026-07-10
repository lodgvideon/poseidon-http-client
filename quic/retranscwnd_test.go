package quic

import "testing"

// TestConformance_RFC9002_Sec7_RetransmitRespectsCwnd checks that a retransmission
// counts against the congestion window: flushRetransmits sends nothing while the
// window is full and drains the queue once there is room (RFC 9002 §7).
func TestConformance_RFC9002_Sec7_RetransmitRespectsCwnd(t *testing.T) {
	dcid := []byte("retrcwnd")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	newConn := func() (*Conn, *capturePC) {
		pc := &capturePC{}
		c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, cwnd: 12000, ssthresh: ^uint64(0)}
		c.keys.OneRTT, _ = NewOpener(keys)
		c.retransQueue[spaceApp] = []retransFrame{{kind: retransStream, streamID: 0, offset: 0, data: []byte("xxxx")}}
		return c, pc
	}

	// Window full: no retransmission, the queue is untouched.
	c, pc := newConn()
	c.bytesInFlight = 12000
	if err := c.flushRetransmits(spaceApp); err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) != 0 {
		t.Fatalf("wrote %d datagrams with a full window, want 0", len(pc.pkts))
	}
	if len(c.retransQueue[spaceApp]) != 1 {
		t.Fatalf("retransmit queue = %d, want it left intact", len(c.retransQueue[spaceApp]))
	}

	// Room under the window: the retransmission is sent.
	c2, pc2 := newConn()
	c2.bytesInFlight = 0
	if err := c2.flushRetransmits(spaceApp); err != nil {
		t.Fatal(err)
	}
	if len(pc2.pkts) != 1 {
		t.Fatalf("wrote %d datagrams with room under the window, want 1", len(pc2.pkts))
	}

	// A PTO exemption lets exactly one packet past a full window (RFC 9002 §6.2.4),
	// then the gate resumes and the remainder stays queued.
	c3, pc3 := newConn()
	c3.bytesInFlight = 12000
	c3.retransQueue[spaceApp] = append(c3.retransQueue[spaceApp],
		retransFrame{kind: retransStream, streamID: 0, offset: 4, data: []byte("yyyy")})
	c3.ptoExempt = true
	if err := c3.flushRetransmits(spaceApp); err != nil {
		t.Fatal(err)
	}
	if len(pc3.pkts) != 1 {
		t.Fatalf("with a PTO exemption on a full window, sent %d datagrams, want exactly 1", len(pc3.pkts))
	}
	if c3.ptoExempt {
		t.Fatal("the PTO exemption should be consumed after one packet")
	}
	if len(c3.retransQueue[spaceApp]) != 1 {
		t.Fatalf("remaining queue = %d, want 1 (only the probe went past the window)", len(c3.retransQueue[spaceApp]))
	}
}
