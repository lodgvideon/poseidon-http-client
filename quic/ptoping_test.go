package quic

import (
	"testing"
	"time"
)

// pingCapture records whether a PING frame was parsed.
type pingCapture struct {
	nopFrameHandler
	got bool
}

func (h *pingCapture) OnPing() error { h.got = true; return nil }

// TestConformance_RFC9002_Sec624_PTOSendsPingWhenNoData checks that a probe
// timeout with only a frameless ack-eliciting packet in flight (e.g. a lone
// *_BLOCKED) still sends an ack-eliciting PING probe (RFC 9002 §6.2.4).
func TestConformance_RFC9002_Sec624_PTOSendsPingWhenNoData(t *testing.T) {
	dcid := []byte("ptoping0")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true, handshakeConfirmed: true, now: func() time.Time { return time.Unix(3, 0) }}
	c.keys.OneRTT = opener
	c.sent[spaceApp].onSent(0, c.clock(), true, nil) // ack-eliciting, no retransmittable frames

	c.onPTO()
	if !c.probePending {
		t.Fatal("a PTO with only a frameless ack-eliciting packet in flight should mark a probe pending")
	}
	if err := c.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1 PING probe", len(pc.writes))
	}
	if c.probePending {
		t.Fatal("probePending should be cleared once the PING is sent")
	}

	hdr, err := ParseHeader(pc.writes[0], len(c.dcid))
	if err != nil {
		t.Fatal(err)
	}
	_, _, payload, err := opener.Open(pc.writes[0], hdr.PNOffset, 0)
	if err != nil {
		t.Fatal(err)
	}
	var h pingCapture
	if err := ParseFrames(payload, &h); err != nil {
		t.Fatal(err)
	}
	if !h.got {
		t.Fatal("the probe packet does not carry a PING")
	}
}

// TestConn_PTO_NoPingWhenDataToResend checks that a PTO resends the oldest
// unacknowledged frames when there are some — and does not also mark a bare PING.
func TestConn_PTO_NoPingWhenDataToResend(t *testing.T) {
	c := &Conn{now: func() time.Time { return time.Unix(3, 0) }, handshakeConfirmed: true}
	c.sent[spaceApp].onSent(0, c.clock(), true, &retransFrame{kind: retransStream, streamID: 0, data: []byte("x")})
	c.onPTO()
	if c.probePending {
		t.Fatal("with retransmittable data in flight, the probe resends it — no bare PING")
	}
	if len(c.retransQueue[spaceApp]) == 0 {
		t.Fatal("the oldest packet's frames should be queued for resend")
	}
}

// TestConformance_RFC9002_Sec621_NoAppPTOBeforeConfirmed pins "An endpoint MUST NOT
// set a timer for the Application Data packet number space until the handshake is
// confirmed." Between TLS completion and HANDSHAKE_DONE the server may still lack
// 1-RTT read keys, so probing 1-RTT there is guaranteed-spurious retransmission.
func TestConformance_RFC9002_Sec621_NoAppPTOBeforeConfirmed(t *testing.T) {
	base := time.Unix(5, 0)
	newConn := func(confirmed bool) *Conn {
		c := &Conn{now: func() time.Time { return base }, handshakeComplete: true, handshakeConfirmed: confirmed}
		c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "oneRTT"))
		return c
	}

	c := newConn(false)
	if c.hasInFlight() {
		t.Fatal("1-RTT data in flight must not arm the probe timer before the handshake is confirmed")
	}
	c.onPTO()
	if len(c.retransQueue[spaceApp]) != 0 {
		t.Fatalf("probe queued in the Application space before confirmation: %+v", c.retransQueue[spaceApp])
	}
	if _, ok := c.sent[spaceApp].packets[0]; !ok {
		t.Fatal("the unconfirmed Application packet must stay in flight, not be probed")
	}

	// Control: once confirmed the very same state does arm and probe.
	c = newConn(true)
	if !c.hasInFlight() {
		t.Fatal("control: confirmed 1-RTT data in flight should arm the probe timer")
	}
	c.onPTO()
	if len(c.retransQueue[spaceApp]) != 1 {
		t.Fatalf("control: probe queue = %+v, want the oldest packet's frame", c.retransQueue[spaceApp])
	}
}
