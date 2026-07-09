package quic

import (
	"bytes"
	"testing"
)

// pathCapture records the data of a PATH_RESPONSE frame.
type pathCapture struct {
	nopFrameHandler
	got  bool
	data [8]byte
}

func (h *pathCapture) OnPathResponse(d *[8]byte) error { h.got, h.data = true, *d; return nil }

// TestConformance_RFC9000_Sec822_PathChallengeEchoed checks that a received
// PATH_CHALLENGE queues a PATH_RESPONSE echoing its data (RFC 9000 §8.2.2).
func TestConformance_RFC9000_Sec822_PathChallengeEchoed(t *testing.T) {
	c := &Conn{}
	h := &connFrameHandler{c: c}
	challenge := &[8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	if err := h.OnPathChallenge(challenge); err != nil {
		t.Fatal(err)
	}
	if !h.ackEliciting {
		t.Fatal("a PATH_CHALLENGE is ack-eliciting")
	}
	if want := AppendPathResponse(nil, *challenge); !bytes.Equal(c.pendingCtrl, want) {
		t.Fatalf("pendingCtrl = %x, want a PATH_RESPONSE echoing the challenge %x", c.pendingCtrl, want)
	}
}

// TestConn_PathChallenge_FloodBounded checks that a flood of PATH_CHALLENGE
// frames cannot grow the control buffer past roughly one datagram — a dropped
// response just prompts the peer to re-challenge (RFC 9000 §8.2.2).
func TestConn_PathChallenge_FloodBounded(t *testing.T) {
	c := &Conn{}
	h := &connFrameHandler{c: c}
	for i := 0; i < 100000; i++ {
		if err := h.OnPathChallenge(&[8]byte{byte(i), byte(i >> 8)}); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.pendingCtrl) == 0 {
		t.Fatal("at least one PATH_RESPONSE should be queued")
	}
	if len(c.pendingCtrl) > maxPendingCtrl+16 { // + one frame's slack past the cap
		t.Fatalf("pendingCtrl grew to %d bytes under a flood, want ≈ %d", len(c.pendingCtrl), maxPendingCtrl)
	}
}

// TestConn_PathChallenge_SentByFlush checks that the queued PATH_RESPONSE is
// actually written on the next flush, carrying the echoed challenge data.
func TestConn_PathChallenge_SentByFlush(t *testing.T) {
	sealer, opener := closeTestSealerOpener(t, 0x82)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("pathtst0"), oneRTTSealer: sealer}
	h := &connFrameHandler{c: c}
	challenge := &[8]byte{9, 8, 7, 6, 5, 4, 3, 2}
	if err := h.OnPathChallenge(challenge); err != nil {
		t.Fatal(err)
	}
	if err := c.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1 (the PATH_RESPONSE)", len(pc.writes))
	}
	hdr, err := ParseHeader(pc.writes[0], len(c.dcid))
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	_, _, payload, err := opener.Open(pc.writes[0], hdr.PNOffset, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var resp pathCapture
	if err := ParseFrames(payload, &resp); err != nil {
		t.Fatalf("ParseFrames: %v", err)
	}
	if !resp.got {
		t.Fatal("no PATH_RESPONSE on the wire")
	}
	if resp.data != *challenge {
		t.Fatalf("PATH_RESPONSE data = %x, want %x", resp.data, *challenge)
	}
}
