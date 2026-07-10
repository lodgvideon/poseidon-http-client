package quic

import "testing"

// TestConformance_RFC9000_Sec822_PathResponsePaddedTo1200 checks that the datagram
// carrying the client's PATH_RESPONSE (queued in answer to a received
// PATH_CHALLENGE) is expanded to at least 1200 bytes, while an ordinary
// control-frame datagram is not (RFC 9000 §8.2.2).
func TestConformance_RFC9000_Sec822_PathResponsePaddedTo1200(t *testing.T) {
	newConn := func() (*Conn, *capturePC) {
		dcid := []byte("pathtst0")
		keys, _ := InitialKeys(dcid)
		sealer, _ := NewSealer(keys)
		pc := &capturePC{}
		c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true}
		c.keys.OneRTT, _ = NewOpener(keys)
		return c, pc
	}

	// A PATH_RESPONSE datagram is padded to at least 1200 bytes.
	c, pc := newConn()
	var data [8]byte
	copy(data[:], "challeng")
	if err := (&connFrameHandler{c: c, space: spaceApp}).OnPathChallenge(&data); err != nil {
		t.Fatal(err)
	}
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) != 1 {
		t.Fatalf("wrote %d datagrams, want 1", len(pc.pkts))
	}
	if len(pc.pkts[0]) < InitialDatagramMinSize {
		t.Fatalf("PATH_RESPONSE datagram = %d bytes, want >= %d (§8.2.2)", len(pc.pkts[0]), InitialDatagramMinSize)
	}
	if c.pathRespPending {
		t.Fatal("pathRespPending should be cleared once the PATH_RESPONSE is sent")
	}

	// An ordinary control-frame datagram (a MAX_DATA grant, no PATH_RESPONSE) is not
	// expanded, so the padding is specific to path validation.
	c2, pc2 := newConn()
	c2.pendingCtrl = AppendMaxData(nil, 1000)
	if err := c2.flush(); err != nil {
		t.Fatal(err)
	}
	if len(pc2.pkts) != 1 {
		t.Fatalf("wrote %d datagrams, want 1", len(pc2.pkts))
	}
	if len(pc2.pkts[0]) >= InitialDatagramMinSize {
		t.Fatalf("a plain MAX_DATA datagram = %d bytes, should not be padded to 1200", len(pc2.pkts[0]))
	}
}
