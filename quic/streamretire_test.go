package quic

import "testing"

// newRetireConn returns a Conn with one bidi stream credit and receive flow
// control enabled, plus an open stream and a frame handler, for exercising
// stream-map retirement.
func newRetireConn(t *testing.T) (*Conn, *Stream, *connFrameHandler) {
	t.Helper()
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	return c, s, &connFrameHandler{c: c}
}

// TestConn_StreamRetire_GracefulBothDirs checks that a stream is dropped from the
// routing map once the FIN has been sent and the whole response received.
func TestConn_StreamRetire_GracefulBothDirs(t *testing.T) {
	c, s, h := newRetireConn(t)
	s.finSent = true // request FIN sent
	if err := h.OnStream(s.ID(), 0, true, []byte("resp")); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.streams[s.ID()]; ok {
		t.Fatal("stream should be retired once both directions are done")
	}
}

// TestConn_StreamRetire_KeptUntilFinSent checks that a stream whose response is
// complete but whose FIN has not been sent stays in the map.
func TestConn_StreamRetire_KeptUntilFinSent(t *testing.T) {
	c, s, h := newRetireConn(t)
	if err := h.OnStream(s.ID(), 0, true, []byte("resp")); err != nil { // recv complete, finSent false
		t.Fatal(err)
	}
	if _, ok := c.streams[s.ID()]; !ok {
		t.Fatal("stream must remain until its send side also finishes")
	}
}

// TestConn_StreamRetire_OnPeerReset checks that a stream is retired when the peer
// resets the receive side after our FIN was sent.
func TestConn_StreamRetire_OnPeerReset(t *testing.T) {
	c, s, h := newRetireConn(t)
	s.finSent = true
	if err := h.OnResetStream(s.ID(), 0, 100); err != nil { // RESET_STREAM, final size 100
		t.Fatal(err)
	}
	if _, ok := c.streams[s.ID()]; ok {
		t.Fatal("stream should be retired after FIN sent + peer RESET_STREAM")
	}
}

// TestConn_StreamRetire_KeepsResetSendStream checks that a stream whose SEND side
// we reset (finSent false) is not retired, so the §13.3 retransmit-suppression
// check can still find it by id.
func TestConn_StreamRetire_KeepsResetSendStream(t *testing.T) {
	c, s, h := newRetireConn(t)
	s.sendReset = true // we sent RESET_STREAM; finSent stays false
	if err := h.OnResetStream(s.ID(), 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.streams[s.ID()]; !ok {
		t.Fatal("a reset send-side stream must stay in the map for §13.3 suppression")
	}
}

// TestConn_StreamRetire_LateFrameIgnored checks that a STREAM frame arriving for a
// retired (client-initiated) stream is ignored, not treated as a protocol error.
func TestConn_StreamRetire_LateFrameIgnored(t *testing.T) {
	_, s, h := newRetireConn(t)
	s.finSent = true
	if err := h.OnStream(s.ID(), 0, true, []byte("resp")); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStream(s.ID(), 0, false, []byte("late retransmit")); err != nil {
		t.Fatalf("late frame for a retired stream = %v, want nil (ignored)", err)
	}
}
