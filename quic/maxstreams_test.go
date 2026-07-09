package quic

import "testing"

// TestConformance_RFC9000_Sec46_MaxStreamsRaisesLimit checks that a MAX_STREAMS
// frame raises the cumulative bidirectional stream limit so the client can open
// more streams than the peer's initial grant (RFC 9000 §4.6).
func TestConformance_RFC9000_Sec46_MaxStreamsRaisesLimit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	h := &connFrameHandler{c: c}
	if _, err := c.OpenStream(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.OpenStream(); err != ErrTooManyStreams { // capped at the initial 1
		t.Fatalf("2nd OpenStream = %v, want ErrTooManyStreams", err)
	}
	if err := h.OnMaxStreams(false, 3); err != nil { // peer raises the limit to 3
		t.Fatal(err)
	}
	if _, err := c.OpenStream(); err != nil {
		t.Fatalf("OpenStream after MAX_STREAMS(3): %v", err)
	}
	if _, err := c.OpenStream(); err != nil {
		t.Fatalf("3rd OpenStream: %v", err)
	}
	if _, err := c.OpenStream(); err != ErrTooManyStreams { // now capped at 3
		t.Fatalf("4th OpenStream = %v, want ErrTooManyStreams", err)
	}
}

// TestConformance_RFC9000_Sec46_MaxStreamsTooLarge checks that a MAX_STREAMS value
// above 2^60 is a FRAME_ENCODING_ERROR (RFC 9000 §19.11).
func TestConformance_RFC9000_Sec46_MaxStreamsTooLarge(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	h := &connFrameHandler{c: c}
	if err := h.OnMaxStreams(false, maxStreamsLimit+1); err != ErrFrameEncoding {
		t.Fatalf("MAX_STREAMS > 2^60 = %v, want ErrFrameEncoding", err)
	}
	if err := h.OnMaxStreams(true, maxStreamsLimit); err != nil { // exactly 2^60 is legal
		t.Fatalf("MAX_STREAMS == 2^60 = %v, want nil", err)
	}
}

// TestConn_MaxStreams_NonIncreasingIgnored checks that a MAX_STREAMS that does not
// increase the limit is ignored (RFC 9000 §19.11).
func TestConn_MaxStreams_NonIncreasingIgnored(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 5}}
	h := &connFrameHandler{c: c}
	if err := h.OnMaxStreams(false, 3); err != nil { // 3 < 5
		t.Fatal(err)
	}
	if c.peer.InitialMaxStreamsBidi != 5 {
		t.Fatalf("bidi limit = %d, want 5 (non-increasing MAX_STREAMS ignored)", c.peer.InitialMaxStreamsBidi)
	}
}

// TestConn_MaxStreams_Uni checks that a MAX_STREAMS with the unidirectional type
// raises the uni limit, not the bidi one.
func TestConn_MaxStreams_Uni(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1, InitialMaxStreamsUni: 1}}
	h := &connFrameHandler{c: c}
	if err := h.OnMaxStreams(true, 4); err != nil {
		t.Fatal(err)
	}
	if c.peer.InitialMaxStreamsUni != 4 {
		t.Fatalf("uni limit = %d, want 4", c.peer.InitialMaxStreamsUni)
	}
	if c.peer.InitialMaxStreamsBidi != 1 {
		t.Fatalf("bidi limit = %d, want 1 (unchanged)", c.peer.InitialMaxStreamsBidi)
	}
}
