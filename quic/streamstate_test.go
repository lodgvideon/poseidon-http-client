package quic

import "testing"

// TestConformance_RFC9000_Sec19_StreamStateErrors checks that a frame violating a
// stream's directionality is a STREAM_STATE_ERROR: a STREAM (§19.8) or
// RESET_STREAM (§19.4) on a send-only client-initiated unidirectional stream
// (id&0x3 == 0x2), and a STOP_SENDING (§19.5) on a receive-only server-initiated
// unidirectional stream (id&0x3 == 0x3).
func TestConformance_RFC9000_Sec19_StreamStateErrors(t *testing.T) {
	t.Run("stream-on-send-only", func(t *testing.T) {
		h := &connFrameHandler{c: &Conn{}}
		if err := h.OnStream(2, 0, false, []byte("x")); err != ErrStreamState {
			t.Fatalf("OnStream on a send-only stream = %v, want ErrStreamState", err)
		}
	})
	t.Run("reset-on-send-only", func(t *testing.T) {
		h := &connFrameHandler{c: &Conn{}}
		if err := h.OnResetStream(2, 0, 0); err != ErrStreamState {
			t.Fatalf("OnResetStream on a send-only stream = %v, want ErrStreamState", err)
		}
	})
	t.Run("stop-sending-on-recv-only", func(t *testing.T) {
		h := &connFrameHandler{c: &Conn{}}
		if err := h.OnStopSending(3, 0); err != ErrStreamState {
			t.Fatalf("OnStopSending on a receive-only stream = %v, want ErrStreamState", err)
		}
	})
}

// TestConformance_RFC9000_Sec19_StreamStateValidDirections checks that the same
// frames on a stream with the matching direction are accepted (no false positive):
// a STREAM on a receive-capable server-uni stream, and a STOP_SENDING on a
// send-capable client-uni stream.
func TestConformance_RFC9000_Sec19_StreamStateValidDirections(t *testing.T) {
	// STREAM on a server-initiated unidirectional stream (receive-only for us): ok.
	c := &Conn{localMaxStreamsUni: 10}
	if err := (&connFrameHandler{c: c}).OnStream(3, 0, false, []byte("x")); err != nil {
		t.Fatalf("OnStream on a receive-capable server-uni stream = %v, want nil", err)
	}
	// STOP_SENDING on a client-initiated unidirectional stream (send-only for us,
	// so a send side exists): ok, not a STREAM_STATE_ERROR.
	if err := (&connFrameHandler{c: &Conn{}}).OnStopSending(2, 0); err != nil {
		t.Fatalf("OnStopSending on a send-capable client-uni stream = %v, want nil", err)
	}

	// RESET_STREAM on a server-initiated unidirectional stream (receive-only for
	// us): ok — the server may reset its own send side, which is our receive side.
	rc := &Conn{localMaxStreamsUni: 10}
	s, err := rc.acceptPeerUniStream(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&connFrameHandler{c: rc}).OnResetStream(3, 7, 0); err != nil {
		t.Fatalf("OnResetStream on a receive-capable server-uni stream = %v, want nil", err)
	}
	if !s.recvReset {
		t.Fatal("a valid RESET_STREAM on a server-uni stream should mark its receive side reset")
	}
}

// TestConn_StreamStateError_ClosesWithCode checks that ErrStreamState maps to the
// STREAM_STATE_ERROR transport code (RFC 9000 §20.1).
func TestConn_StreamStateError_ClosesWithCode(t *testing.T) {
	code, ok := closeCodeFor(ErrStreamState)
	if !ok || code != ErrCodeStreamStateError {
		t.Fatalf("closeCodeFor(ErrStreamState) = (%#x, %v), want (%#x, true)", code, ok, ErrCodeStreamStateError)
	}
}
