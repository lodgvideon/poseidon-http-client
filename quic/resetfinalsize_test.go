package quic

import "testing"

// TestConformance_RFC9000_Sec45_ResetChangesFinalSizeAfterFin checks that once a
// FIN has fixed a stream's final size, a RESET_STREAM declaring a different final
// size is a FINAL_SIZE_ERROR even while the stream is not yet complete (a gap
// below the FIN); a RESET with the matching final size is accepted (RFC 9000 §4.5).
func TestConformance_RFC9000_Sec45_ResetChangesFinalSizeAfterFin(t *testing.T) {
	// Different final size → error.
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	h := &connFrameHandler{c: c}
	// A STREAM frame with FIN at offset 50 (length 50) fixes the final size at 100,
	// but bytes [0,50) are missing, so the stream is not complete.
	if err := h.OnStream(s.id, 50, true, make([]byte, 50)); err != nil {
		t.Fatal(err)
	}
	if s.recv.complete() {
		t.Fatal("the stream must not be complete with a gap below the FIN")
	}
	if err := h.OnResetStream(s.id, 0, 200); err != ErrFinalSize {
		t.Fatalf("RESET_STREAM with a changed final size = %v, want ErrFinalSize", err)
	}

	// Matching final size → accepted.
	c2 := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	s2, _ := c2.OpenStream()
	h2 := &connFrameHandler{c: c2}
	if err := h2.OnStream(s2.id, 50, true, make([]byte, 50)); err != nil {
		t.Fatal(err)
	}
	if err := h2.OnResetStream(s2.id, 0, 100); err != nil {
		t.Fatalf("RESET_STREAM with the matching final size = %v, want nil", err)
	}
}
