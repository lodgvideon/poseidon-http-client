package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec45_ResetChangesFinalSizeAfterFin checks that once a
// FIN has fixed a stream's final size, a RESET_STREAM declaring a different final
// size is a FINAL_SIZE_ERROR even while the stream is not yet complete (a gap
// below the FIN); a RESET with the matching final size is accepted (RFC 9000 §4.5).
func TestConformance_RFC9000_Sec45_ResetChangesFinalSizeAfterFin(t *testing.T) {
	// finToFifty opens a stream whose final size is fixed at 100 by a FIN at
	// offset 50, with bytes [0,50) missing so the stream is not complete.
	finToFifty := func(t *testing.T) (*Stream, *connFrameHandler) {
		t.Helper()
		c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
		s, err := c.OpenStream()
		require.NoError(t, err, "open the stream the final size is fixed on")
		h := &connFrameHandler{c: c}
		require.NoError(t, h.OnStream(s.id, 50, true, make([]byte, 50)),
			"a FIN at offset 50 (length 50) fixes the final size at 100")
		return s, h
	}

	t.Run("changed_final_size", func(t *testing.T) {
		s, h := finToFifty(t)
		notComplete := s.recv.complete()

		changed := h.OnResetStream(s.id, 0, 200)

		require.False(t, notComplete, "the stream must not be complete with a gap below the FIN")
		assert.ErrorIsf(t, changed, ErrFinalSize,
			"RESET_STREAM with a changed final size = %v, want ErrFinalSize", changed)
	})

	t.Run("matching_final_size", func(t *testing.T) {
		s, h := finToFifty(t)

		matching := h.OnResetStream(s.id, 0, 100)

		assert.NoErrorf(t, matching,
			"RESET_STREAM with the matching final size = %v, want nil", matching)
	})
}
