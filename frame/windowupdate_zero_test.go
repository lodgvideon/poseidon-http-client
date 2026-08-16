package frame

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RFC 7540 §6.9 makes a WINDOW_UPDATE whose increment is 0 an error the receiver
// must reject — PROTOCOL_ERROR on the connection, or a stream error on a stream.
// This framer refuses to write one, and refuses to accept one.
//
// The two used to disagree about WHEN they masked the 31-bit field. The reader
// masked and then checked; the writer checked and then masked. So an increment
// with its high bit set — out of range for the field, and reachable because the
// parameter is a public uint32 — passed the writer's guard and went on the wire
// encoded as 0: the exact frame the guard exists to prevent, which the peer is
// then obliged to treat as a connection error (#517).

// TestWriteWindowUpdate_HighBitIncrement is the gate, and it pins the decided
// rule for the two ways the reserved bit can arrive:
//
//   - 0x80000000 masks to 0, which is the frame §6.9 forbids, so it is refused;
//   - 0x80000005 masks to 5 and is written, silently, exactly as an out-of-range
//     promised id or last-stream-id is masked elsewhere in this file.
//
// The second is a deliberate choice rather than an oversight: the high bit is
// the R bit, every other 31-bit field here drops it without complaint, and the
// reader ignores it on receipt. What must never happen is the first case being
// written.
func TestWriteWindowUpdate_HighBitIncrement(t *testing.T) {
	t.Run("masks to zero: refused", func(t *testing.T) {
		fr, buf := newFramerWithBuffer()

		err := fr.WriteWindowUpdate(1, 0x80000000)

		assert.ErrorIsf(t, err, ErrZeroIncrement,
			"WriteWindowUpdate(1, 0x80000000) = %v, want ErrZeroIncrement — it "+
				"masks to a zero increment, the frame §6.9 obliges a receiver to reject", err)
		assert.Zerof(t, buf.Len(), "a refused WINDOW_UPDATE still wrote %d bytes to the wire", buf.Len())
	})

	t.Run("masks to nonzero: written masked", func(t *testing.T) {
		fr, _ := newFramerWithBuffer()
		require.NoError(t, fr.WriteWindowUpdate(1, 0x80000005), "WriteWindowUpdate")
		h := &recordingHandler{}

		_, err := fr.ReadFrame(context.Background(), h)

		require.NoErrorf(t, err, "the framer wrote a frame its own reader rejects: %v", err)
		assert.EqualValuesf(t, 5, h.winInc, "increment = %d, want 5 (the reserved bit masked off)", h.winInc)
	})
}

// TestWriteWindowUpdate_ZeroStillRefused is the control: fixing the high-bit case
// must not cost the plain zero its refusal, which is the rule RFC 7540 §6.9
// actually states.
func TestWriteWindowUpdate_ZeroStillRefused(t *testing.T) {
	fr, buf := newFramerWithBuffer()

	err := fr.WriteWindowUpdate(1, 0)

	assert.ErrorIsf(t, err, ErrZeroIncrement, "WriteWindowUpdate(1, 0) = %v, want ErrZeroIncrement", err)
	assert.Zerof(t, buf.Len(), "a refused WINDOW_UPDATE still wrote %d bytes", buf.Len())
}

// TestWriteWindowUpdate_LegalIncrementRoundTrips is the over-correction guard: an
// ordinary increment must still be written and read back unchanged.
func TestWriteWindowUpdate_LegalIncrementRoundTrips(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	const inc = 65535
	require.NoError(t, fr.WriteWindowUpdate(3, inc), "WriteWindowUpdate")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.EqualValuesf(t, inc, h.winInc, "increment = %d, want %d", h.winInc, inc)
}
