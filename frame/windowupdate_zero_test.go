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

// TestWriteWindowUpdate_StreamIDDistinguishesConnectionFromStream pins the other
// half of the frame, which nothing asserted at all (#779): a connection-level and
// a stream-level WINDOW_UPDATE are protocol-distinct frames, and the only thing
// that tells them apart is the stream id in the header.
//
// RFC 9113 §6.9: "In the former case, the frame's stream identifier indicates the
// affected stream" — the credit lands on that stream's flow-control window, and
// 0 means the connection's. Writing every WINDOW_UPDATE on stream 0 refills the
// connection window while the stream that is actually blocked stays at zero, so
// the peer stalls forever with credit it cannot spend. The round-trip tests above
// read the increment back and never looked at the id, so that mutation was free.
//
// A table rather than one case, because the two ids are different equivalence
// classes and both directions of the decision have to be shown: 0 must stay 0
// (not be rewritten to some stream) and a non-zero id must survive (not be
// flattened to 0).
func TestWriteWindowUpdate_StreamIDDistinguishesConnectionFromStream(t *testing.T) {
	for _, tc := range []struct {
		name     string
		streamID uint32
	}{
		{"connection level", 0},
		{"stream level", 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fr, _ := newFramerWithBuffer()
			require.NoError(t, fr.WriteWindowUpdate(tc.streamID, 4096), "WriteWindowUpdate")
			h := &recordingHandler{}

			fh, err := fr.ReadFrame(context.Background(), h)

			require.NoErrorf(t, err, "read: %v", err)
			assert.Equalf(t, tc.streamID, fh.StreamID,
				"stream id = %d, want %d — a WINDOW_UPDATE addressed to the wrong "+
					"window credits a flow-control window the sender is not blocked on, "+
					"and the blocked one never refills", fh.StreamID, tc.streamID)
			assert.Equalf(t, tc.streamID, h.header.StreamID,
				"handler saw stream id %d, want %d", h.header.StreamID, tc.streamID)
		})
	}
}
