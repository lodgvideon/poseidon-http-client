package conn

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// writeHeaderBlock reserves room for the pad-length octet and the padding before
// deciding how much of the encoded block fits in the first HEADERS frame.
// Disabling only that deduction left the whole package green (#818), because the
// §6.10 test next door asserts the PADDED and PRIORITY flags and never a length.
// Without it the first frame's payload is 1 + maxFrame + padLen — larger than
// the peer's SETTINGS_MAX_FRAME_SIZE, which RFC 9113 §4.2 makes a connection
// error of type FRAME_SIZE_ERROR at the receiver. That is a connection we would
// be tearing down ourselves, on our own outbound frame, and nothing measured it.

// TestConformance_RFC9113_Sec4_2_PaddedHeaderSplitFitsMaxFrameSize pins the
// invariant §4.2 states: "An endpoint MUST send an error code of
// FRAME_SIZE_ERROR if a frame exceeds the size defined in
// SETTINGS_MAX_FRAME_SIZE". Every frame of a split, padded header block has to
// fit — and the first one, which carries the padding, is the one at risk.
func TestConformance_RFC9113_Sec4_2_PaddedHeaderSplitFitsMaxFrameSize(t *testing.T) {
	const peerMaxFrame = 256

	for _, tc := range []struct {
		name   string
		padLen uint8
		prio   *frame.Priority
	}{
		{"padded", 8, nil},
		{"padded with priority", 8, &frame.Priority{StreamDep: 0, Weight: 15}},
		{"padding just under the frame size", 100, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := newWireConn(&buf, peerMaxFrame)
			c.opts.Padding = fixedPadding(tc.padLen)
			s := newStream(0, 8, c, 65535)
			c.nextID = 1

			err := c.writeHeadersWithPriority(context.Background(), s, bigFields(20, 60), false, tc.prio)

			require.NoError(t, err, "writeHeadersWithPriority")
			frames := parseBlockFrames(t, buf.Bytes())
			require.GreaterOrEqualf(t, len(frames), 2,
				"got %d frames, want a CONTINUATION split — without one this test observes "+
					"nothing about the first frame's budget", len(frames))
			for i, f := range frames {
				assert.LessOrEqualf(t, len(f.payload), peerMaxFrame,
					"frame %d carries a %d-byte payload against a peer MAX_FRAME_SIZE of %d; "+
						"RFC 9113 §4.2 makes that a connection error FRAME_SIZE_ERROR at the "+
						"receiver, so we would be killing the connection with our own request",
					i, len(f.payload), peerMaxFrame)
			}
			assert.Equalf(t, peerMaxFrame, len(frames[0].payload),
				"first HEADERS payload = %d, want exactly %d — the pad octet, the padding and "+
					"as much of the block as the remaining budget allows; anything less means "+
					"the split is paying for padding twice", len(frames[0].payload), peerMaxFrame)
		})
	}
}

// TestConn_WriteHeaderBlock_SplitsOnePastThePaddedBudget is the boundary the
// test above approaches from the far side: a block that exactly fills the first
// frame's budget must stay in one frame, and one byte more must split. Drives
// writeHeaderBlock directly so the block length is chosen rather than whatever
// HPACK happens to produce.
func TestConn_WriteHeaderBlock_SplitsOnePastThePaddedBudget(t *testing.T) {
	const (
		peerMaxFrame = 256
		padLen       = 8
		budget0      = peerMaxFrame - 1 - padLen
	)

	for _, tc := range []struct {
		name       string
		blockLen   int
		wantFrames int
	}{
		{"exactly the budget stays in one frame", budget0, 1},
		{"one byte past the budget splits", budget0 + 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := newWireConn(&buf, peerMaxFrame)
			c.opts.Padding = fixedPadding(padLen)

			err := c.writeHeaderBlock(1, make([]byte, tc.blockLen), false, nil)

			require.NoError(t, err, "writeHeaderBlock")
			frames := parseBlockFrames(t, buf.Bytes())
			assert.Equalf(t, tc.wantFrames, len(frames),
				"a %d-byte block became %d frames, want %d — the budget for the first frame is "+
					"MAX_FRAME_SIZE minus the pad-length octet and the padding, so it is %d bytes",
				tc.blockLen, len(frames), tc.wantFrames, budget0)
			for i, f := range frames {
				assert.LessOrEqualf(t, len(f.payload), peerMaxFrame,
					"frame %d carries %d payload bytes against a peer MAX_FRAME_SIZE of %d (§4.2)",
					i, len(f.payload), peerMaxFrame)
			}
		})
	}
}
