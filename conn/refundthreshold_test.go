package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestStreamRefundThreshold_CoversAllThreeArms is the unit table for a function
// no test in the package named at all (#834). refund_window_test.go drives
// advertised windows of 65535, 32768, 16384, 8192 and 1024 end to end, which
// covers two of its three arms; the floor of 1 is reached only by a window of 0
// or 1, both legal per RFC 7540 §6.5.2, and its own comment states what it is
// for: "The floor of 1 keeps a pathological window of 1 refunding per byte
// rather than never." A threshold of 0 would compare `pending >= 0`, which is
// true before a single byte arrives.
func TestStreamRefundThreshold_CoversAllThreeArms(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window uint32
		want   uint32
	}{
		{"a window of 0 falls to the floor", 0, 1},
		{"a window of 1 falls to the floor", 1, 1},
		{"a window of 2 is the first that has a half", 2, 1},
		{"a window of 3 rounds down to the same half", 3, 1},
		{"one below twice the threshold takes the half",
			2*recvWindowRefundThreshold - 1, recvWindowRefundThreshold - 1},
		{"exactly twice the threshold takes the constant",
			2 * recvWindowRefundThreshold, recvWindowRefundThreshold},
		{"one above twice the threshold is capped",
			2*recvWindowRefundThreshold + 1, recvWindowRefundThreshold},
		{"the RFC 7540 §6.9.1 ceiling is capped",
			uint32(maxFlowWindow), recvWindowRefundThreshold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := streamRefundThreshold(tc.window)

			assert.Equalf(t, tc.want, got,
				"streamRefundThreshold(%d) = %d, want %d — a threshold larger than the window "+
					"can never be reached and the peer stalls forever, and a threshold of 0 "+
					"makes every frame emit a WINDOW_UPDATE for nothing",
				tc.window, got, tc.want)
		})
	}
}

// TestHandler_OnContinuation_FloodBoundIsExact pins the CONTINUATION-flood guard
// at its edge (RFC 7540 §6.10 / §10.5.1, CVE-2024-27316). The two CONTINUATION
// tests in coverage_test.go asserted only that no error came back, so neither
// the accumulation limit nor the byte past it was ever driven (#819).
func TestHandler_OnContinuation_FloodBoundIsExact(t *testing.T) {
	const limit = 64

	for _, tc := range []struct {
		name    string
		total   int
		wantErr bool
	}{
		{"exactly the accumulation limit is accepted", limit, false},
		{"one byte past the limit is refused", limit + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)
			h.maxHeaderBytes = limit
			h.pendingStreamID = 1
			h.pendingBuf = make([]byte, 1) // one byte of the block already buffered

			// No END_HEADERS: the frame only accumulates, so the bound is the
			// only thing that can answer.
			err := h.OnContinuation(frame.FrameHeader{
				Type: frame.FrameContinuation, StreamID: 1,
			}, make([]byte, tc.total-1))

			if tc.wantErr {
				var ce *ConnError
				require.ErrorAsf(t, err, &ce,
					"a %d-byte block against a %d-byte limit gave %v; want a ConnError — the "+
						"flood is a connection-level resource attack, not one stream's problem",
					tc.total, limit, err)
				assert.Equalf(t, frame.ErrCodeEnhanceYourCalm, ce.Code,
					"code = %v, want ENHANCE_YOUR_CALM — it is what tells the peer to back off "+
						"rather than that it sent something malformed", ce.Code)
				return
			}
			require.NoErrorf(t, err,
				"a block of exactly the %d-byte limit was refused: %v — the guard is on "+
					"`> limit`, so the limit itself is legal and refusing it drops honest traffic",
				limit, err)
			assert.Lenf(t, h.pendingBuf, tc.total,
				"pendingBuf holds %d bytes, want %d — an accepted CONTINUATION has to be "+
					"appended, or the block the stream is finally handed is truncated",
				len(h.pendingBuf), tc.total)
		})
	}
}
