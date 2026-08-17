package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The RFC 7540 §6.9.1 ceiling used to be declared three times in this package,
// twice under the same name maxWindow with different types (int32 and int64) in
// the same file. Merging them is only safe if every path still rejects at exactly
// the same boundary, which is what these pin (#517).
//
// The value itself is asserted too: a constant that silently became 2^31 or
// 2^31-2 would leave every behavioural test below still passing, since they are
// all expressed relative to the constant.

// TestMaxFlowWindow_IsTheRFCCeiling pins the number, not a relation.
func TestMaxFlowWindow_IsTheRFCCeiling(t *testing.T) {
	got := maxFlowWindow

	assert.EqualValuesf(t, 2147483647, got,
		"maxFlowWindow = %d, want 2147483647 (2^31-1, RFC 7540 §6.9.1)", got)
}

// TestOnWindowUpdate_BoundaryIsExact drives the §6.9.1 path that used to read the
// int32 spelling. At the ceiling it must succeed; one byte past it must be a
// FLOW_CONTROL_ERROR — a stream error for a stream, a connection error for
// stream 0.
func TestOnWindowUpdate_BoundaryIsExact(t *testing.T) {
	t.Run("connection window", func(t *testing.T) {
		c := newGoAwayConn()
		c.peerConnSendWindow = 1

		// 1 + (2^31-2) == the ceiling exactly.
		atCeiling := c.onWindowUpdate(0, uint32(maxFlowWindow-1))
		c.peerConnSendWindow = 1
		past := c.onWindowUpdate(0, uint32(maxFlowWindow))

		require.NoErrorf(t, atCeiling,
			"an increment landing exactly on the ceiling was rejected: %v", atCeiling)
		var ce *ConnError
		if assert.ErrorAsf(t, past, &ce,
			"one past the ceiling gave %v, want a ConnError FLOW_CONTROL_ERROR", past) {
			assert.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
				"one past the ceiling gave %v, want a ConnError FLOW_CONTROL_ERROR", past)
		}
	})

	t.Run("stream window", func(t *testing.T) {
		c := newGoAwayConn()
		s := &Stream{id: 1, sendWindow: 1}
		c.streams[1] = s

		atCeiling := c.onWindowUpdate(1, uint32(maxFlowWindow-1))
		s.sendWindow = 1
		past := c.onWindowUpdate(1, uint32(maxFlowWindow))

		require.NoErrorf(t, atCeiling,
			"an increment landing exactly on the ceiling was rejected: %v", atCeiling)
		var se *StreamError
		if assert.ErrorAsf(t, past, &se,
			"one past the ceiling gave %v, want a StreamError FLOW_CONTROL_ERROR", past) {
			assert.Equalf(t, frame.ErrCodeFlowControlError, se.Code,
				"one past the ceiling gave %v, want a StreamError FLOW_CONTROL_ERROR", past)
		}
	})
}

// TestApplyPeerSettings_InitialWindowBoundaryIsExact drives the §6.5.2 path that
// used to read maxInitialWindowSize, the third declaration.
func TestApplyPeerSettings_InitialWindowBoundaryIsExact(t *testing.T) {
	atCeiling := frame.SettingsParams{N: 1}
	atCeiling.Pairs[0] = frame.SettingPair{
		ID: frame.SettingInitialWindowSize, Value: uint32(maxFlowWindow),
	}
	// One past it is not expressible in the 31-bit field as a larger legal value,
	// so the peer has to send 2^31, which is where the check bites.
	past := frame.SettingsParams{N: 1}
	past.Pairs[0] = frame.SettingPair{
		ID: frame.SettingInitialWindowSize, Value: uint32(maxFlowWindow) + 1,
	}

	atErr := newGoAwayConn().applyPeerSettings(atCeiling)
	pastErr := newGoAwayConn().applyPeerSettings(past)

	require.NoErrorf(t, atErr,
		"SETTINGS_INITIAL_WINDOW_SIZE at exactly 2^31-1 was rejected: %v", atErr)
	var ce *ConnError
	if assert.ErrorAsf(t, pastErr, &ce,
		"SETTINGS_INITIAL_WINDOW_SIZE one past the ceiling gave %v, "+
			"want a ConnError FLOW_CONTROL_ERROR (§6.5.2)", pastErr) {
		assert.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
			"SETTINGS_INITIAL_WINDOW_SIZE one past the ceiling gave %v, "+
				"want a ConnError FLOW_CONTROL_ERROR (§6.5.2)", pastErr)
	}
}
