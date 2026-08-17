package conn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestConn_AcquireSendCredits_WakesOnReaderDeath pins that a writer blocked on an
// exhausted send window wakes and returns ErrConnClosed when the reader goroutine
// dies (a peer RST/close), instead of hanging forever. The only exits from the
// cond-var wait were window credit, c.closed and ctx.Err(); reader death sets none
// of those (c.closed stays false — see IsAlive), so a SendData on a
// non-cancellable context blocked forever on a provably dead connection.
func TestConn_AcquireSendCredits_WakesOnReaderDeath(t *testing.T) {
	c := newOutFCConn(0, 0) // conn send window 0
	s := newStream(1, 8, c, 65535)
	c.streams[1] = s
	s.sendWindow = 0 // stream window 0 too → the writer must block

	out := make(chan error, 1)
	go func() {
		_, err := c.acquireSendCredits(context.Background(), s, s.gen.Load(), 100, 0)
		out <- err
	}()

	// Let it park in fcOutCond.Wait.
	select {
	case r := <-out:
		require.FailNowf(t, "returned early", "acquireSendCredits returned before reader death: %v", r)
	case <-time.After(50 * time.Millisecond):
	}

	// Reader dies (transport error). shutdownStreams runs on that path.
	c.shutdownStreams()

	select {
	case err := <-out:
		require.Truef(t, errors.Is(err, ErrConnClosed),
			"acquireSendCredits = %v, want ErrConnClosed on reader death", err)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "acquireSendCredits did not wake on reader death — SendData hangs forever")
	}
}

// TestConformance_RFC7540_Sec6_5_2_HandshakeOversizedInitialWindow_FlowControlError
// pins that a handshake SETTINGS_INITIAL_WINDOW_SIZE above 2^31-1 is a
// FLOW_CONTROL_ERROR (RFC 7540 §6.5.2), not silently accepted. Accepting it seeded
// int32(value) negative into every stream's send window, so acquireSendCredits
// blocked forever. The mid-connection applyPeerSettings already rejected this; the
// handshake path did not.
func TestConformance_RFC7540_Sec6_5_2_HandshakeOversizedInitialWindow_FlowControlError(t *testing.T) {
	c := newOutFCConn(0, 0)

	var oversized frame.SettingsParams
	oversized.Pairs[0] = frame.SettingPair{ID: frame.SettingInitialWindowSize, Value: 0x80000000} // 2^31 > 2^31-1
	oversized.N = 1
	// The exact limit is accepted.
	var atLimit frame.SettingsParams
	atLimit.Pairs[0] = frame.SettingPair{ID: frame.SettingInitialWindowSize, Value: 1<<31 - 1}
	atLimit.N = 1

	err := c.applyInitialPeerSettings(oversized)
	atLimitErr := c.applyInitialPeerSettings(atLimit)

	var ce *ConnError
	require.Truef(t, errors.As(err, &ce),
		"applyInitialPeerSettings(2^31) = %v, want ConnError FLOW_CONTROL_ERROR", err)
	assert.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
		"applyInitialPeerSettings(2^31) = %v, want ConnError FLOW_CONTROL_ERROR", err)
	assert.NoErrorf(t, atLimitErr,
		"applyInitialPeerSettings(2^31-1) = %v, want nil — the value at the ceiling is legal", atLimitErr)
}
