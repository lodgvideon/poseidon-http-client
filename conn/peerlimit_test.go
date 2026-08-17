package conn

import (
	"context"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLookupPeerSetting_PresentVsAbsent verifies the present/absent
// signal that NewStream uses to tell "peer never advertised
// MAX_CONCURRENT_STREAMS" from "peer advertised the value zero".
func TestLookupPeerSetting_PresentVsAbsent(t *testing.T) {
	var p frame.SettingsParams

	_, absentOK := lookupPeerSetting(p, frame.SettingMaxConcurrentStreams)
	setPeerSetting(&p, frame.SettingMaxConcurrentStreams, 0)
	zeroV, zeroOK := lookupPeerSetting(p, frame.SettingMaxConcurrentStreams)
	setPeerSetting(&p, frame.SettingMaxConcurrentStreams, 5)
	fiveV, fiveOK := lookupPeerSetting(p, frame.SettingMaxConcurrentStreams)

	require.False(t, absentOK, "lookup of absent ID returned ok=true")
	require.True(t, zeroOK, "lookup of zero-valued ID returned ok=false")
	assert.EqualValuesf(t, 0, zeroV, "v = %d, want 0", zeroV)
	assert.Truef(t, fiveOK && fiveV == 5, "got (%d, %v), want (5, true)", fiveV, fiveOK)
}

// TestNewStream_PeerLimitTighterThanLocal_Wins confirms that when the
// peer advertises a smaller MaxConcurrentStreams than our local cap,
// we honor the peer's value.
func TestNewStream_PeerLimitTighterThanLocal_Wins(t *testing.T) {
	c := newPeerLimitConn(10) // local cap 10
	setPeerSetting(&c.peerSettings, frame.SettingMaxConcurrentStreams, 2)
	for i := 0; i < 2; i++ {
		_, err := c.NewStream(context.Background())
		require.NoErrorf(t, err, "NewStream %d", i)
	}

	_, err := c.NewStream(context.Background())

	require.ErrorIsf(t, err, ErrTooManyStreams,
		"3rd NewStream err = %v, want ErrTooManyStreams (peer cap 2)", err)
}

// TestNewStream_PeerLimitAbsent_FallsThroughToLocal confirms that
// without a peer-advertised MaxConcurrentStreams we use only the
// local cap, even though peer "default" per RFC is unlimited.
func TestNewStream_PeerLimitAbsent_FallsThroughToLocal(t *testing.T) {
	c := newPeerLimitConn(3)
	for i := 0; i < 3; i++ {
		_, err := c.NewStream(context.Background())
		require.NoErrorf(t, err, "NewStream %d", i)
	}

	_, err := c.NewStream(context.Background())

	require.ErrorIsf(t, err, ErrTooManyStreams,
		"4th NewStream err = %v, want ErrTooManyStreams (local cap 3)", err)
}

// TestNewStream_PeerLimitLargerThanLocal_LocalWins confirms the
// stricter cap of the two governs.
func TestNewStream_PeerLimitLargerThanLocal_LocalWins(t *testing.T) {
	c := newPeerLimitConn(2)
	setPeerSetting(&c.peerSettings, frame.SettingMaxConcurrentStreams, 100)
	for i := 0; i < 2; i++ {
		_, err := c.NewStream(context.Background())
		require.NoErrorf(t, err, "NewStream %d", i)
	}

	_, err := c.NewStream(context.Background())

	require.ErrorIsf(t, err, ErrTooManyStreams,
		"3rd NewStream err = %v, want ErrTooManyStreams (local cap 2 < peer 100)", err)
}

// TestNewStream_PeerLimitZero_BlocksAllNewStreams confirms peer's
// MaxConcurrentStreams=0 means "no new streams" (RFC 7540 §6.5.2).
// Existing streams continue; we just refuse new ones.
func TestNewStream_PeerLimitZero_BlocksAllNewStreams(t *testing.T) {
	c := newPeerLimitConn(10)
	setPeerSetting(&c.peerSettings, frame.SettingMaxConcurrentStreams, 0)

	_, err := c.NewStream(context.Background())

	require.ErrorIsf(t, err, ErrTooManyStreams,
		"NewStream err = %v, want ErrTooManyStreams (peer cap 0)", err)
}

// TestApplyPeerSettings_LowerMaxConcurrent_DoesNotCloseExistingStreams
// covers the dynamic-update branch of RFC §6.5.2: shrinking the cap
// must not retroactively kill open streams; only future NewStream
// calls are blocked.
func TestApplyPeerSettings_LowerMaxConcurrent_DoesNotCloseExistingStreams(t *testing.T) {
	c := newPeerLimitConn(10)
	setPeerSetting(&c.peerSettings, frame.SettingMaxConcurrentStreams, 5)
	// Open three streams under the original cap of 5.
	for i := 0; i < 3; i++ {
		_, err := c.NewStream(context.Background())
		require.NoErrorf(t, err, "seed NewStream %d", i)
	}
	// Peer drops the cap to 2 mid-flight.
	var update frame.SettingsParams
	setPeerSetting(&update, frame.SettingMaxConcurrentStreams, 2)

	err := c.applyPeerSettings(update)

	require.NoErrorf(t, err, "applyPeerSettings")
	// Open streams remain in the registry; new allocation refused.
	assert.EqualValuesf(t, 3, c.inflight, "inflight = %d, want 3 (existing streams preserved)", c.inflight)
	_, nsErr := c.NewStream(context.Background())
	assert.ErrorIsf(t, nsErr, ErrTooManyStreams,
		"NewStream after lowered cap = %v, want ErrTooManyStreams", nsErr)
}

// newPeerLimitConn builds a *Conn that supports NewStream + dynamic
// SETTINGS apply but no actual I/O. localMax sets the locally
// advertised MaxConcurrentStreams.
func newPeerLimitConn(localMax uint32) *Conn {
	c := &Conn{
		opts: ConnOptions{
			Settings: AdvertisedSettings{MaxConcurrentStreams: localMax},
		}.defaulted(),
		streams:            map[uint32]*Stream{},
		readerDone:         make(chan struct{}),
		peerConnSendWindow: 65535,
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	return c
}
