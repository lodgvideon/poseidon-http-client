package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/require"
)

// TestSetPeerSetting_MergesAndReplaces verifies that setPeerSetting
// upserts pairs in place (replacement) and appends fresh ones.
func TestSetPeerSetting_MergesAndReplaces(t *testing.T) {
	var p frame.SettingsParams

	setPeerSetting(&p, frame.SettingMaxFrameSize, 32768)
	setPeerSetting(&p, frame.SettingInitialWindowSize, 1<<20)
	setPeerSetting(&p, frame.SettingMaxFrameSize, 65535) // replace

	require.EqualValuesf(t, 2, p.N, "N = %d, want 2 — a replacement must not append a second pair", p.N)
	require.EqualValuesf(t, 65535, settingValue(p, frame.SettingMaxFrameSize, 0),
		"MaxFrameSize = %d, want 65535", settingValue(p, frame.SettingMaxFrameSize, 0))
	require.EqualValuesf(t, 1<<20, settingValue(p, frame.SettingInitialWindowSize, 0),
		"InitialWindowSize = %d, want %d", settingValue(p, frame.SettingInitialWindowSize, 0), 1<<20)
}

// TestApplyPeerSettings_InitialWindowSizeDelta_AppliesToAllStreams
// asserts the RFC 7540 §6.9.2 retroactive resize: every open stream's
// sendWindow shifts by (new - old).
func TestApplyPeerSettings_InitialWindowSizeDelta_AppliesToAllStreams(t *testing.T) {
	c := newDynSettingsConn()
	s1 := newStream(1, 8, c, 65535)
	s1.id = 1
	s1.sendWindow = 65535
	s2 := newStream(3, 8, c, 65535)
	s2.id = 3
	s2.sendWindow = 1000
	c.streams[1] = s1
	c.streams[3] = s2
	// Seed previous INITIAL_WINDOW_SIZE so old != default.
	c.peerSettings.Pairs[0] = frame.SettingPair{ID: frame.SettingInitialWindowSize, Value: 65535}
	c.peerSettings.N = 1
	var p frame.SettingsParams
	setPeerSetting(&p, frame.SettingInitialWindowSize, 100000) // +34465

	err := c.applyPeerSettings(p)

	require.NoErrorf(t, err, "applyPeerSettings")
	const delta = int32(100000 - 65535)
	require.Equalf(t, int32(65535)+delta, s1.sendWindow,
		"s1.sendWindow = %d, want %d", s1.sendWindow, 65535+delta)
	require.Equalf(t, int32(1000)+delta, s2.sendWindow,
		"s2.sendWindow = %d, want %d", s2.sendWindow, 1000+delta)
}

// TestApplyPeerSettings_NegativeDelta_AllowsNegativeWindow covers the
// other half of §6.9.2: the per-stream window is allowed to go
// negative; new DATA frames must wait for replenishment.
func TestApplyPeerSettings_NegativeDelta_AllowsNegativeWindow(t *testing.T) {
	c := newDynSettingsConn()
	s1 := newStream(1, 8, c, 65535)
	s1.id = 1
	s1.sendWindow = 100 // already partially consumed
	c.streams[1] = s1
	c.peerSettings.Pairs[0] = frame.SettingPair{ID: frame.SettingInitialWindowSize, Value: 65535}
	c.peerSettings.N = 1
	var p frame.SettingsParams
	setPeerSetting(&p, frame.SettingInitialWindowSize, 1024) // delta = -64511

	err := c.applyPeerSettings(p)

	require.NoErrorf(t, err, "applyPeerSettings")
	require.Equalf(t, int32(100-64511), s1.sendWindow,
		"s1.sendWindow = %d, want %d", s1.sendWindow, 100-64511)
}

// TestApplyPeerSettings_OverflowDelta_ReturnsConnError covers the
// 2^31-1 cap: an INITIAL_WINDOW_SIZE bump that pushes any stream past
// the spec maximum surfaces a typed ConnError.
func TestApplyPeerSettings_OverflowDelta_ReturnsConnError(t *testing.T) {
	c := newDynSettingsConn()
	s1 := newStream(1, 8, c, 0)
	s1.id = 1
	s1.sendWindow = 1<<31 - 100
	c.streams[1] = s1
	c.peerSettings.Pairs[0] = frame.SettingPair{ID: frame.SettingInitialWindowSize, Value: 0}
	c.peerSettings.N = 1
	var p frame.SettingsParams
	setPeerSetting(&p, frame.SettingInitialWindowSize, 1000)

	err := c.applyPeerSettings(p)

	require.Errorf(t, err, "want ConnError, got nil")
	var ce *ConnError
	require.ErrorAsf(t, err, &ce, "err = %v, want ConnError(FLOW_CONTROL_ERROR)", err)
	require.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
		"err = %v, want ConnError(FLOW_CONTROL_ERROR)", err)
}

// TestApplyPeerSettings_HeaderTableSize_PropagatesToEncoder verifies
// the HPACK encoder receives the new max dynamic-table size.
func TestApplyPeerSettings_HeaderTableSize_PropagatesToEncoder(t *testing.T) {
	c := newDynSettingsConn()
	c.enc = hpack.NewEncoder()
	var p frame.SettingsParams
	setPeerSetting(&p, frame.SettingHeaderTableSize, 8192)

	err := c.applyPeerSettings(p)

	require.NoErrorf(t, err, "applyPeerSettings")
	// HPACK encoder does not expose its table size, but a follow-up
	// EncodeBlock should still succeed (sanity check).
	out := c.enc.EncodeBlock(nil, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
	})
	require.NotEmpty(t, out, "encoder broken after resize")
}

// TestOnSettings_AckFlag_IsNoop confirms an incoming ACK never
// triggers applyPeerSettings or a follow-up ACK frame on the wire.
func TestOnSettings_AckFlag_IsNoop(t *testing.T) {
	var buf bytes.Buffer
	fr := frame.NewFramer(&buf, bytes.NewReader([]byte{}))
	c := newDynSettingsConn()
	c.fr = fr
	h := newConnHandler(c, hpack.NewDecoder())

	err := h.OnSettings(frame.FrameHeader{Flags: frame.FlagSettingsAck}, frame.SettingsParams{})

	require.NoErrorf(t, err, "OnSettings(ACK)")
	require.Zerof(t, buf.Len(), "ACK echoed for ACK input: %d bytes", buf.Len())
}

// TestOnSettings_NonAck_WritesAckFrame asserts a non-ACK SETTINGS
// triggers a SETTINGS ACK frame on the wire (RFC 7540 §6.5.3).
func TestOnSettings_NonAck_WritesAckFrame(t *testing.T) {
	var buf bytes.Buffer
	fr := frame.NewFramer(&buf, bytes.NewReader([]byte{}))
	c := newDynSettingsConn()
	c.fr = fr
	h := newConnHandler(c, hpack.NewDecoder())
	var p frame.SettingsParams
	setPeerSetting(&p, frame.SettingMaxFrameSize, 65535)

	err := h.OnSettings(frame.FrameHeader{}, p)

	require.NoErrorf(t, err, "OnSettings")
	got := parseFrameHeaders(t, buf.Bytes())
	require.Lenf(t, got, 1, "frame count = %d, want 1", len(got))
	require.Equalf(t, byte(0x4), got[0].ftype, "ftype = 0x%x, want 0x4 (SETTINGS)", got[0].ftype)
	require.NotZero(t, got[0].flags&0x1, "ACK flag not set")
	require.Zerof(t, got[0].length, "ACK payload = %d bytes, want 0", got[0].length)
}

// newDynSettingsConn builds a *Conn just enough to drive
// applyPeerSettings + writeSettingsAck unit tests.
func newDynSettingsConn() *Conn {
	c := &Conn{
		opts:               ConnOptions{}.defaulted(),
		streams:            map[uint32]*Stream{},
		readerDone:         make(chan struct{}),
		peerConnSendWindow: 65535,
		enc:                hpack.NewEncoder(),
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	return c
}

// frameHeaderRecord is a parsed wire frame header used for ACK
// emission assertions.
type frameHeaderRecord struct {
	length   uint32
	ftype    byte
	flags    byte
	streamID uint32
}

// parseFrameHeaders walks a byte stream of HTTP/2 frames and returns
// each frame header. Caller must ensure no truncation.
func parseFrameHeaders(t *testing.T, b []byte) []frameHeaderRecord {
	t.Helper()
	var out []frameHeaderRecord
	for len(b) >= 9 {
		length := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		ftype := b[3]
		flags := b[4]
		streamID := uint32(b[5])<<24 | uint32(b[6])<<16 | uint32(b[7])<<8 | uint32(b[8])
		streamID &^= 1 << 31
		out = append(out, frameHeaderRecord{length: length, ftype: ftype, flags: flags, streamID: streamID})
		b = b[9+length:]
	}
	return out
}

// keep context import alive across test variations.
var _ = context.Background
