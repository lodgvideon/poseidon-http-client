package frame

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// settingsCapture records the SettingsParams a SETTINGS frame decodes to.
type settingsCapture struct {
	dropHandler
	got    SettingsParams
	called bool
}

func (h *settingsCapture) OnSettings(_ FrameHeader, s SettingsParams) error {
	h.got = s
	h.called = true
	return nil
}

func settingPayload(pairs ...SettingPair) []byte {
	b := make([]byte, 0, len(pairs)*6)
	for _, p := range pairs {
		b = append(b,
			byte(uint16(p.ID)>>8), byte(uint16(p.ID)),
			byte(p.Value>>24), byte(p.Value>>16), byte(p.Value>>8), byte(p.Value))
	}
	return b
}

func readSettings(t *testing.T, payload []byte) SettingsParams {
	t.Helper()
	raw := frameBytes(uint32(len(payload)), FrameSettings, 0, 0, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	fr.SetMaxReadFrameSize(16384)
	h := &settingsCapture{}
	_, err := fr.ReadFrame(context.Background(), h)
	require.NoErrorf(t, err, "ReadFrame: %v (a legal SETTINGS frame must not error)", err)
	require.True(t, h.called, "OnSettings was not called")
	return h.got
}

func lookup(s SettingsParams, id SettingID) (uint32, bool) {
	for i := 0; i < s.N; i++ {
		if s.Pairs[i].ID == id {
			return s.Pairs[i].Value, true
		}
	}
	return 0, false
}

// TestConformance_RFC7540_Sec6_5_ManyParametersAccepted pins that a SETTINGS
// frame carrying more than the internal 16-slot store is accepted, not rejected.
// RFC 7540 §6.5 states only one length rule — "A SETTINGS frame with a length
// other than a multiple of 6 octets MUST be treated as a connection error ... of
// type FRAME_SIZE_ERROR" — and no bound on the parameter count. A server sending
// GREASE reserved settings (RFC 8701) produces >16-parameter frames routinely;
// the old `len(payload)/6 > 16` check tore the whole connection down on them.
func TestConformance_RFC7540_Sec6_5_ManyParametersAccepted(t *testing.T) {
	var pairs []SettingPair
	// 20 distinct undefined identifiers (all ignored) ...
	for id := 0x100; id < 0x114; id++ {
		pairs = append(pairs, SettingPair{ID: SettingID(id), Value: 1})
	}
	// ... then one defined identifier that must still be applied.
	pairs = append(pairs, SettingPair{ID: SettingInitialWindowSize, Value: 4096})

	s := readSettings(t, settingPayload(pairs...))

	v, ok := lookup(s, SettingInitialWindowSize)
	require.Truef(t, ok, "InitialWindowSize = (%d,%v), want (4096,true) — a defined setting after "+
		"20 unknown ids must not be crowded out of the store", v, ok)
	assert.EqualValuesf(t, 4096, v, "InitialWindowSize = (%d,%v), want (4096,true) — a defined setting after "+
		"20 unknown ids must not be crowded out of the store", v, ok)
}

// TestConformance_RFC7540_Sec6_5_LastValueWins pins RFC 7540 §6.5: "the value of
// a SETTINGS parameter is the last value that is seen by a receiver." A repeated
// identifier must resolve to its LAST occurrence, and must occupy a single slot.
func TestConformance_RFC7540_Sec6_5_LastValueWins(t *testing.T) {
	payload := settingPayload(
		SettingPair{ID: SettingMaxFrameSize, Value: 16384},
		SettingPair{ID: SettingMaxFrameSize, Value: 32768},
		SettingPair{ID: SettingMaxFrameSize, Value: 65536},
	)

	s := readSettings(t, payload)

	v, ok := lookup(s, SettingMaxFrameSize)
	require.Truef(t, ok, "MaxFrameSize = (%d,%v), want (65536,true) — last value must win", v, ok)
	assert.EqualValuesf(t, 65536, v, "MaxFrameSize = (%d,%v), want (65536,true) — last value must win", v, ok)
	assert.Equalf(t, 1, s.N, "N = %d, want 1 — a repeated identifier occupies one slot, not three", s.N)
}

// TestConformance_RFC7540_Sec6_5_2_UnknownIgnored pins RFC 7540 §6.5.2: an
// "unsupported identifier MUST ignore that setting." An unknown id contributes
// no slot; a defined one alongside it is still recorded.
func TestConformance_RFC7540_Sec6_5_2_UnknownIgnored(t *testing.T) {
	payload := settingPayload(
		SettingPair{ID: SettingID(0xffff), Value: 123}, // GREASE / reserved
		SettingPair{ID: SettingEnablePush, Value: 0},
	)

	s := readSettings(t, payload)

	_, stored := lookup(s, SettingID(0xffff))
	assert.False(t, stored, "unknown identifier 0xffff was stored; §6.5.2 requires it be ignored")
	v, ok := lookup(s, SettingEnablePush)
	require.Truef(t, ok, "EnablePush = (%d,%v), want (0,true)", v, ok)
	assert.Zerof(t, v, "EnablePush = (%d,%v), want (0,true)", v, ok)
}
