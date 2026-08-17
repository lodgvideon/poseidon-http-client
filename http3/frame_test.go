package http3

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9114_Sec72_FrameRoundTrip writes each frame a client emits
// and parses the header back, checking the type, declared length, and payload.
func TestConformance_RFC9114_Sec72_FrameRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		encoded []byte
		typ     uint64
		payload []byte
	}{
		{"data", AppendData(nil, []byte("request body")), FrameData, []byte("request body")},
		{"headers", AppendHeaders(nil, []byte{0x00, 0x00, 0xc1, 0xd1}), FrameHeaders, []byte{0x00, 0x00, 0xc1, 0xd1}},
		{"data_empty", AppendData(nil, nil), FrameData, nil},
		{"goaway", AppendGoaway(nil, 12), FrameGoaway, []byte{0x0c}},
		{"max_push_id", AppendMaxPushID(nil, 100), FrameMaxPushID, []byte{0x40, 0x64}},
		{"cancel_push", AppendCancelPush(nil, 3), FrameCancelPush, []byte{0x03}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			typ, length, n, err := ParseFrameHeader(c.encoded)

			require.NoError(t, err, "ParseFrameHeader over a frame this client just wrote")
			assert.Equalf(t, c.typ, typ, "type = %#x, want %#x", typ, c.typ)
			// Fatal, not just reported: the payload slice below is cut with this
			// length, so a wrong one is a panic rather than a diagnosis.
			require.Equalf(t, uint64(len(c.payload)), length, "length = %d, want %d", length, len(c.payload))
			got := c.encoded[n : n+int(length)]
			assert.Truef(t, bytes.Equal(got, c.payload), "payload = %x, want %x", got, c.payload)
		})
	}
}

func TestConformance_RFC9114_Sec724_SettingsRoundTrip(t *testing.T) {
	settings := []Setting{
		{SettingQPACKMaxTableCapacity, 4096},
		{SettingMaxFieldSectionSize, 1 << 20},
		{SettingQPACKBlockedStreams, 16},
	}
	frame := AppendSettings(nil, settings)

	typ, length, n, err := ParseFrameHeader(frame)
	require.NoError(t, err, "ParseFrameHeader over a SETTINGS frame this client just wrote")
	require.LessOrEqualf(t, n+int(length), len(frame),
		"the declared length %d overruns the %d-byte frame we wrote", length, len(frame))
	got, parseErr := ParseSettings(frame[n : n+int(length)])

	assert.Equalf(t, FrameSettings, typ, "type = %#x, want SETTINGS", typ)
	require.NoError(t, parseErr, "ParseSettings over our own SETTINGS payload")
	require.Lenf(t, got, len(settings), "got %d settings, want %d", len(got), len(settings))
	for i, s := range settings {
		assert.Equalf(t, s, got[i], "setting %d = %+v, want %+v", i, got[i], s)
	}
}

// TestConformance_RFC9114_Sec724_DuplicateSetting verifies that a repeated
// SETTINGS identifier is rejected (H3_SETTINGS_ERROR).
func TestConformance_RFC9114_Sec724_DuplicateSetting(t *testing.T) {
	payload := AppendSettings(nil, []Setting{{SettingQPACKBlockedStreams, 1}, {SettingQPACKBlockedStreams, 2}})
	_, length, n, hdrErr := ParseFrameHeader(payload)
	require.NoError(t, hdrErr, "ParseFrameHeader over the hand-built duplicate-setting frame")

	_, err := ParseSettings(payload[n : n+int(length)])

	assert.Equalf(t, ErrH3Settings, err,
		"err = %v, want ErrH3Settings: §7.2.4 forbids repeating an identifier, and an "+
			"endpoint that accepts one silently applies whichever value it saw last", err)
}

// TestParseFrameHeader_Incomplete verifies that a header split across stream
// reads signals a benign io.ErrUnexpectedEOF, not the fatal H3_FRAME_ERROR — so
// a stream reader buffers more bytes instead of killing the connection.
func TestParseFrameHeader_Incomplete(t *testing.T) {
	cases := []struct {
		name string
		b    []byte
	}{
		{"empty", nil},
		// Type present (0x04) but the length varint is absent.
		{"no-length", []byte{0x04}},
		// A 2-byte varint length with only its first byte present.
		{"truncated-length", []byte{0x00, 0x40}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, _, err := ParseFrameHeader(c.b)

			assert.Equalf(t, io.ErrUnexpectedEOF, err,
				"%s: err = %v, want io.ErrUnexpectedEOF — a header split across stream reads "+
					"must read as 'need more bytes', or the reader kills a healthy connection",
				c.name, err)
		})
	}
}

// TestConformance_RFC9114_Sec71_SettingsTruncatedIsFrameError checks that a
// SETTINGS payload whose identifier/value is cut off by the frame length is an
// H3_FRAME_ERROR (RFC 9114 §7.1), not an H3_SETTINGS_ERROR.
func TestConformance_RFC9114_Sec71_SettingsTruncatedIsFrameError(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"identifier with no value", []byte{0x06}},
		// 0x40 begins a 2-byte varint whose second byte the frame length cuts off.
		{"value varint cut off mid-encoding", []byte{0x06, 0x40}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseSettings(c.payload)

			assert.Equalf(t, ErrH3Frame, err,
				"%s: err = %v, want ErrH3Frame — a field cut off by the frame length is a "+
					"framing fault (§7.1), and reporting it as H3_SETTINGS_ERROR sends the "+
					"peer the wrong close code", c.name, err)
		})
	}
}

// TestConformance_RFC9114_Sec7241_ReservedSetting verifies that a reserved
// HTTP/2-carryover setting identifier (0x02–0x05) is rejected (§7.2.4.1 MUST →
// H3_SETTINGS_ERROR).
func TestConformance_RFC9114_Sec7241_ReservedSetting(t *testing.T) {
	for _, id := range []uint64{0x02, 0x03, 0x04, 0x05} {
		// Hand-build a SETTINGS payload with the reserved id (AppendSettings
		// would compute a valid frame; we only need the payload pairs).
		payload := append(appendV(nil, id), appendV(nil, 1)...)

		_, err := ParseSettings(payload)

		assert.Equalf(t, ErrH3Settings, err,
			"reserved id %#x: err = %v, want ErrH3Settings — §7.2.4.1 makes every "+
				"HTTP/2-carryover identifier in 0x02-0x05 a MUST-reject", id, err)
	}
}

func BenchmarkAppendFrameHeader(b *testing.B) {
	dst := make([]byte, 0, 16)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = AppendFrameHeader(dst[:0], FrameData, 16384)
	}
}

func BenchmarkParseFrameHeader(b *testing.B) {
	hdr := AppendFrameHeader(nil, FrameHeaders, 16384)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = ParseFrameHeader(hdr)
	}
}

func BenchmarkAppendSettings(b *testing.B) {
	dst := make([]byte, 0, 32)
	settings := []Setting{{SettingQPACKMaxTableCapacity, 4096}, {SettingMaxFieldSectionSize, 65536}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = AppendSettings(dst[:0], settings)
	}
}
