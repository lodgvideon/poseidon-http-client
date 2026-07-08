package http3

import (
	"bytes"
	"io"
	"testing"
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
			if err != nil {
				t.Fatalf("ParseFrameHeader: %v", err)
			}
			if typ != c.typ {
				t.Fatalf("type = %#x, want %#x", typ, c.typ)
			}
			if length != uint64(len(c.payload)) {
				t.Fatalf("length = %d, want %d", length, len(c.payload))
			}
			if got := c.encoded[n : n+int(length)]; !bytes.Equal(got, c.payload) {
				t.Fatalf("payload = %x, want %x", got, c.payload)
			}
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
	if err != nil {
		t.Fatal(err)
	}
	if typ != FrameSettings {
		t.Fatalf("type = %#x, want SETTINGS", typ)
	}
	got, err := ParseSettings(frame[n : n+int(length)])
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(settings) {
		t.Fatalf("got %d settings, want %d", len(got), len(settings))
	}
	for i, s := range settings {
		if got[i] != s {
			t.Fatalf("setting %d = %+v, want %+v", i, got[i], s)
		}
	}
}

// TestConformance_RFC9114_Sec724_DuplicateSetting verifies that a repeated
// SETTINGS identifier is rejected (H3_SETTINGS_ERROR).
func TestConformance_RFC9114_Sec724_DuplicateSetting(t *testing.T) {
	payload := AppendSettings(nil, []Setting{{SettingQPACKBlockedStreams, 1}, {SettingQPACKBlockedStreams, 2}})
	_, length, n, _ := ParseFrameHeader(payload)
	if _, err := ParseSettings(payload[n : n+int(length)]); err != ErrH3Settings {
		t.Fatalf("err = %v, want ErrH3Settings", err)
	}
}

// TestParseFrameHeader_Incomplete verifies that a header split across stream
// reads signals a benign io.ErrUnexpectedEOF, not the fatal H3_FRAME_ERROR — so
// a stream reader buffers more bytes instead of killing the connection.
func TestParseFrameHeader_Incomplete(t *testing.T) {
	if _, _, _, err := ParseFrameHeader(nil); err != io.ErrUnexpectedEOF {
		t.Fatalf("empty: err = %v, want io.ErrUnexpectedEOF", err)
	}
	// Type present (0x04) but the length varint is absent.
	if _, _, _, err := ParseFrameHeader([]byte{0x04}); err != io.ErrUnexpectedEOF {
		t.Fatalf("no-length: err = %v, want io.ErrUnexpectedEOF", err)
	}
	// A 2-byte varint length with only its first byte present.
	if _, _, _, err := ParseFrameHeader([]byte{0x00, 0x40}); err != io.ErrUnexpectedEOF {
		t.Fatalf("truncated-length: err = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestParseSettings_Truncated(t *testing.T) {
	// An identifier with no value.
	if _, err := ParseSettings([]byte{0x06}); err != ErrH3Settings {
		t.Fatalf("err = %v, want ErrH3Settings", err)
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
		if _, err := ParseSettings(payload); err != ErrH3Settings {
			t.Fatalf("reserved id %#x: err = %v, want ErrH3Settings", id, err)
		}
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
