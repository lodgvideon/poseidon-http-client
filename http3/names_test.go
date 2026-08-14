package http3

import "testing"

func TestFrameTypeName(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{FrameData, "DATA"},
		{FrameHeaders, "HEADERS"},
		{FrameCancelPush, "CANCEL_PUSH"},
		{FrameSettings, "SETTINGS"},
		{FramePushPromise, "PUSH_PROMISE"},
		{FrameGoaway, "GOAWAY"},
		{FrameMaxPushID, "MAX_PUSH_ID"},
		// The h2-carryover types RFC 9114 §11.2.1 reserves. Receiving one is an
		// H3_FRAME_UNEXPECTED connection error, and naming it is the difference
		// between "0x06 killed the connection" and "the peer spoke HTTP/2 at
		// us".
		{0x02, "RESERVED_H2_PRIORITY"},
		{0x06, "RESERVED_H2_PING"},
		{0x08, "RESERVED_H2_WINDOW_UPDATE"},
		{0x09, "RESERVED_H2_CONTINUATION"},
		// GREASE, §7.2.8: 0x1f*N + 0x21.
		{0x21, "GREASE(0x21)"},
		{0x40, "GREASE(0x40)"},
		{0x1f*7 + 0x21, "GREASE(0xfa)"},
		{0x0b, "UNKNOWN(0xb)"},
		{0x22, "UNKNOWN(0x22)"},
	}
	for _, c := range cases {
		if got := FrameTypeName(c.in); got != c.want {
			t.Errorf("FrameTypeName(%#x) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestIsGreaseType_NoWrap: the reserved-value test subtracts 0x21 from a varint
// that comes straight off the wire, and an unguarded subtraction wraps for every
// type below 0x21 — which is all the real ones.
func TestIsGreaseType_NoWrap(t *testing.T) {
	for t0 := uint64(0); t0 < 0x21; t0++ {
		if isGreaseType(t0) {
			t.Fatalf("type %#x reported as GREASE", t0)
		}
	}
}

func TestSettingName(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{SettingQPACKMaxTableCapacity, "SETTINGS_QPACK_MAX_TABLE_CAPACITY"},
		{SettingMaxFieldSectionSize, "SETTINGS_MAX_FIELD_SECTION_SIZE"},
		{SettingQPACKBlockedStreams, "SETTINGS_QPACK_BLOCKED_STREAMS"},
		{0x21, "GREASE(0x21)"},
		{0x02, "UNKNOWN(0x2)"},
	}
	for _, c := range cases {
		if got := SettingName(c.in); got != c.want {
			t.Errorf("SettingName(%#x) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestErrorCodeName(t *testing.T) {
	if got, want := ErrorCodeName(H3FrameUnexpected), "H3_FRAME_UNEXPECTED"; got != want {
		t.Errorf("ErrorCodeName = %q, want %q", got, want)
	}
	if got, want := ErrorCodeName(0x9999), "UNKNOWN(0x9999)"; got != want {
		t.Errorf("ErrorCodeName = %q, want %q", got, want)
	}
}
