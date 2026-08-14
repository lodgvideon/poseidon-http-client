package frame

import "testing"

func TestFrameType_String(t *testing.T) {
	cases := []struct {
		in   FrameType
		want string
	}{
		{FrameData, "DATA"},
		{FrameHeaders, "HEADERS"},
		{FramePriority, "PRIORITY"},
		{FrameRSTStream, "RST_STREAM"},
		{FrameSettings, "SETTINGS"},
		{FramePushPromise, "PUSH_PROMISE"},
		{FramePing, "PING"},
		{FrameGoAway, "GOAWAY"},
		{FrameWindowUpdate, "WINDOW_UPDATE"},
		{FrameContinuation, "CONTINUATION"},
		{FrameAltSvc, "ALTSVC"},
		{FrameOrigin, "ORIGIN"},
		// 0x0b sits between the two extension types this codec speaks and is
		// registered to nothing here; §5.5 says ignore it, and a trace says so.
		{FrameType(0x0b), "UNKNOWN(0xb)"},
		{FrameType(0xff), "UNKNOWN(0xff)"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Errorf("FrameType(%#x).String() = %q, want %q", uint8(c.in), got, c.want)
		}
	}
}

func TestFlags_StringFor(t *testing.T) {
	cases := []struct {
		typ  FrameType
		fl   Flags
		want string
	}{
		{FrameData, 0, "NONE"},
		{FrameData, FlagDataEndStream, "END_STREAM"},
		{FrameData, FlagDataEndStream | FlagDataPadded, "END_STREAM|PADDED"},
		{FrameHeaders, FlagHeadersEndStream | FlagHeadersEndHeaders, "END_STREAM|END_HEADERS"},
		{FrameHeaders, FlagHeadersPriority | FlagHeadersPadded, "PADDED|PRIORITY"},
		// The whole reason StringFor takes a type: 0x1 is ACK here, not
		// END_STREAM, and rendering it as END_STREAM would show a SETTINGS ACK
		// as a protocol violation.
		{FrameSettings, FlagSettingsAck, "ACK"},
		{FramePing, FlagPingAck, "ACK"},
		{FrameContinuation, FlagContinuationEndHeaders, "END_HEADERS"},
		{FramePushPromise, FlagPushPromiseEndHeaders | FlagPushPromisePadded, "END_HEADERS|PADDED"},
		// Undefined bits survive as numbers rather than being dropped.
		{FrameData, FlagDataEndStream | 0x40, "END_STREAM|0x40"},
		{FrameWindowUpdate, 0x02, "0x2"},
		{FramePriority, 0x80, "0x80"},
	}
	for _, c := range cases {
		if got := c.fl.StringFor(c.typ); got != c.want {
			t.Errorf("Flags(%#x).StringFor(%v) = %q, want %q", uint8(c.fl), c.typ, got, c.want)
		}
	}
}

// TestFlags_String pins the documented lossy default: a bare Flags has no frame
// type, so 0x1 always reads END_STREAM — including on the SETTINGS ACK where
// StringFor knows better.
func TestFlags_String(t *testing.T) {
	if got, want := (FlagHeadersEndStream | FlagHeadersEndHeaders).String(), "END_STREAM|END_HEADERS"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if got, want := FlagSettingsAck.String(), "END_STREAM"; got != want {
		t.Errorf("ambiguous 0x1 rendered %q, want the documented %q", got, want)
	}
	if got, want := Flags(0).String(), "NONE"; got != want {
		t.Errorf("Flags(0).String() = %q, want %q", got, want)
	}
}

func TestFlags_AppendFor_PreservesPrefix(t *testing.T) {
	// AppendFor must extend the caller's buffer, not restart it — the tracer
	// renders a whole line into one slice, and a flag renderer that treated
	// len(b)==0 as "first name" would emit "flags=|END_STREAM".
	b := []byte("flags=")
	got := string((FlagHeadersEndStream | FlagHeadersEndHeaders).AppendFor(b, FrameHeaders))
	if want := "flags=END_STREAM|END_HEADERS"; got != want {
		t.Errorf("AppendFor = %q, want %q", got, want)
	}
}

func TestErrCode_String(t *testing.T) {
	cases := []struct {
		in   ErrCode
		want string
	}{
		{ErrCodeNoError, "NO_ERROR"},
		{ErrCodeProtocolError, "PROTOCOL_ERROR"},
		{ErrCodeInternalError, "INTERNAL_ERROR"},
		{ErrCodeFlowControlError, "FLOW_CONTROL_ERROR"},
		{ErrCodeSettingsTimeout, "SETTINGS_TIMEOUT"},
		{ErrCodeStreamClosed, "STREAM_CLOSED"},
		{ErrCodeFrameSizeError, "FRAME_SIZE_ERROR"},
		{ErrCodeRefusedStream, "REFUSED_STREAM"},
		{ErrCodeCancel, "CANCEL"},
		{ErrCodeCompressionError, "COMPRESSION_ERROR"},
		{ErrCodeConnectError, "CONNECT_ERROR"},
		{ErrCodeEnhanceYourCalm, "ENHANCE_YOUR_CALM"},
		{ErrCodeInadequateSecurity, "INADEQUATE_SECURITY"},
		{ErrCodeHTTP11Required, "HTTP_1_1_REQUIRED"},
		// §7 tells a receiver to HANDLE an unknown code as INTERNAL_ERROR. It
		// must not be RENDERED as one: that would erase what the peer said.
		{ErrCode(0x63), "UNKNOWN_ERROR(0x63)"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Errorf("ErrCode(%#x).String() = %q, want %q", uint32(c.in), got, c.want)
		}
	}
}

func TestSettingID_String(t *testing.T) {
	cases := []struct {
		in   SettingID
		want string
	}{
		{SettingHeaderTableSize, "SETTINGS_HEADER_TABLE_SIZE"},
		{SettingEnablePush, "SETTINGS_ENABLE_PUSH"},
		{SettingMaxConcurrentStreams, "SETTINGS_MAX_CONCURRENT_STREAMS"},
		{SettingInitialWindowSize, "SETTINGS_INITIAL_WINDOW_SIZE"},
		{SettingMaxFrameSize, "SETTINGS_MAX_FRAME_SIZE"},
		{SettingMaxHeaderListSize, "SETTINGS_MAX_HEADER_LIST_SIZE"},
		{SettingEnableConnectProtocol, "SETTINGS_ENABLE_CONNECT_PROTOCOL"},
		// 0x7 is unassigned, and GREASE settings (RFC 8701) arrive routinely.
		{SettingID(0x7), "UNKNOWN_SETTING(0x7)"},
		{SettingID(0x0a0a), "UNKNOWN_SETTING(0xa0a)"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Errorf("SettingID(%#x).String() = %q, want %q", uint16(c.in), got, c.want)
		}
	}
}

func TestFrameHeader_String(t *testing.T) {
	cases := []struct {
		in   FrameHeader
		want string
	}{
		{
			FrameHeader{Type: FrameHeaders, Flags: FlagHeadersEndHeaders | FlagHeadersEndStream, StreamID: 3, Length: 54},
			"HEADERS stream=3 len=54 flags=END_STREAM|END_HEADERS",
		},
		// No flags → no clause at all, so a DATA-heavy log is not mostly
		// "flags=NONE".
		{FrameHeader{Type: FrameData, StreamID: 1, Length: 1024}, "DATA stream=1 len=1024"},
		{FrameHeader{Type: FrameSettings, Flags: FlagSettingsAck}, "SETTINGS stream=0 len=0 flags=ACK"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Errorf("FrameHeader.String() = %q, want %q", got, c.want)
		}
	}
}

// TestErrCodeNamesReachErrors is the payoff the issue predicted for free: the
// conn error types format their code with %v, so naming ErrCode turns
// "code=3" into "code=FLOW_CONTROL_ERROR" in every error string that carries
// one. Asserted here rather than in conn so the frame package owns the promise.
func TestErrCode_SatisfiesStringer(t *testing.T) {
	var s interface{ String() string } = ErrCodeFlowControlError
	if got := s.String(); got != "FLOW_CONTROL_ERROR" {
		t.Fatalf("ErrCode does not render through fmt.Stringer: %q", got)
	}
}
