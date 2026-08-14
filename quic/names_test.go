package quic

import "testing"

func TestFrameTypeName(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{FramePadding, "PADDING"},
		{FramePing, "PING"},
		{FrameACK, "ACK"},
		{FrameACKECN, "ACK_ECN"},
		{FrameResetStream, "RESET_STREAM"},
		{FrameStopSending, "STOP_SENDING"},
		{FrameCrypto, "CRYPTO"},
		{FrameNewToken, "NEW_TOKEN"},
		{FrameMaxData, "MAX_DATA"},
		{FrameMaxStreamData, "MAX_STREAM_DATA"},
		{FrameMaxStreamsBidi, "MAX_STREAMS_BIDI"},
		{FrameMaxStreamsUni, "MAX_STREAMS_UNI"},
		{FrameDataBlocked, "DATA_BLOCKED"},
		{FrameStreamDataBlocked, "STREAM_DATA_BLOCKED"},
		{FrameStreamsBlockedBidi, "STREAMS_BLOCKED_BIDI"},
		{FrameStreamsBlockedUni, "STREAMS_BLOCKED_UNI"},
		{FrameNewConnectionID, "NEW_CONNECTION_ID"},
		{FrameRetireConnectionID, "RETIRE_CONNECTION_ID"},
		{FramePathChallenge, "PATH_CHALLENGE"},
		{FramePathResponse, "PATH_RESPONSE"},
		{FrameConnectionClose, "CONNECTION_CLOSE"},
		{FrameConnectionCloseApp, "CONNECTION_CLOSE_APP"},
		{FrameHandshakeDone, "HANDSHAKE_DONE"},
		{0x1f, "UNKNOWN(0x1f)"},
		{0x30, "UNKNOWN(0x30)"},
	}
	for _, c := range cases {
		if got := FrameTypeName(c.in); got != c.want {
			t.Errorf("FrameTypeName(%#x) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestFrameTypeName_Stream covers the one type that is a range: RFC 9000 §19.8
// packs OFF/LEN/FIN into the low three bits of the type itself, so 0x08-0x0f
// are all STREAM. Rendering the bits is the point — "did that frame close the
// stream" is usually the question being asked.
func TestFrameTypeName_Stream(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0x08, "STREAM"},
		{0x09, "STREAM[FIN]"},
		{0x0a, "STREAM[LEN]"},
		{0x0b, "STREAM[LEN|FIN]"},
		{0x0c, "STREAM[OFF]"},
		{0x0d, "STREAM[OFF|FIN]"},
		{0x0e, "STREAM[OFF|LEN]"},
		{0x0f, "STREAM[OFF|LEN|FIN]"},
	}
	for _, c := range cases {
		if got := FrameTypeName(c.in); got != c.want {
			t.Errorf("FrameTypeName(%#x) = %q, want %q", c.in, got, c.want)
		}
	}
}
