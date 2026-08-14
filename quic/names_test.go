package quic

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/trace"
)

func TestFrameTypeName(t *testing.T) {
	for typ, want := range map[uint64]string{
		FramePadding: "PADDING", FramePing: "PING", FrameACK: "ACK", FrameACKECN: "ACK_ECN",
		FrameResetStream: "RESET_STREAM", FrameStopSending: "STOP_SENDING",
		FrameCrypto: "CRYPTO", FrameNewToken: "NEW_TOKEN",
		FrameMaxData: "MAX_DATA", FrameMaxStreamData: "MAX_STREAM_DATA",
		FrameMaxStreamsBidi: "MAX_STREAMS_BIDI", FrameMaxStreamsUni: "MAX_STREAMS_UNI",
		FrameDataBlocked: "DATA_BLOCKED", FrameStreamDataBlocked: "STREAM_DATA_BLOCKED",
		FrameStreamsBlockedBidi: "STREAMS_BLOCKED_BIDI", FrameStreamsBlockedUni: "STREAMS_BLOCKED_UNI",
		FrameNewConnectionID: "NEW_CONNECTION_ID", FrameRetireConnectionID: "RETIRE_CONNECTION_ID",
		FramePathChallenge: "PATH_CHALLENGE", FramePathResponse: "PATH_RESPONSE",
		FrameConnectionClose: "CONNECTION_CLOSE", FrameConnectionCloseApp: "CONNECTION_CLOSE_APP",
		FrameHandshakeDone: "HANDSHAKE_DONE",
		0x1f:               trace.UnknownName,
	} {
		if got := FrameTypeName(typ); got != want {
			t.Errorf("FrameTypeName(%#x) = %q, want %q", typ, got, want)
		}
	}
}

// TestFrameTypeName_StreamRange: §19.8 gives STREAM the whole 0x08-0x0f range,
// with OFF/LEN/FIN in the low three bits. All eight are one frame type.
func TestFrameTypeName_StreamRange(t *testing.T) {
	for typ := FrameStreamBase; typ <= FrameStreamMax; typ++ {
		if got := FrameTypeName(typ); got != "STREAM" {
			t.Errorf("FrameTypeName(%#x) = %q, want STREAM", typ, got)
		}
	}
	if got := FrameTypeName(FrameStreamMax + 1); got == "STREAM" {
		t.Errorf("FrameTypeName(%#x) = STREAM; the range ends at %#x", FrameStreamMax+1, FrameStreamMax)
	}
}
