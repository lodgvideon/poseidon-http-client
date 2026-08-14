package quic

import "github.com/lodgvideon/poseidon-http-client/trace"

// frameTypeNames maps the single-value frame types to their names. A dense
// array rather than a switch because the types are contiguous 0x00-0x1e and a
// twenty-four-arm switch is over the gocyclo budget this repo enforces; an
// empty slot is a type the registry does not define.
var frameTypeNames = [FrameHandshakeDone + 1]string{
	FramePadding:            "PADDING",
	FramePing:               "PING",
	FrameACK:                "ACK",
	FrameACKECN:             "ACK_ECN",
	FrameResetStream:        "RESET_STREAM",
	FrameStopSending:        "STOP_SENDING",
	FrameCrypto:             "CRYPTO",
	FrameNewToken:           "NEW_TOKEN",
	FrameMaxData:            "MAX_DATA",
	FrameMaxStreamData:      "MAX_STREAM_DATA",
	FrameMaxStreamsBidi:     "MAX_STREAMS_BIDI",
	FrameMaxStreamsUni:      "MAX_STREAMS_UNI",
	FrameDataBlocked:        "DATA_BLOCKED",
	FrameStreamDataBlocked:  "STREAM_DATA_BLOCKED",
	FrameStreamsBlockedBidi: "STREAMS_BLOCKED_BIDI",
	FrameStreamsBlockedUni:  "STREAMS_BLOCKED_UNI",
	FrameNewConnectionID:    "NEW_CONNECTION_ID",
	FrameRetireConnectionID: "RETIRE_CONNECTION_ID",
	FramePathChallenge:      "PATH_CHALLENGE",
	FramePathResponse:       "PATH_RESPONSE",
	FrameConnectionClose:    "CONNECTION_CLOSE",
	FrameConnectionCloseApp: "CONNECTION_CLOSE_APP",
	FrameHandshakeDone:      "HANDSHAKE_DONE",
}

// FrameTypeName returns the RFC 9000 §12.4 name of a QUIC frame type, or
// trace.UnknownName for one outside the registry.
//
// STREAM is the one type that is a range rather than a value: 0x08-0x0f, where
// the low three bits carry the OFF, LEN and FIN flags (§19.8). All eight map to
// "STREAM" — the flags are frame content, not a different frame — which is why
// the table above leaves that span empty and this checks it first.
//
// It is a function rather than a String method for the same reason as the
// HTTP/3 one: the constants are bare uint64 compared against varints read off
// the wire.
func FrameTypeName(t uint64) string {
	if t >= FrameStreamBase && t <= FrameStreamMax {
		return "STREAM"
	}
	if t < uint64(len(frameTypeNames)) {
		if n := frameTypeNames[t]; n != "" {
			return n
		}
	}
	return trace.UnknownName
}
