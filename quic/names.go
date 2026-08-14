package quic

import "strconv"

// FrameTypeName returns the RFC 9000 §19 name of a QUIC frame type —
// "ACK", "MAX_STREAM_DATA" — or "UNKNOWN(0x…)".
//
// It is a function and not a String method for the same reason its HTTP/3
// counterpart is: the frame-type constants here are plain uint64, the type a
// QUIC varint decodes to, and naming that type now would break every caller
// holding one.
//
// STREAM is the one type that is a range rather than a value. §19.8 packs three
// flags into the low bits of the type itself — OFF (0x04), LEN (0x02), FIN
// (0x01) — so the eight values 0x08 through 0x0f are all STREAM frames that
// differ in which fields follow. The flags are rendered, because "STREAM" alone
// leaves out the two facts you are usually chasing: whether the frame carried
// an explicit offset, and whether it closed the stream.
func FrameTypeName(t uint64) string {
	if t >= FrameStreamBase && t <= FrameStreamMax {
		return streamFrameName(t)
	}
	if t < uint64(len(frameNames)) {
		if n := frameNames[t]; n != "" {
			return n
		}
	}
	return "UNKNOWN(0x" + strconv.FormatUint(t, 16) + ")"
}

// frameNames indexes the §19 registry by type. A table rather than a
// twenty-three-arm switch: the switch tripped the cyclomatic-complexity gate,
// and this reads as what it is — a registry — with the frame types as literal
// indices, so a gap is visible rather than buried in a missing case. The
// STREAM range 0x08-0x0f is deliberately empty; FrameTypeName answers it above.
var frameNames = []string{
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

// streamFrameName renders a STREAM frame type with its OFF/LEN/FIN bits
// (RFC 9000 §19.8). A bare 0x08 has none of them and renders as "STREAM".
func streamFrameName(t uint64) string {
	b := make([]byte, 0, 24)
	b = append(b, "STREAM"...)
	sep := byte('[')
	if t&streamOff != 0 {
		b = append(b, sep, 'O', 'F', 'F')
		sep = '|'
	}
	if t&streamLen != 0 {
		b = append(b, sep, 'L', 'E', 'N')
		sep = '|'
	}
	if t&streamFin != 0 {
		b = append(b, sep, 'F', 'I', 'N')
		sep = '|'
	}
	if sep == '|' {
		b = append(b, ']')
	}
	return string(b)
}
