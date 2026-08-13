package quic

import "github.com/lodgvideon/poseidon-http-client/internal/bytesx"

// appendV appends v as a QUIC varint. The 8-byte scratch stays on the stack.
func appendV(dst []byte, v uint64) []byte {
	var b [8]byte
	n := bytesx.WriteVarint(b[:], v)
	return append(dst, b[:n]...)
}

// appendPadding appends n PADDING frames (n zero bytes).
func appendPadding(dst []byte, n int) []byte {
	for i := 0; i < n; i++ {
		dst = append(dst, 0x00)
	}
	return dst
}

// appendPing appends a PING frame.
func appendPing(dst []byte) []byte { return append(dst, byte(FramePing)) }

// appendHandshakeDone appends a HANDSHAKE_DONE frame (server-only in practice).
func appendHandshakeDone(dst []byte) []byte { return append(dst, byte(FrameHandshakeDone)) }

// AppendAck appends an ACK frame (RFC 9000 §19.3). ranges are the additional
// ranges after First ACK Range, in wire order.
func AppendAck(dst []byte, largestAcked, ackDelay, firstAckRange uint64, ranges []AckRange) []byte {
	dst = append(dst, byte(FrameACK))
	dst = appendV(dst, largestAcked)
	dst = appendV(dst, ackDelay)
	dst = appendV(dst, uint64(len(ranges)))
	dst = appendV(dst, firstAckRange)
	for i := range ranges {
		dst = appendV(dst, ranges[i].Gap)
		dst = appendV(dst, ranges[i].Length)
	}
	return dst
}

// AppendResetStream appends a RESET_STREAM frame.
func AppendResetStream(dst []byte, streamID, errCode, finalSize uint64) []byte {
	dst = append(dst, byte(FrameResetStream))
	dst = appendV(dst, streamID)
	dst = appendV(dst, errCode)
	return appendV(dst, finalSize)
}

// appendStopSending appends a STOP_SENDING frame.
func appendStopSending(dst []byte, streamID, errCode uint64) []byte {
	dst = append(dst, byte(FrameStopSending))
	dst = appendV(dst, streamID)
	return appendV(dst, errCode)
}

// AppendCrypto appends a CRYPTO frame carrying data at the given stream offset.
func AppendCrypto(dst []byte, offset uint64, data []byte) []byte {
	dst = append(dst, byte(FrameCrypto))
	dst = appendV(dst, offset)
	dst = appendV(dst, uint64(len(data)))
	return append(dst, data...)
}

// appendNewToken appends a NEW_TOKEN frame.
func appendNewToken(dst []byte, token []byte) []byte {
	dst = append(dst, byte(FrameNewToken))
	dst = appendV(dst, uint64(len(token)))
	return append(dst, token...)
}

// AppendStream appends a STREAM frame with an explicit Length field. The Offset
// field is emitted only when offset != 0 (offset 0 is the default). fin sets the
// FIN bit.
func AppendStream(dst []byte, streamID, offset uint64, fin bool, data []byte) []byte {
	typ := byte(FrameStreamBase) | streamLen
	if offset != 0 {
		typ |= streamOff
	}
	if fin {
		typ |= streamFin
	}
	dst = append(dst, typ)
	dst = appendV(dst, streamID)
	if offset != 0 {
		dst = appendV(dst, offset)
	}
	dst = appendV(dst, uint64(len(data)))
	return append(dst, data...)
}

// AppendMaxData appends a MAX_DATA frame.
func AppendMaxData(dst []byte, maximum uint64) []byte {
	dst = append(dst, byte(FrameMaxData))
	return appendV(dst, maximum)
}

// AppendMaxStreamData appends a MAX_STREAM_DATA frame.
func AppendMaxStreamData(dst []byte, streamID, maximum uint64) []byte {
	dst = append(dst, byte(FrameMaxStreamData))
	dst = appendV(dst, streamID)
	return appendV(dst, maximum)
}

// AppendMaxStreams appends a MAX_STREAMS frame (bidirectional unless uni).
func AppendMaxStreams(dst []byte, uni bool, maximum uint64) []byte {
	if uni {
		dst = append(dst, byte(FrameMaxStreamsUni))
	} else {
		dst = append(dst, byte(FrameMaxStreamsBidi))
	}
	return appendV(dst, maximum)
}

// appendDataBlocked appends a DATA_BLOCKED frame.
func appendDataBlocked(dst []byte, limit uint64) []byte {
	dst = append(dst, byte(FrameDataBlocked))
	return appendV(dst, limit)
}

// appendStreamDataBlocked appends a STREAM_DATA_BLOCKED frame.
func appendStreamDataBlocked(dst []byte, streamID, limit uint64) []byte {
	dst = append(dst, byte(FrameStreamDataBlocked))
	dst = appendV(dst, streamID)
	return appendV(dst, limit)
}

// appendStreamsBlocked appends a STREAMS_BLOCKED frame (bidirectional unless uni).
func appendStreamsBlocked(dst []byte, uni bool, limit uint64) []byte {
	if uni {
		dst = append(dst, byte(FrameStreamsBlockedUni))
	} else {
		dst = append(dst, byte(FrameStreamsBlockedBidi))
	}
	return appendV(dst, limit)
}

// appendRetireConnectionID appends a RETIRE_CONNECTION_ID frame.
func appendRetireConnectionID(dst []byte, seq uint64) []byte {
	dst = append(dst, byte(FrameRetireConnectionID))
	return appendV(dst, seq)
}

// appendPathResponse appends a PATH_RESPONSE frame echoing an 8-byte challenge.
func appendPathResponse(dst []byte, data [8]byte) []byte {
	dst = append(dst, byte(FramePathResponse))
	return append(dst, data[:]...)
}

// AppendConnectionClose appends a CONNECTION_CLOSE frame. When app is true it is
// the application variant (0x1d, no Frame Type field); otherwise the transport
// variant (0x1c) with frameType.
func AppendConnectionClose(dst []byte, app bool, errCode, frameType uint64, reason []byte) []byte {
	if app {
		dst = append(dst, byte(FrameConnectionCloseApp))
		dst = appendV(dst, errCode)
	} else {
		dst = append(dst, byte(FrameConnectionClose))
		dst = appendV(dst, errCode)
		dst = appendV(dst, frameType)
	}
	dst = appendV(dst, uint64(len(reason)))
	return append(dst, reason...)
}
