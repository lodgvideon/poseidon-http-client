package frame

import "github.com/lodgvideon/poseidon-http-client/internal/bytesx"

// FrameHeaderSize is the wire size of a frame header (RFC 7540 §4.1).
const FrameHeaderSize = 9

// ReadFrameHeader parses the 9-byte prefix from b. b MUST have length ≥ 9.
func ReadFrameHeader(b []byte) (FrameHeader, error) {
	if len(b) < FrameHeaderSize {
		return FrameHeader{}, ErrShortRead
	}
	return FrameHeader{
		Length:   bytesx.ReadUint24(b[:3]),
		Type:     FrameType(b[3]),
		Flags:    Flags(b[4]),
		StreamID: bytesx.ReadUint31(b[5:9]),
	}, nil
}

// WriteFrameHeader writes h into b[:9]. b MUST have length ≥ 9.
func WriteFrameHeader(b []byte, h FrameHeader) {
	_ = b[FrameHeaderSize-1]
	bytesx.WriteUint24(b[:3], h.Length)
	b[3] = byte(h.Type)
	b[4] = byte(h.Flags)
	bytesx.WriteUint31(b[5:9], h.StreamID)
}
