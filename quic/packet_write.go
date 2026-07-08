package quic

import "encoding/binary"

// AppendLongHeader appends an Initial, 0-RTT, or Handshake long header through
// the Length field (RFC 9000 §17.2). pnLen is the packet-number length in bytes
// (1–4), encoded into the low two bits of the first byte (header-protected on
// the wire). length is the byte count of the packet number plus payload that
// the caller will append next. It returns the extended slice and the offset at
// which the caller appends the packet number. Retry / Version Negotiation are
// server-only and have no writer.
func AppendLongHeader(dst []byte, typ PacketType, version uint32, dcid, scid, token []byte, pnLen int, length uint64) ([]byte, int) {
	first := byte(0xc0) // long form + fixed bit
	switch typ {
	case PacketZeroRTT:
		first |= 0x10
	case PacketHandshake:
		first |= 0x20
	default:
		// PacketInitial has type bits 00.
	}
	first |= byte(pnLen-1) & 0x03
	dst = append(dst, first)
	dst = binary.BigEndian.AppendUint32(dst, version)
	dst = append(dst, byte(len(dcid)))
	dst = append(dst, dcid...)
	dst = append(dst, byte(len(scid)))
	dst = append(dst, scid...)
	if typ == PacketInitial {
		dst = appendV(dst, uint64(len(token)))
		dst = append(dst, token...)
	}
	dst = appendV(dst, length)
	return dst, len(dst)
}

// AppendShortHeader appends a short (1-RTT) header (RFC 9000 §17.3): the first
// byte (with the header-protected key-phase and packet-number-length bits) then
// the destination connection ID. It returns the extended slice and the
// packet-number offset.
func AppendShortHeader(dst []byte, dcid []byte, pnLen int, keyPhase bool) ([]byte, int) {
	first := byte(0x40) // short form + fixed bit
	if keyPhase {
		first |= 0x04
	}
	first |= byte(pnLen-1) & 0x03
	dst = append(dst, first)
	dst = append(dst, dcid...)
	return dst, len(dst)
}
