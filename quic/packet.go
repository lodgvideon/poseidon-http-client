package quic

import "encoding/binary"

// PacketType identifies a QUIC packet (RFC 9000 §17).
type PacketType uint8

const (
	PacketInitial            PacketType = iota // long header, type bits 00
	PacketZeroRTT                              // long header, type bits 01
	PacketHandshake                            // long header, type bits 10
	PacketRetry                                // long header, type bits 11
	PacketShort                                // short header (1-RTT)
	PacketVersionNegotiation                   // long form, Version == 0
)

// QUICVersion1 is the QUIC v1 version number (RFC 9000 §15).
const QUICVersion1 uint32 = 0x00000001

// Header is a parsed QUIC packet header. Byte slices alias the input packet.
// PNOffset is the offset of the (still header-protected) packet number, or 0
// when the packet carries none (Retry, Version Negotiation). PacketLen is the
// total bytes this packet occupies within a (possibly coalesced) datagram.
type Header struct {
	Type      PacketType
	Version   uint32
	DCID      []byte
	SCID      []byte // long headers only
	Token     []byte // Initial packets (Token) and Retry packets (Retry Token)
	Length    uint64 // Initial/0-RTT/Handshake: byte count of packet number + payload
	PNOffset  int
	PacketLen int
}

// ParseHeader parses the header of the QUIC packet at the front of pkt. dcidLen
// is the length of the local (client-chosen) connection ID, needed to locate
// the implicit-length DCID in a short header; it is ignored for long headers.
// It parses only the unprotected header fields — the packet number and payload
// remain header-protected / AEAD-encrypted (removed by the RFC 9001 layer).
func ParseHeader(pkt []byte, dcidLen int) (Header, error) {
	if len(pkt) < 1 {
		return Header{}, ErrPacketEncoding
	}
	if pkt[0]&0x80 == 0 {
		return parseShortHeader(pkt, dcidLen)
	}
	return parseLongHeader(pkt)
}

// vnOffers reports whether a Version Negotiation packet's Supported Versions list
// includes version v. The list is a sequence of 4-byte versions following the
// DCID and SCID (RFC 9000 §17.2.1); hdr is the parsed VN header for pkt. A
// trailing partial version (not a multiple of 4) is ignored.
func vnOffers(pkt []byte, hdr Header, v uint32) bool {
	off := 7 + len(hdr.DCID) + len(hdr.SCID) // byte0 + version(4) + dcidLen(1) + DCID + scidLen(1) + SCID
	for off+4 <= len(pkt) {
		if binary.BigEndian.Uint32(pkt[off:]) == v {
			return true
		}
		off += 4
	}
	return false
}

func parseLongHeader(pkt []byte) (Header, error) {
	if len(pkt) < 5 {
		return Header{}, ErrPacketEncoding
	}
	b0 := pkt[0]
	h := Header{Version: binary.BigEndian.Uint32(pkt[1:5])}
	p := 5
	var ok bool
	if h.DCID, p, ok = readCID(pkt, p); !ok {
		return Header{}, ErrPacketEncoding
	}
	if h.SCID, p, ok = readCID(pkt, p); !ok {
		return Header{}, ErrPacketEncoding
	}
	if h.Version == 0 {
		// Version Negotiation: the remainder is a list of supported versions.
		h.Type = PacketVersionNegotiation
		h.PacketLen = len(pkt)
		return h, nil
	}
	if b0&0x40 == 0 {
		return Header{}, ErrPacketEncoding // fixed bit MUST be 1 (RFC 9000 §17.2)
	}
	switch (b0 >> 4) & 0x03 {
	case 0:
		h.Type = PacketInitial
		tokLen, p2, ok2 := readV(pkt, p)
		if !ok2 || tokLen > uint64(len(pkt)-p2) {
			return Header{}, ErrPacketEncoding
		}
		h.Token = pkt[p2 : p2+int(tokLen)]
		return finishLong(h, pkt, p2+int(tokLen))
	case 1:
		h.Type = PacketZeroRTT
		return finishLong(h, pkt, p)
	case 2:
		h.Type = PacketHandshake
		return finishLong(h, pkt, p)
	default:
		// Retry (§17.2.5): the rest is the Retry Token followed by a 16-byte
		// Retry Integrity Tag; no Length or Packet Number.
		h.Type = PacketRetry
		if len(pkt)-p < 16 {
			return Header{}, ErrPacketEncoding
		}
		h.Token = pkt[p : len(pkt)-16]
		h.PacketLen = len(pkt)
		return h, nil
	}
}

// finishLong reads the Length field (Initial/0-RTT/Handshake) and records where
// the protected packet number begins.
func finishLong(h Header, pkt []byte, p int) (Header, error) {
	length, p2, ok := readV(pkt, p)
	if !ok || length > uint64(len(pkt)-p2) {
		return Header{}, ErrPacketEncoding
	}
	h.Length = length
	h.PNOffset = p2
	h.PacketLen = p2 + int(length)
	return h, nil
}

func parseShortHeader(pkt []byte, dcidLen int) (Header, error) {
	if pkt[0]&0x40 == 0 {
		return Header{}, ErrPacketEncoding // fixed bit MUST be 1 (RFC 9000 §17.3)
	}
	if dcidLen < 0 || dcidLen > 20 || len(pkt) < 1+dcidLen {
		return Header{}, ErrPacketEncoding
	}
	return Header{
		Type:      PacketShort,
		DCID:      pkt[1 : 1+dcidLen],
		PNOffset:  1 + dcidLen,
		PacketLen: len(pkt), // a short-header packet is the last in its datagram
	}, nil
}

// readCID reads a 1-byte length prefix and that many connection-ID bytes.
func readCID(pkt []byte, p int) (cid []byte, np int, ok bool) {
	if p >= len(pkt) {
		return nil, p, false
	}
	l := int(pkt[p])
	p++
	if l > 20 || len(pkt)-p < l {
		return nil, p, false
	}
	return pkt[p : p+l], p + l, true
}
