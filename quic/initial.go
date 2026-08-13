package quic

import "github.com/lodgvideon/poseidon-http-client/internal/bytesx"

// InitialDatagramMinSize is the minimum size of a datagram carrying a client
// Initial packet (RFC 9000 §14.1); the client pads to it so the server's
// anti-amplification limit is large enough to send its whole flight.
const InitialDatagramMinSize = 1200

// buildInitialPacket assembles and protects a client Initial packet carrying
// cryptoData (the TLS ClientHello) in a CRYPTO frame, padded with PADDING frames
// so the datagram is at least minSize bytes (use InitialDatagramMinSize for the
// first flight). dcid is the server's connection ID (the client's random
// original DCID before Retry), scid the client's chosen source ID (may be
// empty). token is the Retry/NEW_TOKEN token (nil for a first Initial). pn is
// the Initial packet number, pnLen its on-wire length (1–4). It appends the
// protected packet to dst.
func buildInitialPacket(dst []byte, s *Sealer, dcid, scid, token []byte, pn uint64, pnLen int, cryptoOffset uint64, cryptoData []byte, minSize int) ([]byte, error) {
	payload := AppendCrypto(nil, cryptoOffset, cryptoData)

	// Header length assuming a 2-byte Length varint (true for any datagram near
	// 1200: 63 < Length < 16384). tokenLen is itself a varint.
	tokLenBytes := bytesx.VarintLen(uint64(len(token)))
	hdrLen := 1 + 4 + 1 + len(dcid) + 1 + len(scid) + tokLenBytes + len(token) + 2
	overhead := hdrLen + pnLen + 16 // header + packet number + AEAD tag
	if need := minSize - overhead - len(payload); need > 0 {
		payload = append(payload, make([]byte, need)...) // PADDING frames (zeros)
	}

	length := uint64(pnLen + len(payload) + 16)
	if length < 64 || length >= 16384 {
		return nil, ErrPacketEncoding // Length would not be a 2-byte varint
	}
	hdr, pnOffset := AppendLongHeader(nil, PacketInitial, QUICVersion1, dcid, scid, token, pnLen, length)
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	if len(hdr) != pnOffset+pnLen {
		return nil, ErrPacketEncoding // header-size assumption broke
	}
	return s.Seal(dst, hdr, pnOffset, pnLen, pn, payload)
}
