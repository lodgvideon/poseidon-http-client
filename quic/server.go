package quic

import "errors"

// This file holds the server-role entry points to the QUIC transport. The rest
// of the package drives the client (dialing) role; server support is being added
// in phases (the "S-series"), reusing the shared packet-protection, frame, and
// handshake primitives. AcceptInitial is the first step a server takes for an
// inbound connection: it turns a client's first datagram into the ClientHello
// and connection IDs needed to start a server-side handshake (NewServerHandshake).

// ErrNotInitial is returned by AcceptInitial when the datagram does not begin
// with a QUIC v1 Initial packet.
var ErrNotInitial = errors.New("quic: datagram is not an Initial packet")

// ClientInitial is what a server extracts from a client's first Initial packet
// (RFC 9000 §17.2.2): the client's connection IDs, any address-validation token,
// and the reassembled CRYPTO stream carrying the TLS ClientHello.
type ClientInitial struct {
	// DCID is the Destination Connection ID the client chose. The server derives
	// Initial keys from it (RFC 9001 §5.2) and echoes it back as the
	// original_destination_connection_id transport parameter (RFC 9000 §7.3).
	DCID []byte
	// SCID is the client's Source Connection ID; the server uses it as the
	// Destination Connection ID on the packets it sends back.
	SCID []byte
	// Token is the address-validation token: empty on a first flight, non-empty
	// when the client echoes a Retry or NEW_TOKEN token (RFC 9000 §8.1).
	Token []byte
	// CryptoData is the reassembled CRYPTO stream (the TLS ClientHello) carried by
	// this Initial.
	CryptoData []byte
}

// AcceptInitial parses and decrypts the Initial packet at the front of datagram,
// returning the client's connection IDs and the ClientHello it carries. Initial
// keys are derived from the client's Destination Connection ID (RFC 9001 §5.2),
// so this needs no prior connection state — it is the first step a server takes
// for an inbound connection. It returns ErrNotInitial if the datagram is not an
// Initial, or a decoding/decryption error if the packet is malformed or
// inauthentic.
//
// CRYPTO reassembly assumes the ClientHello starts at offset 0 and is gap-free
// within this datagram (true for a first Initial). Reassembling a ClientHello
// that spans multiple datagrams is the connection engine's responsibility.
func AcceptInitial(datagram []byte) (*ClientInitial, error) {
	hdr, err := ParseHeader(datagram, 0)
	if err != nil {
		return nil, err
	}
	if hdr.Type != PacketInitial {
		return nil, ErrNotInitial
	}
	// Initial packets are protected with keys derived from the client's DCID.
	clientKeys, _ := InitialKeys(hdr.DCID)
	opener, err := NewOpener(clientKeys)
	if err != nil {
		return nil, err
	}
	_, _, payload, err := opener.Open(datagram, hdr.PNOffset, 0)
	if err != nil {
		return nil, err
	}
	var cr cryptoReassembler
	if err := ParseFrames(payload, &cr); err != nil {
		return nil, err
	}
	return &ClientInitial{
		DCID:       append([]byte(nil), hdr.DCID...),
		SCID:       append([]byte(nil), hdr.SCID...),
		Token:      append([]byte(nil), hdr.Token...),
		CryptoData: cr.assembled(),
	}, nil
}

// maxInitialCrypto bounds the ClientHello a single Initial flight may carry. It
// guards AcceptInitial against a hostile CRYPTO offset forcing a large buffer;
// 64 KiB is far above any legitimate ClientHello.
const maxInitialCrypto = 1 << 16

// cryptoReassembler collects the CRYPTO frames of one packet into a contiguous
// buffer indexed by offset (RFC 9000 §19.6), bounded by maxInitialCrypto. It is
// a FrameHandler that ignores every non-CRYPTO frame (e.g. the PADDING that pads
// an Initial to its minimum datagram size).
type cryptoReassembler struct {
	nopFrameHandler
	buf []byte
	end int
}

func (c *cryptoReassembler) OnCrypto(offset uint64, data []byte) error {
	end := offset + uint64(len(data))
	if end > maxInitialCrypto {
		return ErrCryptoBufferExceeded
	}
	if int(end) > len(c.buf) {
		c.buf = append(c.buf, make([]byte, int(end)-len(c.buf))...)
	}
	copy(c.buf[offset:end], data)
	if int(end) > c.end {
		c.end = int(end)
	}
	return nil
}

// assembled returns the contiguous CRYPTO bytes from offset 0.
func (c *cryptoReassembler) assembled() []byte { return c.buf[:c.end] }

// SealPacket builds and protects a single QUIC packet carrying payload (already
// framed) and appends it to dst. typ selects the form: PacketInitial,
// PacketHandshake, and PacketZeroRTT use a long header (dcid, scid, and — for
// Initial only — token); PacketShort uses a 1-RTT short header (dcid only). pn is
// the full packet number and pnLen its on-wire length (1–4).
//
// The payload is padded with PADDING frames when it is too short for header
// protection to sample (RFC 9001 §5.4.2). Unlike BuildInitialPacket it does not
// pad to the 1200-byte anti-amplification floor — a server bounds its early
// flights to 3× the bytes it has received, so that padding is the caller's call.
// It is the send-side counterpart to AcceptInitial, used to assemble the server's
// response flights.
func SealPacket(dst []byte, s *Sealer, typ PacketType, dcid, scid, token []byte, pn uint64, pnLen int, payload []byte) ([]byte, error) {
	if pnLen < 1 || pnLen > 4 {
		return nil, ErrPacketEncoding
	}
	// Header protection samples 16 bytes starting 4 bytes past the packet-number
	// offset, so packet number + payload + 16-byte AEAD tag must be ≥ 20 bytes
	// (RFC 9001 §5.4.2). Pad the frames with PADDING (zero bytes) when too short.
	if need := (4 - pnLen) - len(payload); need > 0 {
		padded := make([]byte, 0, len(payload)+need)
		padded = append(padded, payload...)
		payload = append(padded, make([]byte, need)...)
	}
	const aeadTag = 16
	var hdr []byte
	var pnOffset int
	if typ == PacketShort {
		hdr, pnOffset = AppendShortHeader(nil, dcid, pnLen, false)
	} else {
		length := uint64(pnLen + len(payload) + aeadTag)
		hdr, pnOffset = AppendLongHeader(nil, typ, QUICVersion1, dcid, scid, token, pnLen, length)
	}
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	return s.Seal(dst, hdr, pnOffset, pnLen, pn, payload)
}
