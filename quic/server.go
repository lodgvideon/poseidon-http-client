package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"time"
)

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

// serverFlightSink captures a server handshake's outbound CRYPTO per packet-
// number space, plus the Handshake- and 1-RTT-level send sealers as crypto/tls
// installs write keys, so StartServerHandshake can seal the response flights.
type serverFlightSink struct {
	crypto          [numSpaces][]byte
	handshakeSealer *Sealer
	appSealer       *Sealer
	handshakeOpener *Opener
	appOpener       *Opener
	peerTP          []byte
	complete        bool
}

func (s *serverFlightSink) WriteCrypto(l tls.QUICEncryptionLevel, d []byte) error {
	s.crypto[levelSpace(l)] = append(s.crypto[levelSpace(l)], d...)
	return nil
}

func (s *serverFlightSink) SetReadKeys(l tls.QUICEncryptionLevel, suite uint16, secret []byte) error {
	k, err := KeysFromSecret(suite, secret)
	if err != nil {
		return err
	}
	opener, err := NewOpener(k)
	if err != nil {
		return err
	}
	switch levelSpace(l) {
	case spaceHandshake:
		s.handshakeOpener = opener
	case spaceApp:
		s.appOpener = opener
	}
	return nil
}

func (s *serverFlightSink) SetWriteKeys(l tls.QUICEncryptionLevel, suite uint16, secret []byte) error {
	k, err := KeysFromSecret(suite, secret)
	if err != nil {
		return err
	}
	sealer, err := NewSealer(k)
	if err != nil {
		return err
	}
	switch levelSpace(l) {
	case spaceHandshake:
		s.handshakeSealer = sealer
	case spaceApp:
		s.appSealer = sealer
	}
	return nil
}

func (s *serverFlightSink) PeerTransportParameters(params []byte) error {
	s.peerTP = append([]byte(nil), params...)
	return nil
}
func (s *serverFlightSink) HandshakeComplete() error { s.complete = true; return nil }

// ServerFlight is the server's response to a client Initial and the state to
// carry the connection forward: the sealed datagram(s) to send, the handshake,
// and the send/receive keys as they are installed.
type ServerFlight struct {
	// Datagrams are the sealed packets to send to the client, each its own
	// datagram: the Initial (ServerHello) and the Handshake flight
	// (EncryptedExtensions .. Finished).
	Datagrams [][]byte
	// Handshake is the in-progress server handshake, driven further via
	// HandleClientHandshake as the client's CRYPTO arrives.
	Handshake *TLSHandshake
	// HandshakeSealer / HandshakeOpener protect Handshake-level packets sent and
	// received. AppSealer / AppOpener protect 1-RTT packets and are non-nil once
	// application keys are installed (AppSealer after the server's Finished,
	// AppOpener once the handshake completes).
	HandshakeSealer *Sealer
	HandshakeOpener *Opener
	AppSealer       *Sealer
	AppOpener       *Opener
	// Complete reports whether the TLS handshake has finished.
	Complete bool
	// PeerTransportParams is the client's raw QUIC transport parameters, delivered
	// during the handshake (RFC 9000 §7.4); a server connection parses them for the
	// client's flow-control and stream limits.
	PeerTransportParams []byte
	// LocalTransportParams is the server's own serialized transport parameters
	// (the tp passed to StartServerHandshake); a server connection parses them for
	// the stream and idle limits it advertised.
	LocalTransportParams []byte
	// SCID is the server's source connection ID used on the response packets.
	SCID []byte

	sink *serverFlightSink // retained to keep pumping the handshake
}

// StartServerHandshake processes a client's first Initial (as returned by
// AcceptInitial), drives the TLS server through its first flight, and returns the
// sealed response datagrams plus the state to continue the connection. cfg
// carries the server certificate(s); tp is the server's serialized transport
// parameters; scid is the server's chosen source connection ID, which the client
// adopts as its destination CID. Reply packets are addressed to the client's
// source connection ID (ci.SCID).
//
// It produces only the server's first flight; the handshake completes later, once
// the client's Handshake-level Finished is fed to the returned Handshake.
func StartServerHandshake(ci *ClientInitial, cfg *tls.Config, tp, scid []byte) (*ServerFlight, error) {
	// The server's Initial keys derive from the client's original DCID.
	_, serverKeys := InitialKeys(ci.DCID)
	initialSealer, err := NewSealer(serverKeys)
	if err != nil {
		return nil, err
	}

	hs := NewServerHandshake(cfg, tp)
	sink := &serverFlightSink{}
	if err := hs.Start(context.Background()); err != nil {
		return nil, err
	}
	if err := hs.Pump(sink); err != nil {
		return nil, err
	}
	if err := hs.HandleCrypto(tls.QUICEncryptionLevelInitial, ci.CryptoData); err != nil {
		return nil, err
	}
	if err := hs.Pump(sink); err != nil {
		return nil, err
	}

	flight := &ServerFlight{
		Handshake:            hs,
		HandshakeSealer:      sink.handshakeSealer,
		HandshakeOpener:      sink.handshakeOpener,
		AppSealer:            sink.appSealer,
		AppOpener:            sink.appOpener,
		PeerTransportParams:  sink.peerTP,
		LocalTransportParams: append([]byte(nil), tp...),
		SCID:                 append([]byte(nil), scid...),
		sink:                 sink,
	}
	// Initial packet: the ServerHello, addressed to the client's source CID.
	if len(sink.crypto[spaceInitial]) > 0 {
		f := AppendCrypto(nil, 0, sink.crypto[spaceInitial])
		dg, err := SealPacket(nil, initialSealer, PacketInitial, ci.SCID, scid, nil, 0, 4, f)
		if err != nil {
			return nil, err
		}
		flight.Datagrams = append(flight.Datagrams, dg)
	}
	// Handshake packet: EncryptedExtensions .. Finished, once handshake keys exist.
	if sink.handshakeSealer != nil && len(sink.crypto[spaceHandshake]) > 0 {
		f := AppendCrypto(nil, 0, sink.crypto[spaceHandshake])
		dg, err := SealPacket(nil, sink.handshakeSealer, PacketHandshake, ci.SCID, scid, nil, 0, 4, f)
		if err != nil {
			return nil, err
		}
		flight.Datagrams = append(flight.Datagrams, dg)
	}
	return flight, nil
}

// HandleClientHandshake feeds the client's Handshake-level CRYPTO (its Finished)
// into the server handshake and pumps it. On success the TLS handshake is
// complete and 1-RTT keys are installed: Complete reports true, and AppSealer /
// AppOpener protect application (1-RTT) packets sent and received.
func (f *ServerFlight) HandleClientHandshake(cryptoData []byte) error {
	if err := f.Handshake.HandleCrypto(tls.QUICEncryptionLevelHandshake, cryptoData); err != nil {
		return err
	}
	if err := f.Handshake.Pump(f.sink); err != nil {
		return err
	}
	f.AppSealer = f.sink.appSealer
	f.AppOpener = f.sink.appOpener
	f.Complete = f.sink.complete
	return nil
}

// NewServerConn builds a connected server-role Conn from a completed server
// handshake (f.Complete must be true). pc is the per-connection datagram
// transport; clientDCID is the Destination Connection ID from the client's first
// Initial (RFC 9000 §7.3), and clientSCID is the client's Source Connection ID,
// which becomes this connection's Destination CID. The 1-RTT and Handshake keys,
// the peer transport parameters, and the connection IDs are seeded from f, so the
// connection is ready to send and receive 1-RTT packets.
//
// Key update (RFC 9001 §6) is not yet armed on a server connection, and the
// handshake is assumed already acknowledged; see docs/QUIC_SERVER_DESIGN.md.
func NewServerConn(pc PacketConn, f *ServerFlight, clientDCID, clientSCID []byte) (*Conn, error) {
	if f == nil || !f.Complete {
		return nil, errors.New("quic: server handshake not complete")
	}
	peer, err := ParseTransportParams(f.PeerTransportParams)
	if err != nil {
		return nil, err
	}
	c := &Conn{
		pc:                pc,
		hs:                f.Handshake,
		isServer:          true,
		dcid:              append([]byte(nil), clientSCID...),
		scid:              append([]byte(nil), f.SCID...),
		origDCID:          append([]byte(nil), clientDCID...),
		now:               time.Now,
		connRecvMax:       DefaultConnRecvWindow,
		cwnd:              kInitialWindow,
		ssthresh:          ^uint64(0),
		done:              make(chan struct{}),
		streamCredit:      make(chan struct{}, 1),
		handshakeComplete: true,
		handshakeSealer:   f.HandshakeSealer,
		oneRTTSealer:      f.AppSealer,
		peer:              peer,
		connMax:           peer.InitialMaxData,
	}
	c.keys.Handshake = f.HandshakeOpener
	c.keys.OneRTT = f.AppOpener
	if local, err := ParseTransportParams(f.LocalTransportParams); err == nil {
		c.localMaxStreamsUni = local.InitialMaxStreamsUni
		c.localMaxStreamsBidi = local.InitialMaxStreamsBidi
		c.localMaxIdle = local.MaxIdleTimeout
	}
	c.lastActivity = c.now()
	return c, nil
}
