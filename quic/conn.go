package quic

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
)

// Packet-number spaces (RFC 9000 §12.3): Initial, Handshake, and Application
// each have independent packet numbers, keys, ACK state, and loss timers.
const (
	spaceInitial = iota
	spaceHandshake
	spaceApp
	numSpaces
)

// ErrNoClientHello is returned when the TLS handshake did not produce a
// ClientHello to send (an internal invariant failure).
var ErrNoClientHello = errors.New("quic: handshake produced no ClientHello")

// PacketConn is the datagram transport a Conn sends and receives on — typically
// a connected *net.UDPConn. Read and Write operate on whole QUIC datagrams.
type PacketConn interface {
	Write(p []byte) (int, error)
	Read(p []byte) (int, error)
	Close() error
}

// Conn is a QUIC v1 client connection (RFC 9000). It drives the TLS handshake
// (RFC 9001) over its packet-number spaces and owns the per-space send state.
// The connection engine is built up across phases; this scaffolding wires the
// handshake to the send path and manages establishment. Not safe for concurrent
// use by multiple goroutines.
type Conn struct {
	pc PacketConn
	hs *TLSHandshake

	dcid []byte // server connection ID (our original random DCID until Retry)
	scid []byte // our source connection ID (may be zero-length)

	initialSealer   *Sealer
	handshakeSealer *Sealer
	oneRTTSealer    *Sealer
	keys            KeySet // receive Openers per level

	sendPN        [numSpaces]uint64 // next packet number to send per space
	pendingCrypto [numSpaces][]byte // handshake bytes to send per space
	cryptoOffset  [numSpaces]uint64 // CRYPTO stream offset sent per space

	peerParams        []byte
	handshakeComplete bool
	sendBuf           []byte
}

// NewConn creates a client QUIC connection over pc. tlsConfig must set
// ServerName; transportParams is the serialized QUIC transport parameters
// (RFC 9000 §7.4). It chooses a random Destination Connection ID, derives the
// Initial keys from it (RFC 9001 §5.2), and prepares the TLS handshake — but
// does not send anything until Establish.
func NewConn(pc PacketConn, tlsConfig *tls.Config, transportParams []byte) (*Conn, error) {
	dcid := make([]byte, 8)
	if _, err := rand.Read(dcid); err != nil {
		return nil, err
	}
	client, server := InitialKeys(dcid)
	is, err := NewSealer(client)
	if err != nil {
		return nil, err
	}
	io, err := NewOpener(server)
	if err != nil {
		return nil, err
	}
	c := &Conn{
		pc:            pc,
		hs:            NewClientHandshake(tlsConfig, transportParams),
		dcid:          dcid,
		initialSealer: is,
	}
	c.keys.Initial = io
	return c, nil
}

// levelSpace maps a TLS encryption level to a packet-number space.
func levelSpace(l tls.QUICEncryptionLevel) int {
	switch l {
	case tls.QUICEncryptionLevelHandshake:
		return spaceHandshake
	case tls.QUICEncryptionLevelApplication, tls.QUICEncryptionLevelEarly:
		return spaceApp
	default:
		return spaceInitial
	}
}

// --- HandshakeSink: the Conn reacts to TLS handshake progress. ---

// WriteCrypto buffers handshake bytes to send in CRYPTO frames at level's
// packet-number space (HandshakeSink).
func (c *Conn) WriteCrypto(level tls.QUICEncryptionLevel, data []byte) error {
	sp := levelSpace(level)
	c.pendingCrypto[sp] = append(c.pendingCrypto[sp], data...)
	return nil
}

// SetReadKeys installs the receive Opener for level (HandshakeSink).
func (c *Conn) SetReadKeys(level tls.QUICEncryptionLevel, suite uint16, secret []byte) error {
	keys, err := KeysFromSecret(suite, secret)
	if err != nil {
		return err
	}
	op, err := NewOpener(keys)
	if err != nil {
		return err
	}
	switch levelSpace(level) {
	case spaceHandshake:
		c.keys.Handshake = op
	case spaceApp:
		c.keys.OneRTT = op
	}
	return nil
}

// SetWriteKeys installs the send Sealer for level (HandshakeSink).
func (c *Conn) SetWriteKeys(level tls.QUICEncryptionLevel, suite uint16, secret []byte) error {
	keys, err := KeysFromSecret(suite, secret)
	if err != nil {
		return err
	}
	s, err := NewSealer(keys)
	if err != nil {
		return err
	}
	switch levelSpace(level) {
	case spaceHandshake:
		c.handshakeSealer = s
	case spaceApp:
		c.oneRTTSealer = s
	}
	return nil
}

// PeerTransportParameters records the peer's raw transport parameters
// (HandshakeSink).
func (c *Conn) PeerTransportParameters(params []byte) error {
	c.peerParams = append(c.peerParams[:0], params...)
	return nil
}

// HandshakeComplete marks the TLS handshake finished (HandshakeSink).
func (c *Conn) HandshakeComplete() error {
	c.handshakeComplete = true
	return nil
}

// sendInitialFlight starts the handshake, pumps the resulting ClientHello, and
// sends it in a padded (>=1200-byte) Initial datagram (RFC 9000 §14.1).
func (c *Conn) sendInitialFlight(ctx context.Context) error {
	if err := c.hs.Start(ctx); err != nil {
		return err
	}
	if err := c.hs.Pump(c); err != nil {
		return err
	}
	ch := c.pendingCrypto[spaceInitial]
	if len(ch) == 0 {
		return ErrNoClientHello
	}
	pkt, err := BuildInitialPacket(c.sendBuf[:0], c.initialSealer, c.dcid, c.scid, nil,
		c.sendPN[spaceInitial], 4, c.cryptoOffset[spaceInitial], ch, InitialDatagramMinSize)
	if err != nil {
		return err
	}
	c.cryptoOffset[spaceInitial] += uint64(len(ch))
	c.pendingCrypto[spaceInitial] = c.pendingCrypto[spaceInitial][:0]
	c.sendPN[spaceInitial]++
	_, err = c.pc.Write(pkt)
	c.sendBuf = pkt[:0] // reuse the datagram backing next flight
	return err
}
