package quic

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"time"
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

	acks        [numSpaces]ackTracker
	cryptoRecv  [numSpaces]recvStream // inbound CRYPTO reassembled by offset per space
	largestRecv [numSpaces]uint64     // largest received packet number per space
	haveRecv    [numSpaces]bool

	sent         [numSpaces]sentSpace      // packets we sent, per space (ACK/RTT/loss)
	rtt          rttStats                  // round-trip-time estimates (RFC 9002 §5)
	now          func() time.Time          // clock (time.Now; overridable in tests)
	retransQueue [numSpaces][]retransFrame // frames of lost packets awaiting resend

	peer              TransportParams // parsed peer transport parameters (send limits)
	gotServerCID      bool            // the server's SCID has been adopted as our DCID
	closed            bool
	handshakeComplete bool
	sendBuf           []byte

	nextBidiStreamID uint64             // next client-initiated bidi stream ID (0, 4, 8, …)
	openedBidi       uint64             // count of client bidi streams opened (RFC 9000 §4.6 gate)
	openedUni        uint64             // count of client uni streams opened (§4.6 gate; ID = 2+4n)
	streams          map[uint64]*Stream // open streams by ID
	pollBuf          []byte             // reused datagram buffer for Poll

	connSent         uint64 // cumulative bytes sent in STREAM frames across all streams (§4.1)
	connMax          uint64 // absolute connection-level send ceiling; init = peer.InitialMaxData
	dataBlockedLimit uint64 // last connMax a DATA_BLOCKED was emitted for (emit once per limit)
	dataBlockedSet   bool   // whether a DATA_BLOCKED has been emitted yet
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
		now:           time.Now,
	}
	c.keys.Initial = io
	return c, nil
}

// clock returns the current time, defaulting to time.Now when no clock was
// injected (tests may set c.now to a fake).
func (c *Conn) clock() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return c.now()
}

// onAckRange acknowledges the packet-number range [low, high] in space sp,
// updating the RTT estimate from the largest newly-acknowledged packet
// (RFC 9002 §5). ackDelay is the peer's decoded ACK delay (zero for ranges that
// cannot carry the largest acked).
func (c *Conn) onAckRange(sp int, low, high uint64, ackDelay time.Duration) {
	if sendTime, ok := c.sent[sp].ack(low, high); ok {
		c.rtt.update(c.clock().Sub(sendTime), ackDelay)
	}
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

// PeerTransportParameters parses the peer's transport parameters and seeds the
// connection-level send limit (HandshakeSink). A malformed or invalid parameter
// set aborts the handshake as a TRANSPORT_PARAMETER_ERROR (RFC 9000 §7.4).
func (c *Conn) PeerTransportParameters(params []byte) error {
	tp, err := ParseTransportParams(params)
	if err != nil {
		return err
	}
	c.peer = tp
	c.connMax = tp.InitialMaxData
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
	pn := c.sendPN[spaceInitial]
	pkt, err := BuildInitialPacket(c.sendBuf[:0], c.initialSealer, c.dcid, c.scid, nil,
		pn, 4, c.cryptoOffset[spaceInitial], ch, InitialDatagramMinSize)
	if err != nil {
		return err
	}
	// Record the Initial as ack-eliciting and retransmittable: its CRYPTO bytes
	// (a private copy) at their offset, so a lost ClientHello can be resent.
	c.sent[spaceInitial].onSent(pn, c.clock(), true, []retransFrame{{
		kind: retransCrypto, offset: c.cryptoOffset[spaceInitial], data: append([]byte(nil), ch...),
	}})
	c.cryptoOffset[spaceInitial] += uint64(len(ch))
	c.pendingCrypto[spaceInitial] = c.pendingCrypto[spaceInitial][:0]
	c.sendPN[spaceInitial]++
	_, err = c.pc.Write(pkt)
	c.sendBuf = pkt[:0] // reuse the datagram backing next flight
	return err
}
