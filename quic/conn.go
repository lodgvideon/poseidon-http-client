package quic

import (
	"bytes"
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

	dcid         []byte // server connection ID (our original random DCID until Retry)
	scid         []byte // our source connection ID (may be zero-length)
	retryToken   []byte // address-validation token from a Retry, echoed in later Initials (§8.1.2)
	handledRetry bool   // a Retry has been processed (at most one accepted, §17.2.5.2)

	// Server-issued destination connection IDs (RFC 9000 §5.1). serverCIDs holds
	// the active (not-yet-retired) ones by sequence number — sequence 0 is the
	// server SCID adopted at the handshake, higher ones arrive in
	// NEW_CONNECTION_ID. curCIDSeq is the sequence of the CID in use (c.dcid);
	// retirePriorTo is the highest Retire Prior To seen.
	serverCIDs    map[uint64][]byte
	retiredCIDs   map[uint64]bool // sequences already scheduled for RETIRE_CONNECTION_ID (dedup)
	curCIDSeq     uint64
	retirePriorTo uint64

	initialSealer   *Sealer
	handshakeSealer *Sealer
	oneRTTSealer    *Sealer    // current-generation 1-RTT send Sealer (ratcheted on key update)
	keys            KeySet     // receive Openers per level (OneRTT = current 1-RTT read gen)
	ku              *keyUpdate // RFC 9001 §6 key-update state (nil until app keys installed)

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
	ptoCount     uint                      // consecutive probe timeouts (backoff, RFC 9002 §6.2.1)
	probePending bool                      // a PTO needs a PING probe sent in the app space (§6.2.4)

	// NewReno congestion control (RFC 9002 §7), connection-wide across spaces.
	// cwnd == 0 disables it (the sentinel for hand-built test connections).
	cwnd          uint64    // congestion window in bytes
	bytesInFlight uint64    // ack-eliciting bytes sent but not acked/lost
	ssthresh      uint64    // slow-start threshold (^uint64(0) = infinite)
	ccBytesAcked  uint64    // bytes acked toward the next cwnd increase (avoidance)
	recoveryStart time.Time // start of the current recovery episode (§7.3.1)

	peer               TransportParams // parsed peer transport parameters (send limits)
	gotServerCID       bool            // the server's SCID has been adopted as our DCID
	closed             bool
	handshakeComplete  bool // TLS handshake finished (drives Establish + TLS pump)
	handshakeConfirmed bool // QUIC HANDSHAKE_DONE received (RFC 9001 §4.1.2; gates key update §6.1)
	sendBuf            []byte

	nextBidiStreamID   uint64             // next client-initiated bidi stream ID (0, 4, 8, …)
	openedBidi         uint64             // count of client bidi streams opened (RFC 9000 §4.6 gate)
	openedUni          uint64             // count of client uni streams opened (§4.6 gate; ID = 2+4n)
	streams            map[uint64]*Stream // open streams by ID
	localMaxStreamsUni uint64             // uni streams the peer may open toward us (we advertised, §4.6)
	acceptedUni        []*Stream          // accepted server-initiated uni streams awaiting AcceptUniStream
	pollBuf            []byte             // reused datagram buffer for Poll

	connSent         uint64 // cumulative bytes sent in STREAM frames across all streams (§4.1)
	connMax          uint64 // absolute connection-level send ceiling; init = peer.InitialMaxData
	dataBlockedLimit uint64 // last connMax a DATA_BLOCKED was emitted for (emit once per limit)
	dataBlockedSet   bool   // whether a DATA_BLOCKED has been emitted yet

	connRecvConsumed uint64 // total bytes the app has read across all streams (receive FC)
	connRecvTotal    uint64 // sum of the highest received offset over all streams (receive FC, §4.1)
	connRecvMax      uint64 // connection-level receive limit we advertise; raised via MAX_DATA
	pendingCtrl      []byte // app-space control frames to send (MAX_DATA/MAX_STREAM_DATA)

	// AEAD usage limits (RFC 9001 §6.6). This client supports only AES-GCM and is
	// a pure key-update responder, so on reaching a limit it must close with
	// AEAD_LIMIT_REACHED rather than rotate keys itself.
	appSendCount uint64 // 1-RTT packets sealed under the current write key (confidentiality)
	authFailures uint64 // 1-RTT packets that failed authentication, across all keys (integrity)
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
		connRecvMax:   DefaultConnRecvWindow,
		cwnd:          kInitialWindow, // arm NewReno (RFC 9002 §7.2)
		ssthresh:      ^uint64(0),     // "infinite" until the first loss
	}
	c.keys.Initial = io
	// Retain the unidirectional-stream limit we advertise, so inbound
	// server-initiated uni streams can be gated against it (RFC 9000 §4.6).
	if tp, err := ParseTransportParams(transportParams); err == nil {
		c.localMaxStreamsUni = tp.InitialMaxStreamsUni
	}
	return c, nil
}

// acceptPeerUniStream accepts a server-initiated unidirectional stream (RFC 9000
// §2.1, id&0x3==3), gating on the unidirectional-stream limit we advertised
// (§4.6). The stream is receive-only; it is registered so reassembly and
// RESET_STREAM/STOP_SENDING/MAX_STREAM_DATA handling apply, and queued for
// AcceptUniStream.
func (c *Conn) acceptPeerUniStream(id uint64) (*Stream, error) {
	if id>>2 >= c.localMaxStreamsUni {
		return nil, ErrTooManyUniStreams
	}
	s := &Stream{id: id, conn: c, recvMax: DefaultStreamRecvWindow}
	if c.streams == nil {
		c.streams = map[uint64]*Stream{}
	}
	c.streams[id] = s
	c.acceptedUni = append(c.acceptedUni, s)
	return s, nil
}

// AcceptUniStream returns the next accepted server-initiated unidirectional
// stream, or nil if none is pending. It does not block; the caller drives the
// connection with Poll and drains newly accepted streams between polls.
func (c *Conn) AcceptUniStream() *Stream {
	if len(c.acceptedUni) == 0 {
		return nil
	}
	s := c.acceptedUni[0]
	c.acceptedUni = c.acceptedUni[1:]
	return s
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
	// Credit the congestion controller for each newly acknowledged ack-eliciting
	// packet before ack() removes it (RFC 9002 §7.3). Iterate our own sent packets
	// rather than the ACK range, so a malformed huge range cannot make this costly.
	for pn, p := range c.sent[sp].packets {
		if pn >= low && pn <= high && p.ackEliciting {
			c.onPacketAcked(p)
		}
	}
	if sendTime, ok := c.sent[sp].ack(low, high); ok {
		c.rtt.update(c.clock().Sub(sendTime), ackDelay)
		c.ptoCount = 0 // §6.2.1: a newly-acked ack-eliciting packet resets the backoff
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
		// Retain the 1-RTT read secret + HP key and pre-derive the next
		// generation so a peer key update (RFC 9001 §6) can be decrypted.
		return c.initAppReadKU(suite, secret, op.hp)
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
		// Installing Handshake write keys means the client is about to send
		// Handshake packets, so it discards Initial keys (RFC 9001 §4.9.1).
		c.discardSpace(spaceInitial)
	case spaceApp:
		c.oneRTTSealer = s
		// Retain the 1-RTT write secret + HP key and pre-derive the next
		// generation so the client can flip its own send phase (RFC 9001 §6.2).
		return c.initAppWriteKU(suite, secret, s.hp)
	}
	return nil
}

// discardSpace drops all state for a packet-number space when its keys are
// discarded (RFC 9001 §4.9): it un-counts the space's in-flight bytes from the
// congestion controller (RFC 9002 §6.4) and clears its sent, ACK, retransmit,
// inbound-CRYPTO, and pending state and its keys, so nothing further is sent or
// processed there. Idempotent — discarding an already-empty space is a no-op.
func (c *Conn) discardSpace(sp int) {
	for _, p := range c.sent[sp].packets {
		if p.ackEliciting {
			c.removeInFlight(p.size)
		}
	}
	c.sent[sp] = sentSpace{}
	c.acks[sp] = ackTracker{}
	c.cryptoRecv[sp] = recvStream{}
	c.pendingCrypto[sp] = nil
	c.retransQueue[sp] = nil
	// Discarding a packet-number space is forward progress, so the PTO backoff is
	// reset (RFC 9002 §6.2.2, App. A.4); otherwise a Handshake-space backoff would
	// carry into the Application space and inflate its first probe timeout.
	c.ptoCount = 0
	switch sp {
	case spaceInitial:
		c.keys.Initial = nil
		c.initialSealer = nil
	case spaceHandshake:
		c.keys.Handshake = nil
		c.handshakeSealer = nil
	}
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
	// Authenticate the server's connection ID (RFC 9000 §7.3): its
	// initial_source_connection_id MUST be present and equal the Source Connection
	// ID of the server's first Initial packet, which the client adopted as its
	// destination CID. An absent or mismatched value signals a spoofed handshake.
	if !tp.HaveInitialSourceConnectionID || !bytes.Equal(tp.InitialSourceConnectionID, c.dcid) {
		return ErrTransportParameter
	}
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
	c.onPacketSent(spaceInitial, pn, true, len(pkt)) // count toward bytes_in_flight (RFC 9002 §7)
	c.cryptoOffset[spaceInitial] += uint64(len(ch))
	c.pendingCrypto[spaceInitial] = c.pendingCrypto[spaceInitial][:0]
	c.sendPN[spaceInitial]++
	_, err = c.pc.Write(pkt)
	c.sendBuf = pkt[:0] // reuse the datagram backing next flight
	return err
}
