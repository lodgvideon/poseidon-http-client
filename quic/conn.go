package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"sync"
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
	// mu guards ALL Conn mutable state and the wire (every pc.Write). The
	// discipline mirrors the HTTP/2 layer: public entry points take mu and
	// delegate to an assume-held internal (…Locked); receive-path helpers and
	// frame handlers run only inside a locked section and never re-lock. A
	// single mutex (not RWMutex) because the receive path mutates send-path
	// state, so the state does not partition (see docs/HTTP3_DESIGN.md §3.2).
	mu sync.Mutex

	pc PacketConn
	hs *TLSHandshake

	// isServer marks a connection built for the server role (NewServerConn). It
	// gates the few role-specific behaviors (stream-ID assignment, stream
	// acceptance, connection-ID handling) that differ from the client default.
	isServer bool

	dcid         []byte // server connection ID (our original random DCID until Retry)
	scid         []byte // our source connection ID (may be zero-length)
	retryToken   []byte // address-validation token from a Retry, echoed in later Initials (§8.1.2)
	handledRetry bool   // a Retry has been processed (at most one accepted, §17.2.5.2)

	// Server-issued destination connection IDs (RFC 9000 §5.1). serverCIDs holds
	// the active (not-yet-retired) ones by sequence number — sequence 0 is the
	// server SCID adopted at the handshake, higher ones arrive in
	// NEW_CONNECTION_ID. curCIDSeq is the sequence of the CID in use (c.dcid);
	// retirePriorTo is the highest Retire Prior To seen.
	serverCIDs     map[uint64][]byte
	curCIDSeq      uint64
	retirePriorTo  uint64
	pendingRetires int // RETIRE_CONNECTION_ID frames queued but not yet sent (flood bound, §5.1.2)

	initialSealer   *Sealer
	handshakeSealer *Sealer
	oneRTTSealer    *Sealer    // current-generation 1-RTT send Sealer (ratcheted on key update)
	keys            keySet     // receive Openers per level (OneRTT = current 1-RTT read gen)
	ku              *keyUpdate // RFC 9001 §6 key-update state (nil until app keys installed)

	sendPN        [numSpaces]uint64 // next packet number to send per space
	pendingCrypto [numSpaces][]byte // handshake bytes to send per space
	cryptoOffset  [numSpaces]uint64 // CRYPTO stream offset sent per space

	acks        [numSpaces]ackTracker
	cryptoRecv  [numSpaces]recvStream // inbound CRYPTO reassembled by offset per space
	largestRecv [numSpaces]uint64     // largest received packet number per space
	haveRecv    [numSpaces]bool

	// fh is the frame handler reused for every received packet. It used to be
	// built fresh in recvDatagram, but ParseFrames takes a FrameHandler
	// interface, so the composite literal escaped — 6 allocs and 168 B on every
	// datagram received. Reset per packet by resetFrameHandler; safe to share
	// because every field is per-packet state and recvDatagram runs under c.mu
	// on the single reader goroutine.
	fh connFrameHandler

	sent         [numSpaces]sentSpace      // packets we sent, per space (ACK/RTT/loss)
	rtt          rttStats                  // round-trip-time estimates (RFC 9002 §5)
	now          func() time.Time          // clock (time.Now; overridable in tests)
	retransQueue [numSpaces][]retransFrame // frames of lost packets awaiting resend
	// retransFree recycles the payload copies retransFrames retain (RFC 9000
	// §13.3). Both ends run under c.mu — writeStreamFrame takes the copy,
	// sentSpace.ack releases it — so it needs no synchronisation of its own.
	// See retransbuf.go for why ack is the only place a payload may be released.
	retransFree    [][]byte
	ptoCount       uint // consecutive probe timeouts (backoff, RFC 9002 §6.2.1)
	probePending   bool // a PTO needs a PING probe sent in the app space (§6.2.4)
	handshakeProbe bool // a PTO needs a PING probe in the Handshake/Initial space (§6.2.2.1)
	ptoExempt      bool // a queued PTO probe may exceed the congestion window once (§7, §6.2.4)

	// NewReno congestion control (RFC 9002 §7), connection-wide across spaces.
	// cwnd == 0 disables it (the sentinel for hand-built test connections).
	cwnd           uint64    // congestion window in bytes
	bytesInFlight  uint64    // ack-eliciting bytes sent but not acked/lost
	ssthresh       uint64    // slow-start threshold (^uint64(0) = infinite)
	ccBytesAcked   uint64    // bytes acked toward the next cwnd increase (avoidance)
	recoveryStart  time.Time // start of the current recovery episode (§7.3.1)
	firstRTTSample time.Time // when the first RTT sample arrived; gates persistent congestion (§7.6)
	pacingBudget   uint64    // token-bucket pacing allowance in bytes (RFC 9002 §7.7)
	pacingLast     time.Time // last pacing-bucket refill instant

	// Opt-in BBR v1 congestion control (bbr.go), selected with
	// WithCongestionControl(CCBBR). When ccAlgo == CCNewReno (the zero value and
	// default) NOTHING below is touched and every arithmetic path in cc.go is
	// byte-identical to the NewReno-only build. When BBR is active it drives the
	// SAME send gate by writing c.cwnd and c.pacingRate; NewReno's slow-start /
	// congestion-avoidance arithmetic is bypassed.
	ccAlgo     CongestionControlAlgorithm // CCNewReno (default) or CCBBR
	bbr        *bbr                       // BBR model state; nil unless ccAlgo == CCBBR
	pacingRate uint64                     // BBR pacing rate in bytes/sec (0 ⇒ NewReno's 5·cwnd/4)

	// Delivery-rate sampler (draft-cheng-iccrg-delivery-rate-estimation), advanced
	// by the ack path only when BBR is active.
	delivered         uint64    // cumulative bytes acknowledged over the connection
	deliveredTime     time.Time // instant of the most recent delivery (ack)
	appLimitedUntil   uint64    // delivered-count bound below which samples are app-limited (0 = not app-limited)
	rsPriorDelivered  uint64    // per-ACK-range rate-sample representative: max acked P.delivered
	rsPriorTime       time.Time // its delivered-time snapshot
	rsPriorAppLimited bool      // whether that representative was app-limited
	rsPriorValid      bool      // a representative was seen this range

	peer               TransportParams // parsed peer transport parameters (send limits)
	gotServerCID       bool            // the server's SCID has been adopted as our DCID
	serverSCID         []byte          // the authenticated server Source Connection ID (§7.2 discard check)
	origDCID           []byte          // the DCID of the client's first Initial, for §7.3 validation
	retrySCID          []byte          // the Source Connection ID of a received Retry, for §7.3 validation
	closed             bool
	peerClose          *PeerClosedError    // set when a CONNECTION_CLOSE is received (draining, §10.2.2)
	resetTokens        map[uint64][16]byte // stateless reset token per active peer CID sequence (§10.3)
	handshakeComplete  bool                // TLS handshake finished (drives Establish + TLS pump)
	handshakeConfirmed bool                // QUIC HANDSHAKE_DONE received (RFC 9001 §4.1.2; gates key update §6.1)

	// Per-Conn send scratch, reused across seals to keep the send path alloc-lean
	// instead of allocating a fresh header/frame/seal-output buffer per packet.
	// Safe ONLY because c.mu is held across the whole seal+pc.Write, so no two
	// goroutines ever touch the scratch at once, AND pc.Write copies the datagram
	// into the kernel synchronously (net.UDPConn.Write) and never retains the
	// slice after returning — so the next seal is free to overwrite it. The sealed
	// bytes are NOT retained for retransmit: flushRetransmits re-seals from the
	// retained retransFrame descriptors (whose data is a private copy), never from
	// this scratch, so reuse cannot corrupt a resend.
	hdrScratch   []byte // long/short header + packet number (sealPacket)
	frameScratch []byte // STREAM frame bytes (writeStreamFrame)
	sealScratch  []byte // AEAD seal output = the on-wire datagram (sealPacket)
	// ackScratch holds the frame payload flush builds for a packet that carries no
	// STREAM data — an owed ACK, credit grants, a PTO probe, CRYPTO. It started from
	// nil on every call, so that path allocated once per ACK at a single contiguous
	// range and up to four times as ranges accumulate under loss (#689).
	//
	// Kept separate from frameScratch rather than shared with it: both are only ever
	// touched under c.mu, but they are filled by different paths (the reader's flush
	// versus writeStreamFrame's send loop) and one buffer for two would make their
	// lifetimes a thing to reason about instead of a thing that cannot arise.
	ackScratch []byte

	// gsoBatch is the reused backing array for a GSO send batch (quic/gso.go): a
	// multi-datagram STREAM burst or same-size retransmit run is sealed one packet
	// at a time (each copied out of sealScratch) into this contiguous buffer, then
	// handed to the transport as one WriteGSO. Owned by whatever packetBatch is live
	// under c.mu; only one is ever live at a time (the send path serializes on c.mu),
	// so borrowing and returning it here needs no further guarding.
	gsoBatch []byte

	nextBidiStreamID    uint64             // next client-initiated bidi stream ID (0, 4, 8, …)
	openedBidi          uint64             // count of client bidi streams opened (RFC 9000 §4.6 gate)
	openedUni           uint64             // count of client uni streams opened (§4.6 gate; ID = 2+4n)
	streams             map[uint64]*Stream // open streams by ID
	localMaxStreamsUni  uint64             // uni streams the peer may open toward us (we advertised, §4.6)
	localMaxStreamsBidi uint64             // bidi streams the peer may open toward us (server role; we advertised, §4.6)
	localMaxIdle        time.Duration      // max_idle_timeout we advertised (§10.1); 0 = none
	handshakeTimeout    time.Duration      // whole-handshake bound Establish applies (WithHandshakeTimeout); 0 = defaultHandshakeTimeout
	lastActivity        time.Time          // last received packet, for the idle timer; the timer starts at NewConn and resets on receipt, so §10.1's "restart on send if not already running" is a no-op (§10.1)
	acceptedUni         []*Stream          // accepted server-initiated uni streams awaiting AcceptUniStream
	acceptedBidi        []*Stream          // accepted client-initiated bidi streams awaiting AcceptBidiStream (server role)
	pollBuf             []byte             // reused datagram buffer for Poll

	connSent         uint64 // cumulative bytes sent in STREAM frames across all streams (§4.1)
	connMax          uint64 // absolute connection-level send ceiling; init = peer.InitialMaxData
	dataBlockedLimit uint64 // last connMax a DATA_BLOCKED was emitted for (emit once per limit)
	dataBlockedSet   bool   // whether a DATA_BLOCKED has been emitted yet

	// spin is this connection's fixed latency-spin bit value (RFC 9000 §17.4).
	// Latency spin is not implemented, so the bit carries no meaning; it is drawn
	// once at random per connection rather than left constant.
	spin bool

	// STREAMS_BLOCKED latches, indexed [bidi, uni] (RFC 9000 §19.14). Same
	// emit-once-per-distinct-limit rule as the *_BLOCKED frames above, so a caller
	// retrying an open in a loop does not flood the peer.
	streamsBlockedLimit [2]uint64
	streamsBlockedSet   [2]bool

	connRecvConsumed uint64 // total bytes the app has read across all streams (receive FC)
	connRecvTotal    uint64 // sum of the highest received offset over all streams (receive FC, §4.1)
	connRecvMax      uint64 // connection-level receive limit we advertise; raised via MAX_DATA
	// grantedStreams / grantedConn name the receive limits a MAX_STREAM_DATA or
	// MAX_DATA has already been sent for, so a loss episode can re-send the current
	// value (RFC 9000 §13.3, regrantAfterLoss). Streams that never crossed a
	// half-window of consumption never enter the set.
	grantedStreams map[uint64]struct{}
	grantedConn    bool

	pendingCtrl     []byte // app-space control frames to send (MAX_DATA/MAX_STREAM_DATA)
	pathRespPending bool   // pendingCtrl holds a PATH_RESPONSE; its datagram must reach 1200 (§8.2.2)

	// armedReadDeadline is the read deadline the reader published before parking in
	// the blocking pc.Read (docs/HTTP3_DESIGN.md §3.2). A Do-side send epilogue may
	// shorten it (rearmReadDeadline, §4 INV-4) so the parked reader wakes to run
	// PTO/loss detection instead of sleeping to the idle scale. Zero means no reader
	// has parked yet (hand-built test conns, or before the first Poll).
	armedReadDeadline time.Time
	// armedLossDeadline is the loss/idle/ctx deadline the reader computed before
	// folding in the deferred-ACK deadline (below). armedReadDeadline is the min of
	// the two. handleExpiry compares the wake time against this to tell a genuine
	// loss/PTO/idle expiry from a wake caused only by the ACK deadline firing early,
	// so a deferred ACK never provokes a spurious PTO probe (RFC 9000 §13.2.1).
	armedLossDeadline time.Time
	// armedForAck records, at arm time, that armedReadDeadline was set to the deferred
	// ACK deadline because it was nearer than the loss/idle deadline. Captured then —
	// not recomputed from the live ackDeadline at wake — so a concurrent Do that clears
	// ackDeadline (by piggybacking the ACK) cannot flip a wake back into a spurious PTO.
	armedForAck bool
	// ackDeadline is when a deferred Application-space ACK must be sent by — the
	// receipt time of the first not-yet-acknowledged in-order ack-eliciting packet
	// plus our advertised max_ack_delay (RFC 9000 §13.2.1). Zero means no ACK is
	// deferred (none owed, or an immediate-ACK trigger fired, or the transport cannot
	// schedule the fallback timer). It shortens the reader's read deadline so the
	// owed ACK fires within max_ack_delay when no outbound packet carries it first.
	ackDeadline time.Time
	// readWatchdogStarted guards the one connection-lifetime goroutine that pokes the
	// read deadline into the past on connCtx cancel, so a blocked pc.Read unblocks on
	// Close (§3.1). Guarded by c.mu; started lazily on the first Poll with a live ctx.
	readWatchdogStarted bool

	// AEAD usage limits (RFC 9001 §6.6). This client supports only AES-GCM and is
	// a pure key-update responder, so on reaching a limit it must close with
	// AEAD_LIMIT_REACHED rather than rotate keys itself.
	appSendCount uint64 // 1-RTT packets sealed under the current write key (confidentiality)
	authFailures uint64 // 1-RTT packets that failed authentication, across all keys (integrity)

	// Stream wake vocabulary (docs/HTTP3_DESIGN.md §3.3), all guarded by c.mu.
	// Added in PR 2b as additive, INERT plumbing: the receive path produces these
	// signals but nothing consumes them yet (http3.Client.Do still inline-polls).
	streamCredit   chan struct{} // cap 1; signaled from OnMaxStreams when the peer raises the cumulative stream limit
	sendWindowGrew bool          // a receive burst freed congestion-window/connection credit; swept at end of burst
	done           chan struct{} // the single-close latch channel; closed once by terminateLocked, never reopened
	closeErr       error         // the first terminating error, published before done is closed
	terminated     bool          // guards terminateLocked against a double close(done) / closeErr clobber
}

// ConnOption configures a Conn at construction time. Options are additive and
// applied in order after the connection's defaults are set; passing none leaves
// every default in place (in particular, NewReno congestion control).
type ConnOption func(*Conn)

// NewConn creates a client QUIC connection over pc. tlsConfig must set
// ServerName; transportParams is the serialized QUIC transport parameters
// (RFC 9000 §7.4). It chooses a random Destination Connection ID, derives the
// Initial keys from it (RFC 9001 §5.2), and prepares the TLS handshake — but
// does not send anything until Establish.
//
// opts are optional functional configuration (e.g. WithCongestionControl); with
// none supplied the connection uses NewReno, byte-for-byte as before options
// existed.
func NewConn(pc PacketConn, tlsConfig *tls.Config, transportParams []byte, opts ...ConnOption) (*Conn, error) {
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
		hs:            newClientHandshake(tlsConfig, transportParams),
		dcid:          dcid,
		origDCID:      append([]byte(nil), dcid...), // the first Initial's DCID, for §7.3
		initialSealer: is,
		now:           time.Now,
		connRecvMax:   DefaultConnRecvWindow,
		cwnd:          kInitialWindow,         // arm NewReno (RFC 9002 §7.2)
		ssthresh:      ^uint64(0),             // "infinite" until the first loss
		done:          make(chan struct{}),    // single-close latch (§3.3)
		streamCredit:  make(chan struct{}, 1), // cap-1 stream-limit wake (§3.3)
	}
	c.keys.Initial = io
	// This client does not participate in latency spin, and RFC 9000 §17.4 then
	// RECOMMENDS setting the spin bit "to a random value either chosen independently
	// for each packet or chosen independently for each connection ID". Per-connection-
	// ID is the cheaper of the two and keeps the header writer allocation-free; a
	// constant 0 would be a passive fingerprint distinguishing this client.
	c.redrawSpin()
	// Retain the unidirectional-stream limit we advertise, so inbound
	// server-initiated uni streams can be gated against it (RFC 9000 §4.6).
	if tp, err := ParseTransportParams(transportParams); err == nil {
		c.localMaxStreamsUni = tp.InitialMaxStreamsUni
		c.localMaxIdle = tp.MaxIdleTimeout // the idle timeout we advertised (§10.1)
	}
	c.lastActivity = c.now() // the idle timer starts at connection creation
	for _, opt := range opts {
		opt(c)
	}
	// If an option selected BBR, arm its model now that the clock is in place.
	// NewReno (the default) leaves c.bbr nil and every CC field untouched.
	if c.ccAlgo == CCBBR && c.bbr == nil {
		c.initBBR()
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
	s := &Stream{id: id, conn: c, recvMax: DefaultStreamRecvWindow, ready: make(chan struct{}, 1)}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acceptUniStreamLocked()
}

// acceptUniStreamLocked is AcceptUniStream's body. Assumes c.mu is held.
func (c *Conn) acceptUniStreamLocked() *Stream {
	if len(c.acceptedUni) == 0 {
		return nil
	}
	s := c.acceptedUni[0]
	c.acceptedUni = c.acceptedUni[1:]
	return s
}

// acceptPeerBidiStream accepts a client-initiated bidirectional stream (RFC 9000
// §2.1, id&0x3==0) on a server connection — a request stream. It gates on the
// bidirectional-stream limit the server advertised (§4.6). The stream is
// registered (so reassembly and flow control apply) and queued for
// AcceptBidiStream; its send window is the client's per-stream limit for its own
// bidi streams (initial_max_stream_data_bidi_local), and its receive window is
// the one we advertise.
func (c *Conn) acceptPeerBidiStream(id uint64) (*Stream, error) {
	if id>>2 >= c.localMaxStreamsBidi {
		return nil, ErrTooManyBidiStreams
	}
	s := &Stream{
		id:      id,
		conn:    c,
		sendMax: c.peer.InitialMaxStreamDataBidiLocal,
		recvMax: DefaultStreamRecvWindow,
		ready:   make(chan struct{}, 1),
	}
	if c.streams == nil {
		c.streams = map[uint64]*Stream{}
	}
	c.streams[id] = s
	c.acceptedBidi = append(c.acceptedBidi, s)
	return s, nil
}

// AcceptBidiStream returns the next accepted client-initiated bidirectional
// stream (a request), or nil if none is pending. Server connections only. It
// does not block; the caller drives the connection with Poll and drains newly
// accepted streams between polls.
func (c *Conn) AcceptBidiStream() *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.acceptedBidi) == 0 {
		return nil
	}
	s := c.acceptedBidi[0]
	c.acceptedBidi = c.acceptedBidi[1:]
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

// signalReady pokes a stream's readiness channel without ever blocking
// (docs/HTTP3_DESIGN.md §3.3). The cap-1 channel coalesces: a token already
// buffered is left in place and a slow (or absent) consumer never stalls the
// receive path — the HTTP/2 push rule. Callers run inside the locked receive
// path; s.ready is nil only for hand-built test streams, where the send case on
// a nil channel is never selected, so the default branch keeps it a no-op.
func (c *Conn) signalReady(s *Stream) {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// signalStreamCredit wakes a caller blocked on the cumulative stream limit when
// the peer raises MAX_STREAMS (docs/HTTP3_DESIGN.md §3.3; consumed in PR 2d).
// Non-blocking, cap-1 coalescing, like signalReady.
func (c *Conn) signalStreamCredit() {
	select {
	case c.streamCredit <- struct{}{}:
	default:
	}
}

// maybeBroadcastSendWindow performs the end-of-burst congestion-window wake
// (docs/HTTP3_DESIGN.md §3.3, INV-5). blockCong/blockPace senders have no
// frame-driven wake — the window opens only through ACK/loss accounting — so a
// receive burst that freed in-flight bytes or raised connection credit sets
// sendWindowGrew, and this does one O(n) signalReady sweep over the open streams
// and clears the flag. Gated so the sweep is per-window-growing-burst, not per
// packet. Called at the end of the Poll receive burst, under c.mu.
func (c *Conn) maybeBroadcastSendWindow() {
	if !c.sendWindowGrew {
		return
	}
	c.sendWindowGrew = false
	for _, s := range c.streams {
		c.signalReady(s)
	}
}

// terminateLocked is the single-close latch (docs/HTTP3_DESIGN.md §3.3): it
// records the first terminating error and closes c.done exactly once so every
// blocked WaitReadable / WaitSendable wakes. Idempotent and first-error-wins —
// later callers (there are several independent teardown paths) no-op. Assumes
// c.mu is held.
//
// Every terminal path now funnels here: closeWithErrorLocked (which covers
// Close, fail's CONNECTION_CLOSE and sealPacket's AEAD-limit close), idleClose,
// the stateless-reset detection, Poll's reader fatals, and Establish's
// abandoned handshake. Nothing outside this file may latch by hand — see
// TestConn_Establish_LatchesOnEveryErrorPath for why the Establish one is a
// wrapper around the body rather than a call at each return.
func (c *Conn) terminateLocked(err error) {
	if c.terminated {
		return
	}
	c.terminated = true
	if c.closeErr == nil {
		c.closeErr = err
	}
	// Release the TLS handshake. crypto/tls runs a QUICConn's handshake on its own
	// goroutine that only exits when the handshake completes or Close cancels it,
	// so a connection torn down mid-handshake would leak it. This latch runs once
	// on every terminal path, which makes it the one place that must not miss.
	// (hs is nil only for hand-built test connections.)
	if c.hs != nil {
		_ = c.hs.Close()
	}
	// done is nil only for hand-built test Conns that never use the wake latch;
	// NewConn always allocates it, so a real connection always closes it here.
	if c.done != nil {
		close(c.done)
	}
}

// onAckRange acknowledges the packet-number range [low, high] in space sp,
// updating the RTT estimate from the largest newly-acknowledged packet
// (RFC 9002 §5). ackDelay is the peer's decoded ACK delay (zero for ranges that
// cannot carry the largest acked).
func (c *Conn) onAckRange(sp int, low, high uint64, ackDelay time.Duration, priorInFlight uint64) {
	// Whether the flight was congestion-limited is decided once from the bytes in
	// flight before this acknowledgement removed anything (RFC 9002 §7.8), so a
	// single ACK covering a full-window burst does not misread the later packets as
	// application-limited after the earlier ones are freed.
	limited := c.cwndLimited(priorInFlight)
	if c.ccAlgo == CCBBR {
		c.rsPriorValid = false // reset the BBR delivery-rate representative for this range
	}
	// Credit the congestion controller for each newly acknowledged ack-eliciting
	// packet before ack() removes it (RFC 9002 §7.3). Iterate our own sent packets
	// rather than the ACK range, so a malformed huge range cannot make this costly.
	for pn, p := range c.sent[sp].packets {
		if pn >= low && pn <= high && p.ackEliciting {
			c.onPacketAcked(p, limited)
			// Remember the send time so loss detection can tell whether an
			// acknowledgement fell inside a lost span (RFC 9002 §7.6.1).
			c.sent[sp].ackedElicit = append(c.sent[sp].ackedElicit, p.timeSent)
		}
	}
	if sendTime, ok := c.sent[sp].ack(c, low, high); ok {
		firstSample := !c.rtt.haveSample
		c.rtt.update(c.clock().Sub(sendTime), ackDelay)
		if firstSample {
			// Persistent congestion (§7.6) only considers packets sent after the first
			// RTT sample, so it cannot fire on the pre-handshake flight.
			c.firstRTTSample = c.clock()
		}
		c.ptoCount = 0 // §6.2.1: a newly-acked ack-eliciting packet resets the backoff
		if c.ccAlgo == CCBBR {
			c.bbrUpdateMinRTT(c.rtt.latestRTT, c.clock()) // feed BBR's 10 s windowed-min filter
		}
	}
	// BBR derives cwnd and pacing_rate from the delivery-rate model once per ACK
	// range; NewReno never enters here (its window is grown in onPacketAcked).
	if c.ccAlgo == CCBBR {
		c.bbrOnAckRange()
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

// ConnectionState returns the TLS connection state once the handshake has
// progressed enough to populate it (ALPN, negotiated cipher suite, peer
// certificates). It is a snapshot of the underlying crypto/tls state.
func (c *Conn) ConnectionState() tls.ConnectionState { return c.hs.ConnectionState() }

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
	if tp.HaveStatelessResetToken {
		c.registerResetToken(0, tp.StatelessResetToken) // bound to the server's handshake CID, sequence 0 (§10.3)
	}
	// Authenticate the server's connection ID (RFC 9000 §7.3): its
	// initial_source_connection_id MUST be present and equal the Source Connection
	// ID of the server's first Initial packet, which the client adopted as its
	// destination CID. An absent or mismatched value signals a spoofed handshake.
	if !tp.HaveInitialSourceConnectionID || !bytes.Equal(tp.InitialSourceConnectionID, c.serverSCID) {
		return ErrTransportParameter
	}
	// The server MUST include original_destination_connection_id, and it MUST equal
	// the Destination Connection ID of the client's first Initial (RFC 9000 §7.3).
	// This authenticates the CID exchange against an off-path injection.
	if !tp.HaveOriginalDestinationConnectionID || !bytes.Equal(tp.OriginalDestinationConnectionID, c.origDCID) {
		return ErrTransportParameter
	}
	// retry_source_connection_id MUST be present exactly when a Retry was processed,
	// and MUST equal that Retry's Source Connection ID (RFC 9000 §7.3). Its presence
	// without a Retry, absence after one, or a mismatch is a spoofed exchange.
	if c.handledRetry {
		if !tp.HaveRetrySourceConnectionID || !bytes.Equal(tp.RetrySourceConnectionID, c.retrySCID) {
			return ErrTransportParameter
		}
	} else if tp.HaveRetrySourceConnectionID {
		return ErrTransportParameter
	}
	return nil
}

// registerResetToken remembers the stateless reset token the peer bound to the
// connection ID with the given sequence (RFC 9000 §10.3). Keying by sequence lets
// the token be dropped when that CID is retired, so §10.3.1's rule that a reset is
// only ever matched against the CID in use — never a retired or unused one — holds.
func (c *Conn) registerResetToken(seq uint64, t [16]byte) {
	if c.resetTokens == nil {
		c.resetTokens = map[uint64][16]byte{}
	}
	c.resetTokens[seq] = t
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
	if len(c.pendingCrypto[spaceInitial]) == 0 {
		return errNoClientHello
	}
	// The flight itself is an ordinary flush of the Initial space: its arm frames
	// the pumped CRYPTO, advances cryptoOffset, drains pendingCrypto, and seals
	// through sealPacket — which pads the datagram to 1200 (§14.1), records the
	// packet as ack-eliciting with a private copy of the ClientHello so a lost one
	// can be resent (§13.3), and charges it to bytes_in_flight (RFC 9002 §7). Only
	// the Initial space has a sealer and pending data at this point, so exactly one
	// datagram goes out.
	//
	// sealPacket puts c.retryToken in the header where this used to hard-code an
	// empty token. Not a behavior change: a Retry can only be processed from a
	// received datagram, and Establish calls this before reading any, so the token
	// is still empty here. Any later Initial — including a retransmit of this one —
	// already went through sealPacket and already carried the token (§8.1.2).
	return c.flush()
}
