package quic

import (
	"context"
	"crypto/tls"
	"time"
)

// Establish sends the client's Initial flight, then reads datagrams and drives
// the handshake — feeding inbound CRYPTO to TLS, installing keys, and sending
// the client's responding flights and acknowledgements — until the handshake
// completes (RFC 9000 §7, RFC 9001 §4). ctx bounds only the initial send; the
// caller sets a read deadline on the PacketConn to bound the loop.
func (c *Conn) Establish(ctx context.Context) error {
	if err := c.sendInitialFlight(ctx); err != nil {
		return err
	}
	buf := make([]byte, 2048)
	for !c.handshakeComplete && !c.closed {
		n, err := c.readWithPTO(buf)
		if err != nil {
			return err
		}
		if err := c.recvDatagram(buf[:n]); err != nil {
			return err
		}
		if err := c.flush(); err != nil {
			return err
		}
	}
	if !c.handshakeComplete {
		return ErrHandshakeClosed // loop exited on c.closed before completing
	}
	return nil
}

// Poll reads one datagram and processes it — dispatching frames to open streams
// and sending acknowledgements — for driving receive after the handshake
// completes (RFC 9000 §13). The caller sets a read deadline on the PacketConn to
// bound the wait.
func (c *Conn) Poll() error {
	if c.pollBuf == nil {
		c.pollBuf = make([]byte, 2048)
	}
	// Flush any output queued since the last read — notably MAX_STREAM_DATA /
	// MAX_DATA credit grants from the application consuming a response — before
	// blocking, or a flow-control-blocked peer would never send the data that
	// would unblock the read (a deadlock until the idle timeout).
	if err := c.flush(); err != nil {
		return err
	}
	n, err := c.readWithPTO(c.pollBuf)
	if err != nil {
		return err
	}
	if err := c.recvDatagram(c.pollBuf[:n]); err != nil {
		return err
	}
	c.discardStaleKeys() // drop a superseded key-update generation past its window (§6.3)
	// Drain datagrams already buffered in the socket without blocking, so a
	// server's response burst is processed and acknowledged as one batch rather
	// than one datagram per Poll — one-at-a-time is too slow for a bulk transfer,
	// the kernel receive buffer overflows, and the peer treats the resulting gaps
	// as loss and collapses its congestion window (RFC 9002 §7).
	if err := c.drainBuffered(); err != nil {
		return err
	}
	return c.flush()
}

// maxDrainBurst bounds how many extra datagrams a single Poll drains after its
// blocking read, so the acknowledgement flush is not starved by a peer sending
// as fast as we read. A full receive-window burst fits well under this; the cap
// is only a runaway guard.
const maxDrainBurst = 512

// drainBuffered reads and processes datagrams already waiting in the socket
// without blocking, using a read deadline in the past so an empty socket returns
// immediately. It is a no-op on transports that cannot set a read deadline (they
// have no non-blocking read), preserving their one-datagram-per-Poll behavior.
func (c *Conn) drainBuffered() error {
	dl, ok := c.pc.(interface{ SetReadDeadline(time.Time) error })
	if !ok {
		return nil
	}
	for i := 0; i < maxDrainBurst; i++ {
		_ = dl.SetReadDeadline(c.clock().Add(-time.Nanosecond))
		n, err := c.pc.Read(c.pollBuf)
		if err != nil {
			if isTimeout(err) {
				return nil // socket drained
			}
			return err
		}
		if err := c.recvDatagram(c.pollBuf[:n]); err != nil {
			return err
		}
	}
	return nil
}

// recvDatagram processes every packet coalesced in a datagram: decrypt with the
// space's Opener, dispatch frames, record the packet number, then feed any
// reassembled CRYPTO to the TLS handshake and pump the resulting events.
func (c *Conn) recvDatagram(datagram []byte) error {
	rest := datagram
	for len(rest) > 0 {
		hdr, err := ParseHeader(rest, len(c.scid))
		if err != nil {
			return err
		}
		pkt := rest[:hdr.PacketLen]
		rest = rest[hdr.PacketLen:]
		if hdr.Type == PacketRetry || hdr.Type == PacketVersionNegotiation {
			continue // no protected payload; handled in a later phase
		}
		// Adopt the server's connection ID as our destination (RFC 9000 §7.2).
		if !c.gotServerCID && len(hdr.SCID) > 0 {
			c.dcid = append(c.dcid[:0], hdr.SCID...)
			c.gotServerCID = true
		}
		sp := packetSpace(hdr.Type)
		var pn uint64
		var payload []byte
		if sp == spaceApp {
			// The application space may carry a key update (RFC 9001 §6), so it
			// needs the key-phase-aware open path.
			var ok bool
			pn, payload, ok = c.openApp(pkt, hdr.PNOffset)
			if !ok {
				continue // no keys yet, or authentication failed; skip
			}
		} else {
			op := c.openerFor(sp)
			if op == nil {
				continue // keys for this level not yet installed
			}
			var err error
			pn, _, payload, err = op.Open(pkt, hdr.PNOffset, c.largestRecv[sp])
			if err != nil {
				continue // authentication failed; skip
			}
		}
		fh := connFrameHandler{c: c, space: sp}
		if err := ParseFrames(payload, &fh); err != nil {
			return err
		}
		c.acks[sp].receive(pn, fh.ackEliciting)
		if !c.haveRecv[sp] || pn > c.largestRecv[sp] {
			c.largestRecv[sp] = pn
			c.haveRecv[sp] = true
		}
		// An ACK removed acknowledged packets and updated the RTT during parsing;
		// now run loss detection with the fresh RTT (RFC 9002 §6.1, §A.7 order).
		if fh.sawAck {
			c.detectLost(sp)
		}
	}
	fedCrypto := false
	for sp := 0; sp < numSpaces; sp++ {
		if data := c.cryptoRecv[sp].read(); len(data) > 0 {
			if err := c.hs.HandleCrypto(spaceLevel(sp), data); err != nil {
				return err
			}
			fedCrypto = true
		}
	}
	// Drive TLS only while the handshake runs or when new CRYPTO arrived
	// (e.g. a post-handshake session ticket); otherwise there is nothing to do.
	if fedCrypto || !c.handshakeComplete {
		return c.hs.Pump(c)
	}
	return nil
}

// openApp removes protection from a received application (1-RTT) packet,
// handling RFC 9001 §6 key updates. It unprotects the header once with the shared
// (non-rotated) header-protection key, reads the Key Phase bit, then makes exactly
// one AEAD attempt against the matching key generation. A packet whose phase
// differs from the current one is either a reordered previous-generation packet
// (packet number below the generation boundary) or a peer-initiated update, which
// is trial-decrypted with the pre-derived next generation and committed on success
// (§6.2). ok is false for a keyless or undecryptable packet — never fatal, so a
// forged Key Phase bit costs one AEAD open and is dropped (§6.4).
func (c *Conn) openApp(pkt []byte, pnOffset int) (pn uint64, payload []byte, ok bool) {
	op := c.keys.OneRTT
	if op == nil {
		return 0, nil, false // 1-RTT keys not installed yet
	}
	if c.ku == nil {
		// No key-update state (e.g. a hand-built test Conn): plain 1-RTT open.
		p, _, pl, err := op.Open(pkt, pnOffset, c.largestRecv[spaceApp])
		if err != nil {
			return 0, nil, false
		}
		return p, pl, true
	}
	pn, pnLen, keyPhase, err := op.unprotectHeader(pkt, pnOffset, c.largestRecv[spaceApp])
	if err != nil {
		return 0, nil, false // too short to sample / malformed
	}
	if keyPhase == c.ku.phase {
		payload, err = op.openAEAD(pkt, pnOffset, pnLen, pn)
		if err != nil {
			return 0, nil, false
		}
		return pn, payload, true
	}
	// Flipped Key Phase: a reordered previous-generation packet (below the
	// boundary, within the retention window) decrypts with the retained prev keys.
	if c.ku.prev != nil && c.ku.haveBoundary && pn < c.ku.boundary && c.clock().Before(c.ku.prevUntil) {
		payload, err = c.ku.prev.openAEAD(pkt, pnOffset, pnLen, pn)
		if err != nil {
			return 0, nil, false
		}
		return pn, payload, true
	}
	// Otherwise trial-decrypt with the next generation (RFC 9001 §6.2).
	payload, err = c.ku.next.openAEAD(pkt, pnOffset, pnLen, pn)
	if err != nil {
		return 0, nil, false
	}
	if !c.handshakeConfirmed {
		return 0, nil, false // MUST NOT accept a key update before the handshake is confirmed (§6.1)
	}
	c.commitKeyUpdate(pn) // peer initiated an update — ratchet forward and flip our send phase
	return pn, payload, true
}

// NOTE (RFC 9001 §6.6, deferred): this client is a pure responder — it never
// initiates its own key update, so it does not enforce the AEAD confidentiality
// limit (2^23 packets under one AES-GCM key). Reaching it requires ~10 GB on a
// single connection with a peer that never updates; real peers (quic-go) do
// initiate. Self-initiated key update + the send-packet counter are a follow-up
// phase (they need separate read/write key phases, since an initiator flips its
// write phase before its read phase).

// flush sends, for every space that owes a retransmission, CRYPTO, or an ACK:
// first one packet per queued retransmit frame (retransmit takes priority,
// RFC 9000 §13.3), then one packet with any pending ACK and new CRYPTO.
func (c *Conn) flush() error {
	for sp := 0; sp < numSpaces; sp++ {
		hasCtrl := sp == spaceApp && len(c.pendingCtrl) > 0
		nothing := len(c.pendingCrypto[sp]) == 0 && !c.acks[sp].ackPending() &&
			len(c.retransQueue[sp]) == 0 && !hasCtrl
		if nothing || c.sealerFor(sp) == nil {
			continue // idle, or keys not yet available
		}
		// Resend lost frames, one per packet, each as a new (retransmittable)
		// ack-eliciting packet with a fresh packet number.
		for len(c.retransQueue[sp]) > 0 {
			rf := c.retransQueue[sp][0]
			c.retransQueue[sp] = c.retransQueue[sp][1:]
			pkt, err := c.sealPacket(sp, rf.encode(nil), true, []retransFrame{rf})
			if err != nil {
				return err
			}
			if _, err := c.pc.Write(pkt); err != nil {
				return err
			}
		}
		if len(c.pendingCrypto[sp]) == 0 && !c.acks[sp].ackPending() && !hasCtrl {
			continue
		}
		var frames []byte
		if c.acks[sp].ackPending() {
			frames = c.acks[sp].appendACK(frames, 0)
		}
		if hasCtrl {
			// MAX_DATA / MAX_STREAM_DATA credit grants (§4.1). Not retransmitted:
			// a later grant supersedes a lost one, so recovery is self-healing.
			frames = append(frames, c.pendingCtrl...)
			c.pendingCtrl = c.pendingCtrl[:0]
		}
		var retrans []retransFrame
		hasCrypto := len(c.pendingCrypto[sp]) > 0
		if hasCrypto {
			off := c.cryptoOffset[sp]
			data := append([]byte(nil), c.pendingCrypto[sp]...) // copy retained for resend
			frames = AppendCrypto(frames, off, c.pendingCrypto[sp])
			c.cryptoOffset[sp] += uint64(len(c.pendingCrypto[sp]))
			c.pendingCrypto[sp] = c.pendingCrypto[sp][:0]
			retrans = []retransFrame{{kind: retransCrypto, offset: off, data: data}}
		}
		// A packet carrying CRYPTO or a credit grant is ack-eliciting.
		pkt, err := c.sealPacket(sp, frames, hasCrypto || hasCtrl, retrans)
		if err != nil {
			return err
		}
		if _, err := c.pc.Write(pkt); err != nil {
			return err
		}
	}
	return nil
}

// sealPacket builds and protects a packet for a space carrying frames. Frames
// are padded to keep the packet long enough for the header-protection sample
// (RFC 9001 §5.4.2).
func (c *Conn) sealPacket(sp int, frames []byte, ackEliciting bool, retrans []retransFrame) ([]byte, error) {
	minFrames := 20 // ensures payload+tag reaches the 16-byte HP sample
	if sp == spaceInitial {
		// RFC 9000 §14.1: a datagram carrying an Initial packet MUST be at least
		// 1200 bytes or the server discards it. Pad the frames so the whole
		// datagram (header + pn + frames + 16-byte tag) reaches that minimum.
		hdr := 1 + 4 + 1 + len(c.dcid) + 1 + len(c.scid) + 1 + 2 // +token len 0, +2-byte length
		if need := InitialDatagramMinSize - hdr - 4 - 16; need > minFrames {
			minFrames = need
		}
	}
	if len(frames) < minFrames {
		frames = append(frames, make([]byte, minFrames-len(frames))...) // PADDING
	}
	pnLen := 4
	pn := c.sendPN[sp]
	c.sendPN[sp]++
	c.sent[sp].onSent(pn, c.clock(), ackEliciting, retrans)
	length := uint64(pnLen + len(frames) + 16)

	var hdr []byte
	var pnOffset int
	switch sp {
	case spaceInitial:
		hdr, pnOffset = AppendLongHeader(nil, PacketInitial, QUICVersion1, c.dcid, c.scid, nil, pnLen, length)
	case spaceHandshake:
		hdr, pnOffset = AppendLongHeader(nil, PacketHandshake, QUICVersion1, c.dcid, c.scid, nil, pnLen, length)
	default:
		hdr, pnOffset = AppendShortHeader(nil, c.dcid, pnLen, c.appSendPhase())
	}
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	return c.sealerFor(sp).Seal(nil, hdr, pnOffset, pnLen, pn, frames)
}

func (c *Conn) openerFor(sp int) *Opener {
	switch sp {
	case spaceInitial:
		return c.keys.Initial
	case spaceHandshake:
		return c.keys.Handshake
	default:
		return c.keys.OneRTT
	}
}

func (c *Conn) sealerFor(sp int) *Sealer {
	switch sp {
	case spaceInitial:
		return c.initialSealer
	case spaceHandshake:
		return c.handshakeSealer
	default:
		return c.oneRTTSealer
	}
}

func packetSpace(t PacketType) int {
	switch t {
	case PacketHandshake:
		return spaceHandshake
	case PacketShort:
		return spaceApp
	default:
		return spaceInitial
	}
}

func spaceLevel(sp int) tls.QUICEncryptionLevel {
	switch sp {
	case spaceHandshake:
		return tls.QUICEncryptionLevelHandshake
	case spaceApp:
		return tls.QUICEncryptionLevelApplication
	default:
		return tls.QUICEncryptionLevelInitial
	}
}

// connFrameHandler dispatches the frames of one received packet into the Conn.
type connFrameHandler struct {
	nopFrameHandler
	c            *Conn
	space        int
	ackEliciting bool
	sawAck       bool   // an ACK frame was seen (run loss detection after parsing)
	ackLow       uint64 // smallest packet number of the ACK range being decoded
}

// OnAck processes the first (largest) range of an ACK frame: it acknowledges
// [largest-firstRange, largest] and samples the RTT from the largest packet
// (RFC 9000 §19.3, RFC 9002 §5). ACK frames are not themselves ack-eliciting.
func (h *connFrameHandler) OnAck(largest, ackDelay, firstRange uint64) error {
	h.sawAck = true
	delay := time.Duration(ackDelay<<ackDelayExponent) * time.Microsecond
	low := largest - firstRange
	h.c.onAckRange(h.space, low, largest, delay)
	h.ackLow = low
	return nil
}

// OnAckRange processes an additional ACK range below the previous one: a gap of
// gap+1 unacknowledged packets, then length+1 acknowledged (RFC 9000 §19.3).
func (h *connFrameHandler) OnAckRange(gap, length uint64) error {
	high := h.ackLow - gap - 2
	low := high - length
	h.c.onAckRange(h.space, low, high, 0) // only the first range carries the largest
	h.ackLow = low
	return nil
}

func (h *connFrameHandler) OnCrypto(offset uint64, data []byte) error {
	// The handshake CRYPTO stream spans many frames and packets (a server's
	// certificate flight is several KB); reassemble it by offset so out-of-order
	// or gapped delivery still yields the TLS messages in order.
	h.c.cryptoRecv[h.space].receive(offset, data, false)
	h.ackEliciting = true
	return nil
}

func (h *connFrameHandler) OnPing() error { h.ackEliciting = true; return nil }

func (h *connFrameHandler) OnStream(id, offset uint64, fin bool, data []byte) error {
	h.ackEliciting = true
	// The server's response arrives on the client-initiated bidirectional
	// stream the request opened; deliver it there. Frames for unknown streams
	// are ignored until stream acceptance is implemented.
	if s := h.c.streams[id]; s != nil {
		s.recv.receive(offset, data, fin)
	}
	return nil
}

func (h *connFrameHandler) OnMaxData(maximum uint64) error {
	h.ackEliciting = true
	if maximum > h.c.connMax { // absolute ceiling; ignore non-increasing (§4.1)
		h.c.connMax = maximum
	}
	return nil
}

func (h *connFrameHandler) OnMaxStreamData(streamID, maximum uint64) error {
	h.ackEliciting = true
	if s := h.c.streams[streamID]; s != nil && maximum > s.sendMax {
		s.sendMax = maximum
	}
	return nil
}

func (h *connFrameHandler) OnHandshakeDone() error {
	h.c.handshakeComplete = true
	// HANDSHAKE_DONE confirms the handshake for a client (RFC 9001 §4.1.2), which
	// is the precondition for accepting a key update (§6.1) — distinct from TLS
	// completion, which fires earlier.
	h.c.handshakeConfirmed = true
	h.ackEliciting = true
	return nil
}

func (h *connFrameHandler) OnConnectionClose(bool, uint64, uint64, []byte) error {
	h.c.closed = true
	return nil
}
