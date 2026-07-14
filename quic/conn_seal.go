package quic

import "time"

// NOTE (RFC 9001 §6.6): the AEAD usage limits ARE enforced — sealPacket counts
// 1-RTT packets under the current key (appSendCount) and closes with
// AEAD_LIMIT_REACHED at the confidentiality limit, and openApp counts failed
// authentications (authFailures) toward the integrity limit. What remains
// deferred is SELF-INITIATED key update: this client is a pure responder, so at
// the confidentiality limit it closes rather than rotating its own keys. A peer
// that updates regularly (quic-go does) resets appSendCount before the limit is
// reached, so the close only fires against a peer that never updates.

// flushRetransmits resends each queued lost frame for space sp, one per packet,
// as a new ack-eliciting packet with a fresh packet number (retransmit takes
// priority, RFC 9000 §13.3). A STREAM frame for a stream whose send side has
// been reset is dropped (§13.3: its data is not retransmitted).
func (c *Conn) flushRetransmits(sp int) error {
	var sent uint64
	// Resent packets of the same size (e.g. a run of full-size STREAM datagrams from
	// one lost flight) coalesce into a single GSO write; a size change or a cap flushes
	// the batch and starts a new one (quic/gso.go). Each packet is still sealed and
	// recorded individually, so retransmit accounting and packet numbers are unchanged.
	batch := c.newBatch()
	for len(c.retransQueue[sp]) > 0 {
		if c.cwnd > 0 {
			// A retransmission counts against the congestion window like any other
			// ack-eliciting packet (RFC 9002 §7): stop once the window is full. Loss
			// detection removes the lost bytes from bytes_in_flight before queueing the
			// retransmit, so there is room to resend. A queued PTO probe is exempt from
			// the window (§7) and MUST send at least one packet (§6.2.4), so let exactly
			// one packet through; the remainder drains on the next flush.
			if c.bytesInFlight >= c.cwnd {
				if !c.ptoExempt {
					break
				}
				c.ptoExempt = false
			}
			// Burst limit (RFC 9002 §7.7): resend at most an initial congestion window
			// per flush so a whole-flight loss is not retransmitted back-to-back.
			if sent >= kInitialWindow {
				break
			}
		}
		rf := c.retransQueue[sp][0]
		c.retransQueue[sp] = c.retransQueue[sp][1:]
		if rf.kind == retransStream {
			if s := c.streams[rf.streamID]; s != nil && s.sendReset {
				continue
			}
		}
		if rf.kind == retransRetire && c.pendingRetires > 0 {
			c.pendingRetires-- // a queued RETIRE_CONNECTION_ID is now on the wire
		}
		pkt, err := c.sealPacket(sp, rf.encode(nil), true, []retransFrame{rf}, false)
		if err != nil {
			_ = c.flushBatch(&batch) // best-effort: send what was already sealed
			return err
		}
		if err := c.addToBatch(&batch, pkt); err != nil {
			return err
		}
		sent += uint64(len(pkt))
	}
	return c.flushBatch(&batch)
}

// handshakeProbeSpace returns the packet-number space a pending pre-handshake
// anti-deadlock PING (RFC 9002 §6.2.2.1) is sent in — the highest space with keys,
// Handshake if available else Initial (which sealPacket pads to 1200, §14.1) — or
// -1 when no such probe is pending.
func (c *Conn) handshakeProbeSpace() int {
	if !c.handshakeProbe {
		return -1
	}
	if c.handshakeSealer != nil {
		return spaceHandshake
	}
	return spaceInitial
}

// flush sends, for every space that owes a retransmission, CRYPTO, or an ACK:
// first one packet per queued retransmit frame (retransmit takes priority,
// RFC 9000 §13.3), then one packet with any pending ACK and new CRYPTO.
// Assumes c.mu is held (called from Poll/Establish and the send helpers).
func (c *Conn) flush() error {
	if c.closed {
		// Draining or closing (RFC 9000 §10.2.2): once the connection is closed —
		// including by a received CONNECTION_CLOSE — no further packets may be sent.
		// The single final CONNECTION_CLOSE, if any, is emitted directly by
		// CloseWithError, never through flush.
		return nil
	}
	hsProbeSpace := c.handshakeProbeSpace()
	for sp := 0; sp < numSpaces; sp++ {
		hasCtrl := sp == spaceApp && len(c.pendingCtrl) > 0
		hasProbe := (sp == spaceApp && c.probePending) || sp == hsProbeSpace
		// A deferred-but-not-yet-due Application ACK (RFC 9000 §13.2.1) does not by
		// itself warrant a packet — it rides the next outbound one or the timer — so
		// the "is there work" test uses ackDue, not ackPending.
		nothing := len(c.pendingCrypto[sp]) == 0 && !c.ackDue(sp) &&
			len(c.retransQueue[sp]) == 0 && !hasCtrl && !hasProbe
		if nothing || c.sealerFor(sp) == nil {
			continue // idle, or keys not yet available
		}
		if err := c.flushRetransmits(sp); err != nil {
			return err
		}
		if len(c.pendingCrypto[sp]) == 0 && !c.ackDue(sp) && !hasCtrl && !hasProbe {
			continue
		}
		// We are building a packet for this space anyway, so coalesce any owed ACK —
		// even one still inside its deferral window — clearing the deferral. Sending
		// early on a packet we must send is always within max_ack_delay (§13.2.1).
		frames := c.takePendingAck(sp, nil)
		padPath := false
		if hasCtrl {
			// MAX_DATA / MAX_STREAM_DATA credit grants (§4.1). Not retransmitted:
			// a later grant supersedes a lost one, so recovery is self-healing.
			frames = append(frames, c.pendingCtrl...)
			c.pendingCtrl = c.pendingCtrl[:0]
			padPath, c.pathRespPending = c.pathRespPending, false // §8.2.2 pad if a PATH_RESPONSE rode along
		}
		if hasProbe {
			// A PTO probe with nothing else to resend (RFC 9002 §6.2.4, §6.2.2.1): a
			// bare PING to elicit an ACK. Not retransmitted — a later PTO re-probes.
			// At most one probe flag is ever set (onPTO chooses one), so clearing
			// both here is safe once this space's probe is emitted.
			frames = AppendPing(frames)
			c.probePending, c.handshakeProbe = false, false
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
		// A packet carrying CRYPTO, a credit grant, or a PING probe is ack-eliciting.
		pkt, err := c.sealPacket(sp, frames, hasCrypto || hasCtrl || hasProbe, retrans, padPath)
		if err != nil {
			return err
		}
		if _, err := c.pc.Write(pkt); err != nil {
			return err
		}
	}
	// Drop any PTO exemption not consumed above (the probe found room under the
	// window), so it cannot leak past this flush onto an ordinary retransmit.
	c.ptoExempt = false
	return nil
}

// takePendingAck appends the owed ACK for space sp to frames — clearing the owed
// state and, for the Application space, the deferred-ACK deadline (RFC 9000
// §13.2.1) — and returns the extended slice. A no-op returning frames unchanged
// when no ACK is owed. Assumes c.mu is held.
func (c *Conn) takePendingAck(sp int, frames []byte) []byte {
	if !c.acks[sp].ackPending() {
		return frames
	}
	frames = c.acks[sp].appendACK(frames, 0)
	if sp == spaceApp {
		c.ackDeadline = time.Time{}
	}
	return frames
}

// sealPacket builds and protects a packet for a space carrying frames. Frames
// are padded to keep the packet long enough for the header-protection sample
// (RFC 9001 §5.4.2). Assumes c.mu is held (it advances sendPN and touches the
// sealer/pacing/congestion state, all guarded by c.mu).
func (c *Conn) sealPacket(sp int, frames []byte, ackEliciting bool, retrans []retransFrame, padTo1200 bool) ([]byte, error) {
	// AEAD confidentiality limit (RFC 9001 §6.6): this client cannot initiate its
	// own key update, so once it has sealed the limit of 1-RTT packets under the
	// current key it must stop and close with AEAD_LIMIT_REACHED rather than seal
	// another. The !c.closed guard lets the CONNECTION_CLOSE packet itself seal.
	if sp == spaceApp && !c.closed && c.appSendCount >= aeadConfidentialityLimit {
		// closeWithErrorLocked, not the public CloseWithError: sealPacket already
		// holds c.mu, so re-locking would self-deadlock the single mutex.
		_ = c.closeWithErrorLocked(false, ErrCodeAEADLimitReached, "")
		return nil, ErrAEADLimit
	}
	minFrames := 20 // ensures payload+tag reaches the 16-byte HP sample
	if sp == spaceInitial {
		// RFC 9000 §14.1: a datagram carrying an Initial packet MUST be at least
		// 1200 bytes or the server discards it. Pad the frames so the whole
		// datagram (header + pn + frames + 16-byte tag) reaches that minimum. The
		// header includes the token varint + token bytes (a Retry token, or empty).
		tok := 1 + len(c.retryToken)
		if len(c.retryToken) >= 0x40 {
			tok++ // a token of 64+ bytes needs a 2-byte length varint
		}
		hdr := 1 + 4 + 1 + len(c.dcid) + 1 + len(c.scid) + tok + 2 // +2-byte length field
		if need := InitialDatagramMinSize - hdr - 4 - 16; need > minFrames {
			minFrames = need
		}
	} else if padTo1200 {
		// RFC 9000 §8.2.2: a datagram carrying a PATH_RESPONSE MUST be expanded to at
		// least 1200 bytes, to confirm the path carries that size in both directions
		// and to limit amplification. The short header is the first byte plus the DCID.
		hdr := 1 + len(c.dcid)
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

	// Build the header into the reused hdrScratch and seal into the reused
	// sealScratch (see the Conn scratch fields' doc): both are consumed by the
	// immediately-following pc.Write and never retained, so reuse under c.mu is
	// safe. Reassigning the fields captures any capacity growth for the next seal.
	var pnOffset int
	switch sp {
	case spaceInitial:
		c.hdrScratch, pnOffset = AppendLongHeader(c.hdrScratch[:0], PacketInitial, QUICVersion1, c.dcid, c.scid, c.retryToken, pnLen, length)
	case spaceHandshake:
		c.hdrScratch, pnOffset = AppendLongHeader(c.hdrScratch[:0], PacketHandshake, QUICVersion1, c.dcid, c.scid, nil, pnLen, length)
	default:
		c.hdrScratch, pnOffset = AppendShortHeader(c.hdrScratch[:0], c.dcid, pnLen, c.appSendPhase())
	}
	for i := pnLen - 1; i >= 0; i-- {
		c.hdrScratch = append(c.hdrScratch, byte(pn>>(8*uint(i))))
	}
	pkt, err := c.sealerFor(sp).Seal(c.sealScratch[:0], c.hdrScratch, pnOffset, pnLen, pn, frames)
	if err != nil {
		return nil, err
	}
	c.sealScratch = pkt                            // retain the (possibly grown) backing array for the next seal
	c.onPacketSent(sp, pn, ackEliciting, len(pkt)) // congestion accounting (RFC 9002 §7)
	if sp == spaceApp {
		c.appSendCount++ // toward the AEAD confidentiality limit (§6.6)
		if ackEliciting {
			// Debit the pacing bucket by the on-wire size so the burst limit is in the
			// same units as the congestion window; ACK-only packets are not paced
			// (RFC 9002 §7.7).
			c.pacingOnSend(uint64(len(pkt)))
		}
	}
	return pkt, nil
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
