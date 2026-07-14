package quic

import (
	"crypto/subtle"
	"crypto/tls"
)

// shouldAbandonOnVN decides whether a received Version Negotiation packet makes
// the client abandon the connection attempt (RFC 9000 §6.2). This client offers
// only QUIC v1, so a genuine VN — one it has not already superseded and that does
// not list v1 — means no common version exists. The two exceptions are discarded,
// not abandoned: a VN received after another server packet was already processed
// (stale or spoofed), and one whose Supported Versions list includes v1 (which a
// genuine VN never does).
func (c *Conn) shouldAbandonOnVN(pkt []byte, hdr Header) bool {
	// A VN is discarded once an Initial or Retry has been processed (RFC 9000
	// §6.2, §17.2.5.2): the handshake is already under way, so a later VN is stale
	// or spoofed.
	if c.handledRetry || c.haveRecv[spaceInitial] || c.haveRecv[spaceHandshake] || c.haveRecv[spaceApp] {
		return false
	}
	return !vnOffers(pkt, hdr, QUICVersion1)
}

// isStatelessReset reports whether the first packet of an undecryptable datagram
// is a stateless reset: at least 21 bytes and ending in the reset token bound to
// the connection ID currently in use (RFC 9000 §10.3, §10.3.1). Only the in-use
// CID's token is matched — never a retired or unused one, per §10.3.1 — and the
// comparison is constant time so a near-miss token cannot be probed by timing. It
// is a no-op unless isFirst.
func (c *Conn) isStatelessReset(isFirst bool, datagram []byte) bool {
	if !isFirst || len(datagram) < 21 {
		return false
	}
	token, ok := c.resetTokens[c.curCIDSeq]
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare(datagram[len(datagram)-16:], token[:]) == 1
}

// statelessResetReceived tears the connection down on a stateless reset (RFC 9000
// §10.3): the state is discarded and no CONNECTION_CLOSE is sent (the peer has no
// connection state), so ErrStatelessReset is returned to the caller. The transport
// socket is closed too, mirroring idleClose, so no descriptor leaks.
func (c *Conn) statelessResetReceived() error {
	// Latch the single-close state so a blocked WaitReadable / WaitSendable wakes
	// with ErrStatelessReset (docs/HTTP3_DESIGN.md §3.3, F5).
	c.terminateLocked(ErrStatelessReset)
	c.closed = true
	_ = c.pc.Close()
	return ErrStatelessReset
}

// decryptPacket removes protection from a received packet in space sp, returning
// its packet number and frame payload. ok is false when the packet cannot be
// decrypted — no keys installed yet, or authentication failed — which is never
// fatal; the caller skips the packet. A non-nil err ends the connection: a
// short-header failure that is a stateless reset (RFC 9000 §10.3.1) or the AEAD
// integrity limit being exceeded (RFC 9001 §6.6).
func (c *Conn) decryptPacket(sp int, pkt []byte, pnOffset int, isFirst bool, datagram []byte) (pn uint64, payload []byte, ok bool, err error) {
	if sp == spaceApp {
		// The application space may carry a key update (RFC 9001 §6), so it needs the
		// key-phase-aware open path.
		if pn, payload, ok = c.openApp(pkt, pnOffset); ok {
			return pn, payload, true, nil
		}
		if c.isStatelessReset(isFirst, datagram) {
			return 0, nil, false, c.statelessResetReceived()
		}
		if c.authFailures > c.integrityLimit() {
			return 0, nil, false, ErrAEADLimit
		}
		return 0, nil, false, nil
	}
	op := c.openerFor(sp)
	if op == nil {
		return 0, nil, false, nil // keys for this level not yet installed
	}
	if pn, _, payload, err = op.Open(pkt, pnOffset, c.largestRecv[sp]); err != nil {
		return 0, nil, false, nil // authentication failed; skip
	}
	return pn, payload, true, nil
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
			c.authFailures++ // a decryption failure counts toward the integrity limit (§6.6)
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
			c.authFailures++
			return 0, nil, false
		}
		return pn, payload, true
	}
	// Flipped Key Phase: a reordered previous-generation packet (below the
	// boundary, within the retention window) decrypts with the retained prev keys.
	if c.ku.prev != nil && c.ku.haveBoundary && pn < c.ku.boundary && c.clock().Before(c.ku.prevUntil) {
		payload, err = c.ku.prev.openAEAD(pkt, pnOffset, pnLen, pn)
		if err != nil {
			c.authFailures++
			return 0, nil, false
		}
		return pn, payload, true
	}
	// Otherwise trial-decrypt with the next generation (RFC 9001 §6.2).
	payload, err = c.ku.next.openAEAD(pkt, pnOffset, pnLen, pn)
	if err != nil {
		c.authFailures++
		return 0, nil, false
	}
	if !c.handshakeConfirmed {
		return 0, nil, false // MUST NOT accept a key update before the handshake is confirmed (§6.1)
	}
	c.commitKeyUpdate(pn) // peer initiated an update — ratchet forward and flip our send phase
	return pn, payload, true
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
