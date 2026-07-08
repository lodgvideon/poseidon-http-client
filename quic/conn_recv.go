package quic

import (
	"context"
	"crypto/tls"
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
		n, err := c.pc.Read(buf)
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
		op := c.openerFor(sp)
		if op == nil {
			continue // keys for this level not yet installed
		}
		pn, _, payload, err := op.Open(pkt, hdr.PNOffset, c.largestRecv[sp])
		if err != nil {
			continue // authentication failed; skip
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
	}
	for sp := 0; sp < numSpaces; sp++ {
		if len(c.recvCrypto[sp]) > 0 {
			if err := c.hs.HandleCrypto(spaceLevel(sp), c.recvCrypto[sp]); err != nil {
				return err
			}
			c.recvCrypto[sp] = c.recvCrypto[sp][:0]
		}
	}
	return c.hs.Pump(c)
}

// flush sends, for every space that owes CRYPTO or an ACK, one packet carrying
// an ACK frame (if pending) followed by any buffered CRYPTO.
func (c *Conn) flush() error {
	for sp := 0; sp < numSpaces; sp++ {
		if len(c.pendingCrypto[sp]) == 0 && !c.acks[sp].ackPending() {
			continue
		}
		if c.sealerFor(sp) == nil {
			continue // keys not yet available
		}
		var frames []byte
		if c.acks[sp].ackPending() {
			frames = c.acks[sp].appendACK(frames, 0)
		}
		if len(c.pendingCrypto[sp]) > 0 {
			frames = AppendCrypto(frames, c.cryptoOffset[sp], c.pendingCrypto[sp])
			c.cryptoOffset[sp] += uint64(len(c.pendingCrypto[sp]))
			c.pendingCrypto[sp] = c.pendingCrypto[sp][:0]
		}
		pkt, err := c.sealPacket(sp, frames)
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
func (c *Conn) sealPacket(sp int, frames []byte) ([]byte, error) {
	const minFrames = 20 // ensures payload+tag reaches the 16-byte HP sample
	if len(frames) < minFrames {
		frames = append(frames, make([]byte, minFrames-len(frames))...) // PADDING
	}
	pnLen := 4
	pn := c.sendPN[sp]
	c.sendPN[sp]++
	length := uint64(pnLen + len(frames) + 16)

	var hdr []byte
	var pnOffset int
	switch sp {
	case spaceInitial:
		hdr, pnOffset = AppendLongHeader(nil, PacketInitial, QUICVersion1, c.dcid, c.scid, nil, pnLen, length)
	case spaceHandshake:
		hdr, pnOffset = AppendLongHeader(nil, PacketHandshake, QUICVersion1, c.dcid, c.scid, nil, pnLen, length)
	default:
		hdr, pnOffset = AppendShortHeader(nil, c.dcid, pnLen, false)
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
}

func (h *connFrameHandler) OnCrypto(_ uint64, data []byte) error {
	// The handshake CRYPTO stream is delivered in order on a reliable path;
	// reassembly of out-of-order offsets is a later refinement.
	h.c.recvCrypto[h.space] = append(h.c.recvCrypto[h.space], data...)
	h.ackEliciting = true
	return nil
}

func (h *connFrameHandler) OnPing() error { h.ackEliciting = true; return nil }

func (h *connFrameHandler) OnStream(uint64, uint64, bool, []byte) error {
	h.ackEliciting = true
	return nil
}

func (h *connFrameHandler) OnHandshakeDone() error {
	h.c.handshakeComplete = true
	h.ackEliciting = true
	return nil
}

func (h *connFrameHandler) OnConnectionClose(bool, uint64, uint64, []byte) error {
	h.c.closed = true
	return nil
}
