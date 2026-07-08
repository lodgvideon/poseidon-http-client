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

// Poll reads one datagram and processes it — dispatching frames to open streams
// and sending acknowledgements — for driving receive after the handshake
// completes (RFC 9000 §13). The caller sets a read deadline on the PacketConn to
// bound the wait.
func (c *Conn) Poll() error {
	if c.pollBuf == nil {
		c.pollBuf = make([]byte, 2048)
	}
	n, err := c.pc.Read(c.pollBuf)
	if err != nil {
		return err
	}
	if err := c.recvDatagram(c.pollBuf[:n]); err != nil {
		return err
	}
	return c.flush()
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
		hasCrypto := len(c.pendingCrypto[sp]) > 0
		if hasCrypto {
			frames = AppendCrypto(frames, c.cryptoOffset[sp], c.pendingCrypto[sp])
			c.cryptoOffset[sp] += uint64(len(c.pendingCrypto[sp]))
			c.pendingCrypto[sp] = c.pendingCrypto[sp][:0]
		}
		// A packet carrying CRYPTO is ack-eliciting; an ACK-only packet is not.
		pkt, err := c.sealPacket(sp, frames, hasCrypto)
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
func (c *Conn) sealPacket(sp int, frames []byte, ackEliciting bool) ([]byte, error) {
	const minFrames = 20 // ensures payload+tag reaches the 16-byte HP sample
	if len(frames) < minFrames {
		frames = append(frames, make([]byte, minFrames-len(frames))...) // PADDING
	}
	pnLen := 4
	pn := c.sendPN[sp]
	c.sendPN[sp]++
	c.sent[sp].onSent(pn, c.clock(), ackEliciting)
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
	ackLow       uint64 // smallest packet number of the ACK range being decoded
}

// OnAck processes the first (largest) range of an ACK frame: it acknowledges
// [largest-firstRange, largest] and samples the RTT from the largest packet
// (RFC 9000 §19.3, RFC 9002 §5). ACK frames are not themselves ack-eliciting.
func (h *connFrameHandler) OnAck(largest, ackDelay, firstRange uint64) error {
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
	h.ackEliciting = true
	return nil
}

func (h *connFrameHandler) OnConnectionClose(bool, uint64, uint64, []byte) error {
	h.c.closed = true
	return nil
}
