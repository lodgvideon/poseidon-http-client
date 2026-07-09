package quic

// CloseWithError terminates the connection by sending a single CONNECTION_CLOSE
// frame (RFC 9000 §10.2) and closing the underlying transport. app selects the
// application-error variant (0x1d) over the transport variant (0x1c); code is the
// error code and reason an optional diagnostic phrase. It is idempotent: a call
// after the connection is already closed (by us or by the peer) only closes the
// transport, without sending a second frame.
//
// Per RFC 9000 §10.2.3 an application close requested before 1-RTT keys exist
// cannot be sent as an application frame, so it is downgraded to a transport
// CONNECTION_CLOSE with APPLICATION_ERROR in the highest available space.
func (c *Conn) CloseWithError(app bool, code uint64, reason string) error {
	if c.closed {
		return c.pc.Close()
	}
	c.closed = true
	sp, sealer := c.closeSpace()
	if sealer != nil {
		if app && sp != spaceApp {
			app, code, reason = false, ErrCodeApplicationError, ""
		}
		frame := AppendConnectionClose(nil, app, code, 0, []byte(reason))
		// CONNECTION_CLOSE is not ack-eliciting (RFC 9000 §13.2.1); it is sent
		// once, not tracked for retransmission.
		if pkt, err := c.sealPacket(sp, frame, false, nil); err == nil {
			_, _ = c.pc.Write(pkt)
		}
	}
	return c.pc.Close()
}

// Close terminates the connection gracefully, sending a transport NO_ERROR
// CONNECTION_CLOSE (RFC 9000 §10.2) before closing the transport.
func (c *Conn) Close() error {
	return c.CloseWithError(false, ErrCodeNoError, "")
}

// closeSpace returns the highest packet-number space that has send keys and its
// Sealer, for emitting a CONNECTION_CLOSE (RFC 9000 §10.2.3). The Sealer is nil
// when no keys are available (nothing can be sent).
func (c *Conn) closeSpace() (int, *Sealer) {
	switch {
	case c.oneRTTSealer != nil:
		return spaceApp, c.oneRTTSealer
	case c.handshakeSealer != nil:
		return spaceHandshake, c.handshakeSealer
	default:
		return spaceInitial, c.initialSealer
	}
}
