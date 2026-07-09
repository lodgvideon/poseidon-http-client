package quic

import "errors"

// closeCodeFor maps a local protocol-violation error to the transport error code
// to signal in a CONNECTION_CLOSE (RFC 9000 §10.2, §20.1). The bool is false for
// errors that are not connection errors to signal — I/O failures, read timeouts,
// or a peer-initiated close.
func closeCodeFor(err error) (uint64, bool) {
	switch {
	case errors.Is(err, ErrFrameEncoding):
		return ErrCodeFrameEncodingError, true
	case errors.Is(err, ErrTransportParameter):
		return ErrCodeTransportParameterError, true
	case errors.Is(err, ErrFlowControl):
		return ErrCodeFlowControlError, true
	case errors.Is(err, ErrFinalSize):
		return ErrCodeFinalSizeError, true
	case errors.Is(err, ErrTooManyUniStreams), errors.Is(err, ErrServerBidiStream):
		return ErrCodeStreamLimitError, true
	case errors.Is(err, ErrAEADLimit):
		return ErrCodeAEADLimitReached, true
	default:
		return 0, false
	}
}

// fail handles a local error from the receive path: if it is a protocol violation
// (closeCodeFor), the connection sends a CONNECTION_CLOSE with the matching
// transport error code (RFC 9000 §10.2) so the peer learns why, then the error is
// returned unchanged. Non-protocol errors pass through without sending anything.
func (c *Conn) fail(err error) error {
	if code, ok := closeCodeFor(err); ok {
		_ = c.CloseWithError(false, code, "")
	}
	return err
}

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
