package quic

import (
	"crypto/tls"
	"errors"
)

// closeCodeFor maps a local protocol-violation error to the transport error code
// to signal in a CONNECTION_CLOSE (RFC 9000 §10.2, §20.1). The bool is false for
// errors that are not connection errors to signal — I/O failures, read timeouts,
// or a peer-initiated close.
func closeCodeFor(err error) (uint64, bool) {
	// A TLS handshake failure surfaces as a tls.AlertError; RFC 9001 §4.8 turns it
	// into a CRYPTO_ERROR whose code is 0x0100 plus the one-byte alert description.
	var alert tls.AlertError
	if errors.As(err, &alert) {
		return ErrCodeCryptoBase + uint64(alert), true
	}
	switch {
	case errors.Is(err, ErrFrameEncoding):
		return ErrCodeFrameEncodingError, true
	case errors.Is(err, ErrTransportParameter):
		return ErrCodeTransportParameterError, true
	case errors.Is(err, ErrFlowControl):
		return ErrCodeFlowControlError, true
	case errors.Is(err, ErrFinalSize):
		return ErrCodeFinalSizeError, true
	case errors.Is(err, ErrCryptoBufferExceeded):
		return ErrCodeCryptoBufferExceeded, true
	case errors.Is(err, ErrTooManyUniStreams), errors.Is(err, ErrTooManyBidiStreams), errors.Is(err, ErrServerBidiStream):
		return ErrCodeStreamLimitError, true
	case errors.Is(err, ErrStreamState):
		return ErrCodeStreamStateError, true
	case errors.Is(err, ErrAEADLimit):
		return ErrCodeAEADLimitReached, true
	case errors.Is(err, ErrProtocolViolation):
		return ErrCodeProtocolViolation, true
	case errors.Is(err, ErrConnectionIDLimit):
		return ErrCodeConnectionIDLimitError, true
	default:
		return 0, false
	}
}

// fail handles a local error from the receive path: if it is a protocol violation
// (closeCodeFor), the connection sends a CONNECTION_CLOSE with the matching
// transport error code (RFC 9000 §10.2) so the peer learns why, then the error is
// returned unchanged. Non-protocol errors pass through without sending anything.
//
// Assumes c.mu is held: fail runs only from the receive path (Poll/Establish),
// so it calls closeWithErrorLocked directly rather than the re-locking public
// CloseWithError wrapper (which would self-deadlock on the single mutex).
func (c *Conn) fail(err error) error {
	if code, ok := closeCodeFor(err); ok {
		_ = c.closeWithErrorLocked(false, code, "")
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeWithErrorLocked(app, code, reason)
}

// closeWithErrorLocked is CloseWithError's body. Assumes c.mu is held. It is the
// reentrancy-safe target for the receive-path up-calls (sealPacket's AEAD-limit
// close and fail), which already hold c.mu and must not re-take it.
func (c *Conn) closeWithErrorLocked(app bool, code uint64, reason string) error {
	// Latch the single-close state (docs/HTTP3_DESIGN.md §3.3): record the
	// terminating error once and close c.done so any blocked WaitReadable /
	// WaitSendable wakes. Idempotent and first-error-wins. PR 2b wires the latch
	// here only; routing the other teardown paths (idleClose, statelessReset, the
	// AEAD-limit close, the reader's fatal) through it is PR 2c.
	c.terminateLocked(ErrConnClosed)
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
		if pkt, err := c.sealPacket(sp, frame, false, nil, false); err == nil {
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
