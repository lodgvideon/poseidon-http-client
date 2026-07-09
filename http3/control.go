package http3

import (
	"context"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// maxControlFrameLen bounds a frame the client will buffer on the server control
// stream. Control frames (SETTINGS, GOAWAY, MAX_PUSH_ID) are small; a larger
// declared length is treated as H3_EXCESSIVE_LOAD rather than buffered.
const maxControlFrameLen = 1 << 16

// poll drives the connection one step and then services the server control
// stream (RFC 9114 §6.2.1), so SETTINGS and GOAWAY are processed at the same
// cadence as request-stream reads. It is the single funnel for driving the
// connection; the engine is single-goroutine, so no locking is needed.
func (c *Client) poll(ctx context.Context) error {
	if err := c.conn.Poll(ctx); err != nil {
		return err
	}
	return c.serviceControl()
}

// serviceControl accepts newly arrived server-initiated unidirectional streams,
// identifies each by its leading stream-type varint, and reads the control
// stream. It does not block; it processes only bytes already received.
func (c *Client) serviceControl() error {
	for s := c.conn.AcceptUniStream(); s != nil; s = c.conn.AcceptUniStream() {
		c.pendingUni = append(c.pendingUni, &uniStream{stream: s})
	}
	kept := c.pendingUni[:0]
	for _, u := range c.pendingUni {
		u.buf = append(u.buf, u.stream.Recv()...)
		typ, n, err := ReadStreamType(u.buf)
		if err != nil {
			kept = append(kept, u) // ErrNeedMore: the type varint is not complete
			continue
		}
		if err := c.routeUni(typ, u.stream, u.buf[n:]); err != nil {
			c.pendingUni = kept
			return err
		}
	}
	c.pendingUni = kept
	return c.readControl()
}

// routeUni dispatches a server uni stream by its type (RFC 9114 §6.2). rest is
// the stream bytes already received after the type varint.
func (c *Client) routeUni(typ uint64, s quicStream, rest []byte) error {
	switch typ {
	case StreamTypeControl:
		if c.control != nil {
			// Only one control stream is permitted (§6.2.1).
			return c.controlError(H3StreamCreationError)
		}
		c.control = s
		c.controlReader.SetMaxFrameLen(maxControlFrameLen)
		c.controlReader.Feed(rest)
	case StreamTypeQPACKEncoder, StreamTypeQPACKDecoder:
		// We advertise a zero-capacity QPACK dynamic table, so these carry no
		// instructions; drop the stream (it is not re-queued and not read again).
	case StreamTypePush:
		// We never send MAX_PUSH_ID, so the server MUST NOT open a push stream
		// (§6.2.5, §7.2.5).
		return c.controlError(H3IDError)
	default:
		// An unknown stream type is aborted rather than buffered (§6.2, GREASE).
		_ = s.StopSending(H3StreamCreationError)
	}
	return nil
}

// readControl parses frames off the server control stream: the mandatory first
// SETTINGS (§6.2.1), then GOAWAY (§5.2). A rule violation closes the connection
// with the matching HTTP/3 error code.
func (c *Client) readControl() error {
	if c.control == nil {
		return nil
	}
	if data := c.control.Recv(); len(data) > 0 {
		c.controlReader.Feed(data)
	}
	for {
		typ, payload, err := c.controlReader.ReadFrame()
		if err == ErrH3FrameTooLarge {
			return c.controlError(H3ExcessiveLoad)
		}
		if err != nil {
			break // ErrNeedMore
		}
		if !c.settingsRead {
			if typ != FrameSettings {
				return c.controlError(H3MissingSettings) // §6.2.1: SETTINGS must be first
			}
			if _, perr := ParseSettings(payload); perr != nil {
				return c.controlError(H3SettingsErrorCode)
			}
			c.settingsRead = true
			continue
		}
		switch typ {
		case FrameSettings:
			return c.controlError(H3FrameUnexpected) // a second SETTINGS (§7.2.4)
		case FrameGoaway:
			id, n := bytesx.ReadVarint(payload)
			if n == 0 || n != len(payload) {
				return c.controlError(H3FrameError)
			}
			// A GOAWAY id MUST NOT be greater than any previously received
			// (RFC 9114 §5.2); a larger one is a connection error. An equal or
			// smaller id lowers (or re-confirms) the drain boundary.
			if c.haveGoaway && id > c.goawayID {
				return c.controlError(H3IDError)
			}
			c.goawayID, c.haveGoaway = id, true
		case FrameData, FrameHeaders, FramePushPromise, FrameMaxPushID, 0x02, 0x06, 0x08, 0x09:
			// These frames MUST NOT appear on the control stream (DATA §7.2.1,
			// HEADERS §7.2.2, PUSH_PROMISE §7.2.5, MAX_PUSH_ID at a client §7.2.7,
			// reserved HTTP/2-carryover types §7.2.8): H3_FRAME_UNEXPECTED.
			return c.controlError(H3FrameUnexpected)
		default:
			// GREASE (0x1f·N+0x21) and other genuinely-unknown types MUST be
			// ignored (§9); CANCEL_PUSH (0x03) is legal here and harmless to a
			// client that never enables push.
		}
	}
	return nil
}

// controlError sends a CONNECTION_CLOSE with a HTTP/3 error code for a control
// stream violation and returns ErrH3Control.
func (c *Client) controlError(code uint64) error {
	_ = c.conn.CloseWithError(true, code, "")
	return ErrH3Control
}
