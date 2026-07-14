package http3

import (
	"errors"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// maxControlFrameLen bounds a frame the client will buffer on the server control
// stream. Control frames (SETTINGS, GOAWAY, MAX_PUSH_ID) are small; a larger
// declared length is treated as H3_EXCESSIVE_LOAD rather than buffered.
const maxControlFrameLen = 1 << 16

// serviceControl accepts newly arrived server-initiated unidirectional streams,
// identifies each by its leading stream-type varint, and reads the control
// stream. It does not block; it processes only bytes already received. It runs on
// the reader goroutine after each Poll — outside c.mu, safe because Poll never
// returns holding it (docs/HTTP3_DESIGN.md §3.1, §3.2 postcondition).
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
	if err := c.readControl(); err != nil {
		return err
	}
	if err := c.readQPACKEncoder(); err != nil {
		return err
	}
	if c.decHasResidue.Load() {
		c.flushQPACKDecoder() // flush any decoder-stream bytes a prior send left flow-control-blocked
	}
	return c.checkCriticalStreams()
}

// readQPACKEncoder applies the server's QPACK encoder-stream instructions (RFC
// 9204 §4.3) to the shared dynamic table and, when new entries were inserted,
// emits an Insert Count Increment on our decoder stream (§4.4.3). It is
// reader-owned: only this call, on the reader goroutine, writes the table, and it
// holds qpackMu.Lock only for the pure apply (no I/O, no wait, no c.mu), so qpackMu
// stays a leaf lock. A partial trailing instruction is retained in qpackEncBuf for
// the next service pass. A malformed instruction or a capacity violation is a
// QPACK_ENCODER_STREAM_ERROR connection error (§6). INERT: with capacity 0 the
// server sends no inserts, so this applies nothing and emits nothing.
func (c *Client) readQPACKEncoder() error {
	if c.qpackEnc == nil {
		return nil
	}
	if data := c.qpackEnc.Recv(); len(data) > 0 {
		c.qpackEncBuf = append(c.qpackEncBuf, data...)
	}
	if len(c.qpackEncBuf) == 0 {
		return nil
	}
	c.qpackMu.Lock()
	before := c.qpackDyn.InsertCount()
	n, perr := c.qpackDyn.ParseEncoderInstructions(c.qpackEncBuf)
	after := c.qpackDyn.InsertCount()
	c.insertCount.Store(after) // publish for lock-free observation (§3.5)
	c.qpackMu.Unlock()
	if n > 0 {
		c.qpackEncBuf = append(c.qpackEncBuf[:0], c.qpackEncBuf[n:]...) // drop applied bytes; keep the partial tail
	}
	if perr != nil {
		return c.connError(H3QpackEncoderStreamError)
	}
	if after > before {
		// Acknowledge the newly received inserts so the encoder can advance its Known
		// Received Count (RFC 9204 §4.4.3). qpackMu is already released, so the
		// decoder-stream Send does not run under the table lock.
		c.qpackDecWrite(qpack.AppendInsertCountIncrement(nil, after-before))
	}
	return nil
}

// checkCriticalStreams reports a connection error if the server has closed any
// critical stream — its control stream (RFC 9114 §6.2.1) or either QPACK stream
// (RFC 9204 §4.2). Those streams MUST stay open for the life of the connection;
// a FIN or a reset on any of them (both surface as Finished) is
// H3_CLOSED_CRITICAL_STREAM.
func (c *Client) checkCriticalStreams() error {
	for _, s := range []quicStream{c.control, c.qpackEnc, c.qpackDec} {
		if s != nil && s.Finished() {
			return c.connError(H3ClosedCriticalStream)
		}
	}
	return nil
}

// routeUni dispatches a server uni stream by its type (RFC 9114 §6.2). rest is
// the stream bytes already received after the type varint.
func (c *Client) routeUni(typ uint64, s quicStream, rest []byte) error {
	switch typ {
	case StreamTypeControl:
		if c.control != nil {
			// Only one control stream is permitted (§6.2.1).
			return c.connError(H3StreamCreationError)
		}
		c.control = s
		c.controlReader.SetMaxFrameLen(maxControlFrameLen)
		c.controlReader.Feed(rest)
	case StreamTypeQPACKEncoder:
		// The server's encoder stream (RFC 9204 §4.2, §4.3). It is a critical stream,
		// so its reference is retained to detect the server closing it, and its
		// instructions are applied to the shared dynamic table by readQPACKEncoder.
		// Any instruction bytes that arrived with the type varint are seeded here.
		// INERT: with capacity 0 the server sends no instructions.
		if c.qpackEnc != nil {
			return c.connError(H3StreamCreationError) // only one encoder stream (§4.2)
		}
		c.qpackEnc = s
		c.qpackEncBuf = append(c.qpackEncBuf, rest...)
	case StreamTypeQPACKDecoder:
		if c.qpackDec != nil {
			return c.connError(H3StreamCreationError) // only one decoder stream (§4.2)
		}
		c.qpackDec = s
	case StreamTypePush:
		// We never send MAX_PUSH_ID, so the server MUST NOT open a push stream
		// (§6.2.5, §7.2.5).
		return c.connError(H3IDError)
	default:
		// An unknown stream type is aborted rather than buffered (§6.2, GREASE).
		_ = s.StopSending(H3StreamCreationError)
	}
	return nil
}

// applyServerSettings records the server settings the client acts on. Only
// SETTINGS_MAX_FIELD_SECTION_SIZE (§4.2.2) currently bounds client behavior — it
// caps the size of a request field section; the QPACK settings stay at the
// static-table-only defaults (RFC 9204 §5). An absent value leaves the field at
// its no-limit default (§7.2.4.1).
func (c *Client) applyServerSettings(settings []Setting) {
	for _, s := range settings {
		if s.ID == SettingMaxFieldSectionSize {
			c.maxFieldSection.Store(s.Value) // reader writes; a Do reads (§3.5)
		}
	}
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
			return c.connError(H3ExcessiveLoad)
		}
		if err != nil {
			break // ErrNeedMore
		}
		if !c.settingsRead {
			if typ != FrameSettings {
				return c.connError(H3MissingSettings) // §6.2.1: SETTINGS must be first
			}
			settings, perr := ParseSettings(payload)
			if perr != nil {
				if errors.Is(perr, ErrH3Frame) {
					return c.connError(H3FrameError) // a setting cut off by the frame length (§7.1)
				}
				return c.connError(H3SettingsErrorCode)
			}
			c.applyServerSettings(settings)
			c.settingsRead = true
			continue
		}
		switch typ {
		case FrameSettings:
			return c.connError(H3FrameUnexpected) // a second SETTINGS (§7.2.4)
		case FrameGoaway:
			id, n := bytesx.ReadVarint(payload)
			if n == 0 || n != len(payload) {
				return c.connError(H3FrameError)
			}
			// A GOAWAY a client receives carries a client-initiated bidirectional
			// (request) stream id, whose low two bits are zero (RFC 9000 §2.1). A
			// stream id of any other type is a connection error (RFC 9114 §7.2.6).
			if id&0x3 != 0 {
				return c.connError(H3IDError)
			}
			// A GOAWAY id MUST NOT be greater than any previously received
			// (RFC 9114 §5.2); a larger one is a connection error. An equal or
			// smaller id lowers (or re-confirms) the drain boundary. goaway is
			// ^uint64(0) until the first GOAWAY (§3.5), so a first id always lands.
			if prev := c.goaway.Load(); prev != ^uint64(0) && id > prev {
				return c.connError(H3IDError)
			}
			c.goaway.Store(id) // reader writes; a Do reads (§3.5)
		case FrameCancelPush:
			// The client never sends MAX_PUSH_ID, so the maximum push ID is unset
			// and every push ID is out of range: a CANCEL_PUSH referencing any push
			// ID is a connection error (RFC 9114 §7.2.3, §4.6). A malformed payload
			// (not a single push-ID varint) is a frame error (§7.1).
			if _, n := bytesx.ReadVarint(payload); n == 0 || n != len(payload) {
				return c.connError(H3FrameError)
			}
			return c.connError(H3IDError)
		case FrameData, FrameHeaders, FramePushPromise, FrameMaxPushID, 0x02, 0x06, 0x08, 0x09:
			// These frames MUST NOT appear on the control stream (DATA §7.2.1,
			// HEADERS §7.2.2, PUSH_PROMISE §7.2.5, MAX_PUSH_ID at a client §7.2.7,
			// reserved HTTP/2-carryover types §7.2.8): H3_FRAME_UNEXPECTED.
			return c.connError(H3FrameUnexpected)
		default:
			// GREASE (0x1f·N+0x21) and other genuinely-unknown types MUST be
			// ignored (§9).
		}
	}
	return nil
}

// connError sends a CONNECTION_CLOSE with an HTTP/3 error code for a connection
// error detected while servicing a stream — a control-stream violation (§6.2.1,
// §5.2) or a frame that may not appear on a request stream (§7.2) — and returns
// ErrH3Control.
func (c *Client) connError(code uint64) error {
	_ = c.conn.CloseWithError(true, code, "")
	return ErrH3Control
}
