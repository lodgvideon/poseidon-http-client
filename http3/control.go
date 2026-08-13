package http3

import (
	"errors"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// maxControlFrameLen bounds a frame the client will buffer on the server control
// stream. Control frames (SETTINGS, GOAWAY, MAX_PUSH_ID) are small; a larger
// declared length is treated as H3_EXCESSIVE_LOAD rather than buffered.
const maxControlFrameLen = 1 << 16

// qpackInstrOverhead bounds the non-literal octets of one encoder instruction: a
// name index plus two literal length prefixes (RFC 9204 §4.3.2, §4.3.3). Each is a
// prefix integer of at most 6 octets — hpack's decoder rejects a 6th continuation
// octet as an overflow — so 18 is the true ceiling and 64 leaves room to spare.
const qpackInstrOverhead = 64

// qpackEncoderInstrCap returns the maximum octets of a single partially-arrived
// encoder-stream instruction the client will retain while the rest of it arrives,
// for a decoder advertising maxTableCapacity as SETTINGS_QPACK_MAX_TABLE_CAPACITY.
//
// The retained tail only ever holds ONE partial instruction — ParseEncoderInstructions
// applies and drops every whole one — so the cap is the largest instruction a
// conforming encoder can send. The largest is an insert, and RFC 9204 §3.2.2 forbids
// inserting an entry larger than the table capacity while §3.2.1 charges every entry
// name+value+32 octets, so an insertable entry's DECODED name+value is at most
// capacity-32. Its WIRE form is at most 4x that: a Huffman literal (§4.3.3) is
// normally smaller than its decoding, but the longest RFC 7541 Huffman code is 30
// bits, so an encoder that Huffman-codes bytes it should have sent raw could expand
// by just under 3.75x. 4*capacity + qpackInstrOverhead therefore admits every
// instruction any conforming encoder can send, and refuses everything else.
//
// maxTableCapacity is clamped first. It is the value WE advertise, but NewClient
// takes it from a caller, and a literal's wire length is itself bounded at 2^32-1 by
// the prefix-integer decoder — so a larger capacity cannot make a legitimate
// instruction any larger, it could only overflow the multiply.
func qpackEncoderInstrCap(maxTableCapacity uint64) uint64 {
	const clamp = 1 << 32
	if maxTableCapacity > clamp {
		maxTableCapacity = clamp
	}
	return 4*maxTableCapacity + qpackInstrOverhead
}

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
	if err := c.readQPACKDecoder(); err != nil {
		return err
	}
	if c.decHasResidue.Load() {
		c.flushQPACKDecoder() // flush any decoder-stream bytes a prior send left flow-control-blocked
	}
	if c.encHasResidue.Load() {
		c.flushQPACKEncoderStream() // flush any encoder-stream inserts a prior send left flow-control-blocked
	}
	return c.checkCriticalStreams()
}

// readQPACKEncoder applies the server's QPACK encoder-stream instructions (RFC
// 9204 §4.3) to the shared dynamic table and, when new entries were inserted,
// acknowledges them with an Insert Count Increment on our decoder stream (§4.4.3).
// It is reader-owned: only this call, on the reader goroutine, writes the table,
// and it holds qpackMu.Lock only for the pure apply (no I/O, no wait, no c.mu), so
// qpackMu stays a leaf lock. A partial trailing instruction is retained in
// qpackEncBuf for the next service pass. A malformed instruction or a capacity
// violation is a QPACK_ENCODER_STREAM_ERROR connection error (§6). Live: with a
// non-zero advertised capacity the server inserts entries here and we acknowledge
// them so it can advance its Known Received Count and reference them.
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
	// What survives the drop above is one partial instruction the server has declared
	// but not finished sending. Bounding it here — AFTER the parse, on the retained
	// tail rather than on the bytes received — is what lets a server legitimately
	// burst megabytes of whole instructions (all applied and dropped within this
	// pass) while refusing one that declares a length it never delivers. Without it a
	// server declaring a huge literal and dribbling grows qpackEncBuf forever.
	if uint64(len(c.qpackEncBuf)) > c.qpackEncBufMax {
		return c.connError(H3ExcessiveLoad)
	}
	if after > before {
		// Acknowledge the newly received inserts so the encoder can advance its Known
		// Received Count (RFC 9204 §4.4.3). qpackAckInserts increments only past the
		// current Known Received Count, so an insert a Section Acknowledgment already
		// covered is not counted twice. qpackMu is already released, so the
		// decoder-stream Send does not run under the table lock.
		c.qpackAckInserts(after)
		// Wake every Do parked waiting for the insert count to reach its section's
		// Required Insert Count (RFC 9204 §2.1.3, Q4). insertCount was already
		// published (under qpackMu.Lock above) before this broadcast, so a woken Do
		// re-reads the new value; a Do that checks between the store and this signal
		// sees the new count and never parks. qpackMu is released, so this does not
		// run under the table lock.
		c.signalQPACKReady()
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
		// Live: with a non-zero advertised capacity the server sends inserts here.
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
		// Seed any decoder-stream instruction bytes that arrived coalesced with
		// the stream-type varint, exactly as the encoder case does above. A
		// conformant server MAY pipeline its first Section Acknowledgment / Insert
		// Count Increment with the type byte in one STREAM frame; those bytes are
		// already off the stream (Recv will not return them again), so dropping
		// them here desyncs the decoder stream — the encoder's Known Received Count
		// never advances, or a later read misaligns on a partial instruction.
		c.qpackDecBuf = append(c.qpackDecBuf, rest...)
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

// applyServerSettings records the server settings the client acts on.
// SETTINGS_MAX_FIELD_SECTION_SIZE (§4.2.2) caps the size of a request field
// section. SETTINGS_QPACK_MAX_TABLE_CAPACITY (RFC 9204 §3.2.3) bounds the encode-
// side dynamic table WE maintain for the server's decoder (Q5): a non-zero value
// installs that table (enableEncoderDynamic), so repeated request headers are
// inserted and referenced; 0 (or absent) keeps the encoder static-only. The
// server's SETTINGS_QPACK_BLOCKED_STREAMS is not used because our conservative
// encoder never sends a section blocked on an unacknowledged insert. An absent
// value leaves each field at its default (§7.2.4.1).
func (c *Client) applyServerSettings(settings []Setting) {
	var serverQPACKMaxCap uint64
	for _, s := range settings {
		switch s.ID {
		case SettingMaxFieldSectionSize:
			c.maxFieldSection.Store(s.Value) // reader writes; a Do reads (§3.5)
		case SettingQPACKMaxTableCapacity:
			serverQPACKMaxCap = s.Value
		}
	}
	c.enableEncoderDynamic(serverQPACKMaxCap) // no-op when 0 / too small (§3.2.3)
}

// readQPACKDecoder applies the server's QPACK decoder-stream instructions (RFC
// 9204 §4.4) to the encode-side state: an Insert Count Increment advances the
// encoder's Known Received Count so those inserts become referenceable, and a
// Section Acknowledgment / Stream Cancellation releases outstanding references (a
// no-op in the eviction-free reference-after-ack profile). It mirrors
// readQPACKEncoder: reader-owned, holding encMu only for the pure apply (no I/O,
// no wait), so encMu stays a leaf lock. A partial trailing instruction is retained
// in qpackDecBuf. A malformed instruction, a zero increment, or an acknowledgment
// past the inserts we made is a QPACK_DECODER_STREAM_ERROR connection error (§6).
// Before the encoder is installed (pre-SETTINGS, or a server capacity of 0), the
// server has nothing to acknowledge, so any bytes are drained and discarded.
func (c *Client) readQPACKDecoder() error {
	if c.qpackDec == nil {
		return nil
	}
	if data := c.qpackDec.Recv(); len(data) > 0 {
		c.qpackDecBuf = append(c.qpackDecBuf, data...)
	}
	if len(c.qpackDecBuf) == 0 {
		return nil
	}
	c.encMu.Lock()
	enc := c.qpackEncoder.Load()
	if enc == nil {
		c.qpackDecBuf = c.qpackDecBuf[:0] // nothing to acknowledge yet; discard
		c.encMu.Unlock()
		return nil
	}
	n, perr := enc.ParseDecoderInstructions(c.qpackDecBuf)
	c.encMu.Unlock()
	if n > 0 {
		c.qpackDecBuf = append(c.qpackDecBuf[:0], c.qpackDecBuf[n:]...) // drop applied bytes; keep the partial tail
	}
	if perr != nil {
		return c.connError(H3QpackDecoderStreamError)
	}
	// No cap is needed on qpackDecBuf, unlike the encoder stream's (see
	// readQPACKEncoder). Every decoder instruction is a single prefix integer with no
	// length-prefixed literal to declare and withhold, so the retained tail is only
	// ever a partial integer — at most 5 octets, because a 6th continuation octet is
	// an overflow (a typed decoder-stream error) rather than a request for more bytes.
	// A cap here could never fire; TestConformance_RFC9204_Sec44_DecoderStreamTailIsSelfBounding
	// pins that, so a future length-carrying instruction on this stream fails loudly.
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
		if errors.Is(err, ErrH3FrameTooLarge) {
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
	//nolint:nilerr // The loop above breaks on ErrNeedMore, which is not a
	// failure: it means the buffered bytes hold no further complete frame.
	// Every genuine violation returned c.connError before reaching here.
	return nil
}

// connError sends a CONNECTION_CLOSE with an HTTP/3 error code for a connection
// error detected while servicing a stream — a control-stream violation (§6.2.1,
// §5.2) or a frame that may not appear on a request stream (§7.2) — and returns
// it as a *H3ConnError.
//
// The code used to go on the wire and nowhere else: every violation returned the
// bare ErrH3Control sentinel, so nothing above this layer could tell a peer's
// framing bug from a local QPACK failure. errors.Is(err, ErrH3Control) still
// matches, so only a direct == comparison had to change.
func (c *Client) connError(code uint64) error {
	_ = c.conn.CloseWithError(true, code, "")
	return &H3ConnError{Code: code}
}
