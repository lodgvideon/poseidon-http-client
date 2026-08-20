package frame

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The 31-bit PAYLOAD fields — the promised id in PUSH_PROMISE, the last-stream-id
// in GOAWAY, and the window increment in WINDOW_UPDATE — share their word with a
// reserved bit that RFC 7540 §4.1 requires a sender to leave clear. All three
// writers mask, through bufx.WriteUint31.
//
// A round trip cannot prove that. The READER masks too, so a writer emitting the
// reserved bit is invisible to any test that decodes its own output — which is
// why removing the mask from WriteGoAway broke nothing in the whole 81-test frame
// suite when it was tried. These read the byte instead (#517).
//
// PAYLOAD is the word that scopes this file, and it is load-bearing. A 31-bit id
// in a frame HEADER needs no test here: every header goes out through
// WriteFrameHeader, which masks the Stream Identifier itself, so a local mask at
// a call site is redundant on the wire and cannot be caught by reading the
// emitted byte. WriteAltSvc's `streamID &= 0x7fffffff` is exactly that case —
// measured byte-identical with and without it across every high-bit id — so what
// pins it is TestFramer_TraceOut_AltSvcStreamIDMatchesTheWire below, not a
// reserved-bit assertion that two mechanisms would satisfy (#779).

// TestWriter_ReservedBitStaysClear drives an id whose high bit is set — out of
// range for the field, and reachable because both parameters are public uint32.
func TestWriter_ReservedBitStaysClear(t *testing.T) {
	const (
		hdr        = 9          // frame header precedes the payload
		outOfRange = 0x80000009 // reserved bit set, id 9 underneath
	)

	t.Run("GOAWAY last-stream-id", func(t *testing.T) {
		fr, buf := newFramerWithBuffer()

		err := fr.WriteGoAway(outOfRange, ErrCodeNoError, nil)

		require.NoError(t, err, "WriteGoAway")
		b := buf.Bytes()[hdr]
		assert.Zerof(t, b&0x80,
			"first payload byte = %#02x: the reserved bit is set on the wire, "+
				"which RFC 7540 §4.1 forbids a sender to do", b)
	})

	t.Run("PUSH_PROMISE promised id", func(t *testing.T) {
		fr, buf := newFramerWithBuffer()

		err := fr.WritePushPromise(1, outOfRange, []byte{0x82}, true, 0)

		require.NoError(t, err, "WritePushPromise")
		b := buf.Bytes()[hdr]
		assert.Zerof(t, b&0x80, "first payload byte = %#02x: the reserved bit is set on the wire", b)
	})

	// The third payload mask, and the one whose own doc comment claims the
	// parity with the two above ("masked off exactly as the promised id and
	// last-stream-id are elsewhere in this file"). It was the only one of the
	// three with nothing asserting it: WriteWindowUpdate masks BEFORE the
	// zero check, so an increment of exactly 0x80000000 masks to 0 and is
	// refused, while 0x80000009 masks to 9 and is written — and writing the
	// caller's word unmasked instead puts the reserved bit on the wire with
	// the same nine on the receiving end, which no round trip can see (#779).
	t.Run("WINDOW_UPDATE increment", func(t *testing.T) {
		fr, buf := newFramerWithBuffer()

		err := fr.WriteWindowUpdate(1, outOfRange)

		require.NoError(t, err, "WriteWindowUpdate")
		b := buf.Bytes()[hdr]
		assert.Zerof(t, b&0x80,
			"first payload byte = %#02x: the reserved bit is set on the WINDOW_UPDATE "+
				"increment, which RFC 7540 §4.1 forbids a sender to do", b)
	})
}

// TestFramer_TraceOut_AltSvcStreamIDMatchesTheWire is what actually pins
// WriteAltSvc's `streamID &= 0x7fffffff`, and it is here rather than in the
// table above because the table cannot pin it.
//
// Issue #779 filed the line as an untested 31-bit mask. Measured, it is not one:
// with the mask deleted, the emitted octets are byte-identical for every id
// tried — 0, 1, 0x7fffffff, 0x80000000, 0x80000001, 0x80000009, 0xffffffff, with
// and without an entry — because WriteFrameHeader encodes every Stream
// Identifier through bufx.WriteUint31 and masks there. An assertion on the wire
// byte would pass under the deletion, satisfied by the wrong mechanism.
//
// One thing does change: the FrameHeader handed to the tracer still carries the
// caller's word, so a frame log would report 0x80000001 for a frame the peer
// sees on stream 1. That is the property the line buys, so that is what this
// asserts.
func TestFramer_TraceOut_AltSvcStreamIDMatchesTheWire(t *testing.T) {
	const outOfRangeID = 0x80000001 // reserved bit set, stream 1 underneath
	fr, buf := newFramerWithBuffer()
	rec := &recorder{}
	fr.SetTracer(rec)

	err := fr.WriteAltSvc(outOfRangeID, []AltSvcEntry{{Origin: "https://example.com", AltValue: `h2=":443"`}})

	require.NoError(t, err, "WriteAltSvc")
	got := rec.only(t)
	wire, herr := ReadFrameHeader(buf.Bytes()[:FrameHeaderSize])
	require.NoError(t, herr, "ReadFrameHeader over the emitted bytes")
	assert.EqualValuesf(t, wire.StreamID, got.StreamID,
		"traced stream id %#x but wrote %#x: a frame log that disagrees with the "+
			"wire about which stream a frame was for is worse than no log, because "+
			"it is the artefact someone debugging the connection trusts",
		got.StreamID, wire.StreamID)
	assert.EqualValuesf(t, 1, wire.StreamID,
		"wire stream id = %#x, want 1 — RFC 7540 §4.1 makes the high bit reserved", wire.StreamID)
}
