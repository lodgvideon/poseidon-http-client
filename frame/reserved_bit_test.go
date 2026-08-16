package frame

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The 31-bit id fields — the promised id in PUSH_PROMISE and the last-stream-id
// in GOAWAY — share their word with a reserved bit that RFC 7540 §4.1 requires a
// sender to leave clear. Both writers mask, now through bufx.WriteUint31.
//
// A round trip cannot prove that. The READER masks too, so a writer emitting the
// reserved bit is invisible to any test that decodes its own output — which is
// why removing the mask from WriteGoAway broke nothing in the whole 81-test frame
// suite when it was tried. These read the byte instead (#517).

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
}
