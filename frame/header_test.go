package frame

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadFrameHeader_Sample(t *testing.T) {
	// length=10, type=0x1 (HEADERS), flags=0x05 (END_STREAM|END_HEADERS), stream=1
	raw := []byte{0x00, 0x00, 0x0a, 0x01, 0x05, 0x00, 0x00, 0x00, 0x01}

	h, err := ReadFrameHeader(raw)

	require.NoErrorf(t, err, "err: %v", err)
	assert.EqualValuesf(t, 10, h.Length, "got %+v", h)
	assert.Equalf(t, FrameHeaders, h.Type, "got %+v", h)
	assert.EqualValuesf(t, 0x05, h.Flags, "got %+v", h)
	assert.EqualValuesf(t, 1, h.StreamID, "got %+v", h)
}

func TestReadFrameHeader_RBitMasked(t *testing.T) {
	raw := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x01}

	h, err := ReadFrameHeader(raw)

	require.NoErrorf(t, err, "err: %v", err)
	assert.EqualValuesf(t, 1, h.StreamID, "StreamID = %d, want 1 (R-bit must be masked)", h.StreamID)
}

func TestReadFrameHeader_Short(t *testing.T) {
	short := []byte{0x00, 0x00}

	_, err := ReadFrameHeader(short)

	require.Error(t, err, "want error on short header")
}

func TestWriteFrameHeader(t *testing.T) {
	h := FrameHeader{Length: 0x1234, Type: FrameSettings, Flags: 0x01, StreamID: 0}
	var buf [9]byte

	WriteFrameHeader(buf[:], h)

	want := []byte{0x00, 0x12, 0x34, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00}
	require.Truef(t, bytes.Equal(buf[:], want), "got %x, want %x", buf, want)
}
