package hpack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeStringLiteral_Plain(t *testing.T) {
	src := []byte("/sample/path")

	dst := encodeStringLiteral(nil, src, false)

	require.NotEmpty(t, dst, "the literal must encode to something")
	assert.Equalf(t, byte(0x0c), dst[0],
		"prefix = %#x, want 0x0c (H=0, length 12); the length prefix is how the peer knows where this literal ends and the next representation begins", dst[0])
	assert.Equal(t, src, dst[1:], "an H=0 literal carries the bytes verbatim")
}

func TestEncodeStringLiteral_Huffman(t *testing.T) {
	src := []byte("no-cache")

	dst := encodeStringLiteral(nil, src, true)

	require.NotEmpty(t, dst, "the literal must encode to something")
	assert.NotZerof(t, dst[0]&0x80,
		"H bit not set: prefix = %#x; without it the peer reads the compressed bytes as the literal value", dst[0])
	assert.Equalf(t, byte(0x86), dst[0],
		"prefix = %#x, want 0x86 (H=1, length 6) — the declared length must be the HUFFMAN length, not the source length", dst[0])
}

func TestDecodeStringLiteral_Plain(t *testing.T) {
	src := append([]byte{0x0c}, []byte("/sample/path")...)
	dst := make([]byte, 0, 32)

	out, consumed, err := decodeStringLiteral(dst, src)

	require.NoError(t, err, "a well-formed H=0 literal must decode")
	assert.Equal(t, "/sample/path", string(out), "an H=0 literal decodes to its bytes verbatim")
	assert.Equal(t, 13, consumed,
		"consumed must cover the length prefix and the whole body; a short count leaves the literal's tail to be parsed as a new representation")
}

func TestDecodeStringLiteral_Huffman(t *testing.T) {
	src := []byte{0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf}

	out, consumed, err := decodeStringLiteral(nil, src)

	require.NoError(t, err, "a well-formed H=1 literal must decode")
	assert.Equal(t, "no-cache", string(out),
		"the H bit must select Huffman decoding; reading the body as raw octets hands the caller the compressed bytes as the header value")
	assert.Equal(t, len(src), consumed, "consumed must cover the length prefix and the whole Huffman body")
}

func TestDecodeStringLiteral_Truncated(t *testing.T) {
	// Declared length 5, but only 3 body bytes follow.
	src := []byte{0x05, 0x61, 0x62, 0x63}

	_, _, err := decodeStringLiteral(nil, src)

	require.ErrorIs(t, err, ErrTruncated,
		"a declared length reaching past the end of the buffer is truncation; slicing to it would read bytes the peer never sent")
}
