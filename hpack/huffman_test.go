package hpack

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RFC 7541 §C.4.1: "www.example.com" Huffman-encoded.
func TestHuffmanEncode_C_4_1(t *testing.T) {
	src := []byte("www.example.com")
	want, err := hex.DecodeString("f1e3c2e5f23a6ba0ab90f4ff")
	require.NoError(t, err, "hex decode of the expected wire bytes")

	dst := HuffmanEncode(nil, src)
	gotLen := HuffmanEncodedLen(src)

	assert.Equal(t, want, dst,
		"§C.4.1 fixes these bytes; a peer decodes what we emit against its own copy of Appendix B, so a differing encoding is a differing header")
	assert.Equal(t, len(want), gotLen,
		"HuffmanEncodedLen is the oracle callers size buffers with before encoding; disagreeing with HuffmanEncode puts a wrong length on the wire ahead of the bytes")
}

// RFC 7541 §C.4.2: "no-cache" Huffman-encoded.
func TestHuffmanEncode_C_4_2(t *testing.T) {
	src := []byte("no-cache")
	want, err := hex.DecodeString("a8eb10649cbf")
	require.NoError(t, err, "hex decode of the expected wire bytes")

	dst := HuffmanEncode(nil, src)

	assert.Equal(t, want, dst, "§C.4.2 fixes these bytes for a value that crosses a byte boundary mid-symbol")
}

// RFC 7541 §C.4.3: "custom-key" Huffman-encoded.
func TestHuffmanEncode_C_4_3_Names(t *testing.T) {
	src := []byte("custom-key")
	want, err := hex.DecodeString("25a849e95ba97d7f")
	require.NoError(t, err, "hex decode of the expected wire bytes")

	dst := HuffmanEncode(nil, src)

	assert.Equal(t, want, dst, "§C.4.3 fixes these bytes for a field NAME, which is coded exactly as a value is")
}

func TestHuffmanEncodedLen_Empty(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  []byte
	}{
		{"nil", nil},
		{"empty slice", []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := tc.src

			got := HuffmanEncodedLen(src)

			assert.Zero(t, got,
				"an empty string codes to zero bytes; reporting one pad byte would make every caller reserve room for a byte HuffmanEncode never writes")
		})
	}
}

func BenchmarkHuffmanEncode_path(b *testing.B) {
	src := []byte("/index.html")
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = HuffmanEncode(dst[:0], src)
	}
	_ = dst
}

func TestHuffmanDecode_C_4_1(t *testing.T) {
	enc, err := hex.DecodeString("f1e3c2e5f23a6ba0ab90f4ff")
	require.NoError(t, err, "hex decode of the fixture")

	dst, err := HuffmanDecode(make([]byte, 0, 32), enc)

	require.NoError(t, err, "§C.4.1 is well-formed Huffman and must decode")
	assert.Equal(t, "www.example.com", string(dst),
		"decoding the RFC's own bytes is the only external check that this FSM agrees with Appendix B")
}

func TestHuffmanDecode_C_4_2(t *testing.T) {
	enc, err := hex.DecodeString("a8eb10649cbf")
	require.NoError(t, err, "hex decode of the fixture")

	dst, err := HuffmanDecode(nil, enc)

	require.NoError(t, err, "§C.4.2 is well-formed Huffman and must decode")
	assert.Equal(t, "no-cache", string(dst), "decoding the RFC's own bytes for a value with symbols straddling byte boundaries")
}

func TestHuffmanDecode_RoundTrip(t *testing.T) {
	for _, s := range []string{"", "a", "abc", "hello, world!", "/index.html?x=1&y=2"} {
		t.Run(s, func(t *testing.T) {
			src := []byte(s)

			enc := HuffmanEncode(nil, src)
			dec, err := HuffmanDecode(nil, enc)

			require.NoErrorf(t, err, "decoding our own encoding of %q", s)
			assert.Equalf(t, s, string(dec),
				"round trip of %q through %x; encode and decode share one table, so a mismatch means the two directions read it differently", s, enc)
		})
	}
}

func TestHuffmanDecode_TooLongPadding(t *testing.T) {
	// Padding strictly longer than 7 bits is a decode error (RFC 7541 §5.2).
	bad := []byte{0xff, 0xff, 0xff} // all 1s — too much padding to be valid

	_, err := HuffmanDecode(nil, bad)

	require.ErrorIs(t, err, ErrInvalidHuffman,
		"§5.2: padding strictly longer than 7 bits is a decoding error, and a run of ones long enough to reach EOS must never be accepted as data")
}

func BenchmarkHuffmanDecode_path(b *testing.B) {
	enc, _ := hex.DecodeString("60d5e8b1d754df")
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = HuffmanDecode(dst[:0], enc)
	}
	_ = dst
}
