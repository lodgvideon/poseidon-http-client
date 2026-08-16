package hpack

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RFC 7541 §5.1 examples.
func TestEncodeInteger_RFCExamples(t *testing.T) {
	cases := []struct {
		name       string
		i          uint64
		n          uint8
		prefixByte byte
		want       []byte
	}{
		// §C.1.1: encode 10 in 5-bit prefix, prefixByte = 0
		{"c_1_1__10__5bit", 10, 5, 0x00, []byte{0x0a}},
		// §C.1.2: encode 1337 in 5-bit prefix, prefixByte = 0
		{"c_1_2__1337__5bit", 1337, 5, 0x00, []byte{0x1f, 0x9a, 0x0a}},
		// §C.1.3: encode 42 in 8-bit prefix
		{"c_1_3__42__8bit", 42, 8, 0x00, []byte{0x2a}},
		// 2^N - 1 = 31 with N=5 → triggers continuation
		{"boundary_2N_minus_1", 31, 5, 0x00, []byte{0x1f, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i, n, prefixByte := tc.i, tc.n, tc.prefixByte

			got := EncodeInteger(nil, n, prefixByte, i)

			assert.Equalf(t, tc.want, got,
				"EncodeInteger(%d, n=%d, prefix=%#x) — §5.1 fixes this byte string, and the peer's decoder reads it literally; a value of exactly 2^N-1 must still spend a continuation octet or it decodes as an unfinished integer",
				i, n, prefixByte)
		})
	}
}

func TestDecodeInteger_RFCExamples(t *testing.T) {
	cases := []struct {
		name         string
		src          []byte
		n            uint8
		wantVal      uint64
		wantConsumed int
	}{
		{"c_1_1__10__5bit", []byte{0x0a}, 5, 10, 1},
		{"c_1_2__1337__5bit", []byte{0x1f, 0x9a, 0x0a}, 5, 1337, 3},
		{"c_1_3__42__8bit", []byte{0x2a}, 8, 42, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src, n := tc.src, tc.n

			got, consumed, err := DecodeInteger(src, n)

			require.NoErrorf(t, err, "DecodeInteger(%x, n=%d) on a §5.1 example", src, n)
			assert.Equalf(t, tc.wantVal, got, "DecodeInteger(%x, n=%d) value", src, n)
			assert.Equalf(t, tc.wantConsumed, consumed,
				"DecodeInteger(%x, n=%d) consumed count; a wrong count leaves the next representation to be parsed from the middle of this one", src, n)
		})
	}
}

func TestDecodeInteger_Truncated(t *testing.T) {
	cases := [][]byte{
		{},
		{0x1f},
		{0x1f, 0x80},
		{0x1f, 0xff},
		{0x1f, 0xff, 0xff},
	}
	for _, src := range cases {
		t.Run(fmt.Sprintf("%x", src), func(t *testing.T) {
			in := src

			_, _, err := DecodeInteger(in, 5)

			require.ErrorIsf(t, err, ErrTruncated,
				"DecodeInteger(%x): an integer whose continuation run runs off the end of the buffer is truncation, not a value to guess at", in)
		})
	}
}

func TestDecodeInteger_Overflow(t *testing.T) {
	src := []byte{0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00}

	_, _, err := DecodeInteger(src, 5)

	require.ErrorIs(t, err, ErrIntegerOverflow,
		"a §5.1 integer above 2^32-1 must be refused; letting it wrap hands the caller a length or index the peer never sent")
}

func TestEncodeDecodeInteger_RoundTrip(t *testing.T) {
	for _, n := range []uint8{1, 4, 5, 6, 7, 8} {
		for _, v := range []uint64{0, 1, 2, 30, 31, 100, 1000, 1 << 20, 1 << 31, 1<<32 - 1} {
			t.Run(fmt.Sprintf("n%d_v%d", n, v), func(t *testing.T) {
				prefixBits, value := n, v

				enc := EncodeInteger(nil, prefixBits, 0, value)
				dec, _, err := DecodeInteger(enc, prefixBits)

				require.NoErrorf(t, err, "decoding our own encoding of %d in an %d-bit prefix", value, prefixBits)
				assert.Equalf(t, value, dec,
					"%d in an %d-bit prefix encoded to %x and read back as %d; the prefix width changes where the continuation starts, so each width is its own encoding",
					value, prefixBits, enc, dec)
			})
		}
	}
}

func BenchmarkDecodeInteger_Max(b *testing.B) {
	src := []byte{0x1f, 0xff, 0xff, 0xff, 0xff, 0x0f}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeInteger(src, 5)
	}
}
