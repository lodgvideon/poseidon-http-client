package hpack

import (
	"bytes"
	"errors"
	"testing"
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
			got := EncodeInteger(nil, tc.n, tc.prefixByte, tc.i)
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("EncodeInteger(%d, %d, %#x) = %x, want %x", tc.i, tc.n, tc.prefixByte, got, tc.want)
			}
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
			got, n, err := DecodeInteger(tc.src, tc.n)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if got != tc.wantVal || n != tc.wantConsumed {
				t.Fatalf("DecodeInteger = (%d, %d), want (%d, %d)", got, n, tc.wantVal, tc.wantConsumed)
			}
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
		_, _, err := DecodeInteger(src, 5)
		if !errors.Is(err, ErrTruncated) {
			t.Fatalf("DecodeInteger(%x) err = %v, want ErrTruncated", src, err)
		}
	}
}

func TestDecodeInteger_Overflow(t *testing.T) {
	src := []byte{0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00}
	_, _, err := DecodeInteger(src, 5)
	if !errors.Is(err, ErrIntegerOverflow) {
		t.Fatalf("err = %v, want ErrIntegerOverflow", err)
	}
}

func TestEncodeDecodeInteger_RoundTrip(t *testing.T) {
	for _, n := range []uint8{1, 4, 5, 6, 7, 8} {
		for _, v := range []uint64{0, 1, 2, 30, 31, 100, 1000, 1 << 20, 1 << 31, 1<<32 - 1} {
			enc := EncodeInteger(nil, n, 0, v)
			dec, _, err := DecodeInteger(enc, n)
			if err != nil {
				t.Fatalf("v=%d n=%d: err=%v", v, n, err)
			}
			if dec != v {
				t.Fatalf("v=%d n=%d: dec=%d", v, n, dec)
			}
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
