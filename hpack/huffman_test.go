package hpack

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// RFC 7541 §C.4.1: "www.example.com" Huffman-encoded.
func TestHuffmanEncode_C_4_1(t *testing.T) {
	src := []byte("www.example.com")
	wantHex := "f1e3c2e5f23a6ba0ab90f4ff"
	want, _ := hex.DecodeString(wantHex)
	dst := HuffmanEncode(nil, src)
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
	if got := HuffmanEncodedLen(src); got != len(want) {
		t.Fatalf("HuffmanEncodedLen = %d, want %d", got, len(want))
	}
}

// RFC 7541 §C.4.2: "no-cache" Huffman-encoded.
func TestHuffmanEncode_C_4_2(t *testing.T) {
	src := []byte("no-cache")
	wantHex := "a8eb10649cbf"
	want, _ := hex.DecodeString(wantHex)
	dst := HuffmanEncode(nil, src)
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

// RFC 7541 §C.4.3: "custom-key" Huffman-encoded.
func TestHuffmanEncode_C_4_3_Names(t *testing.T) {
	src := []byte("custom-key")
	wantHex := "25a849e95ba97d7f"
	want, _ := hex.DecodeString(wantHex)
	dst := HuffmanEncode(nil, src)
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

func TestHuffmanEncodedLen_Empty(t *testing.T) {
	if HuffmanEncodedLen(nil) != 0 || HuffmanEncodedLen([]byte{}) != 0 {
		t.Fatalf("empty len != 0")
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
	encHex := "f1e3c2e5f23a6ba0ab90f4ff"
	want := []byte("www.example.com")
	enc, _ := hex.DecodeString(encHex)
	dst, err := HuffmanDecode(make([]byte, 0, 32), enc)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %q, want %q", dst, want)
	}
}

func TestHuffmanDecode_C_4_2(t *testing.T) {
	enc, _ := hex.DecodeString("a8eb10649cbf")
	want := []byte("no-cache")
	dst, err := HuffmanDecode(nil, enc)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %q, want %q", dst, want)
	}
}

func TestHuffmanDecode_RoundTrip(t *testing.T) {
	for _, s := range []string{"", "a", "abc", "hello, world!", "/index.html?x=1&y=2"} {
		enc := HuffmanEncode(nil, []byte(s))
		dec, err := HuffmanDecode(nil, enc)
		if err != nil {
			t.Fatalf("s=%q err=%v", s, err)
		}
		if string(dec) != s {
			t.Fatalf("roundtrip s=%q got %q", s, dec)
		}
	}
}

func TestHuffmanDecode_TooLongPadding(t *testing.T) {
	// Padding strictly longer than 7 bits is a decode error (RFC 7541 §5.2).
	bad := []byte{0xff, 0xff, 0xff} // all 1s — too much padding to be valid
	_, err := HuffmanDecode(nil, bad)
	if err == nil {
		t.Fatalf("want error, got nil")
	}
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
