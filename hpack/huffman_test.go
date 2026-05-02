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
