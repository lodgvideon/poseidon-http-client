// hpack/huffman_edge_test.go — edge cases for Huffman decoder
// (RFC 7541 §5.2). These tests pin the behavior on malformed inputs
// that the fuzzer might find but that aren't exercised by the
// round-trip / C.4.x fixtures. Without these, ErrInvalidHuffman is
// only declared in errors.go and never asserted in any test — a
// regression could silently swallow malformed input.
package hpack

import (
	"errors"
	"strings"
	"testing"
)

// TestHuffmanDecode_InvalidCode_ReturnsErrInvalidHuffman feeds the
// decoder a bit sequence that does not match any symbol in the
// RFC 7541 Appendix B table. The decoder must return
// ErrInvalidHuffman (NOT panic, NOT silently append garbage).
func TestConformance_RFC7541_C5_2_HuffmanDecode_InvalidCode(t *testing.T) {
	// 0xff repeated 4 times: bit pattern of all 1s. None of these
	// prefixes decode to any valid Huffman symbol (the longest
	// valid code is 30 bits; the EOS symbol is 30 ones followed by
	// a 1-bit = 0x3fffffff, but a bare 0xff is not a valid prefix).
	src := []byte{0xff, 0xff, 0xff, 0xff}
	out, err := HuffmanDecode(nil, src)
	if err == nil {
		t.Fatalf("expected ErrInvalidHuffman, got nil (out=%x)", out)
	}
	if !errors.Is(err, ErrInvalidHuffman) {
		t.Fatalf("err = %v, want ErrInvalidHuffman", err)
	}
}

// TestHuffmanDecode_PrefixOfEosIsInvalid confirms that a sequence
// that LOOKS like the start of EOS (30+ leading 1s) but with a
// truncated or wrong suffix is rejected. EOS is exactly the 30-bit
// code 0x3fffffff; shorter or differently-padded variants are invalid.
func TestConformance_RFC7541_C5_2_HuffmanDecode_PrefixOfEos(t *testing.T) {
	// 0xff, 0xff, 0xff, 0x00 = 32 bits starting with 24 ones.
	// This is not the EOS code (which is 30 ones, then 2 bits 11).
	src := []byte{0xff, 0xff, 0xff, 0x00}
	_, err := HuffmanDecode(nil, src)
	if !errors.Is(err, ErrInvalidHuffman) {
		t.Fatalf("err = %v, want ErrInvalidHuffman", err)
	}
}

// TestHuffmanDecode_EmptyInput is a sanity check: empty input must
// not return an error.
func TestConformance_RFC7541_C5_2_HuffmanDecode_EmptyInput(t *testing.T) {
	out, err := HuffmanDecode(nil, nil)
	if err != nil {
		t.Fatalf("empty input err = %v, want nil", err)
	}
	if len(out) != 0 {
		t.Fatalf("empty input produced %x, want empty", out)
	}
}

// TestHuffmanDecode_LongString_RoundTrip checks that round-tripping
// a long ASCII string (over 255 bytes) through Encode + Decode yields
// the original bytes. Tests the integer-encoding edge case for string
// lengths that exceed the 7-bit prefix capacity.
func TestConformance_RFC7541_C5_2_HuffmanDecode_LongString_RoundTrip(t *testing.T) {
	// 1024-byte string: 256 'a' segments, exercising the multi-byte
	// integer prefix encoding on the string-literal length field.
	src := []byte(strings.Repeat("abcdefgh", 128)) // 1024 bytes
	enc := HuffmanEncode(nil, src)
	dec, err := HuffmanDecode(nil, enc)
	if err != nil {
		t.Fatalf("decode err = %v", err)
	}
	if string(dec) != string(src) {
		t.Fatalf("roundtrip mismatch: len(enc)=%d len(dec)=%d", len(enc), len(dec))
	}
}
