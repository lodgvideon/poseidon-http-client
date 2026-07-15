package hpack

import "testing"

// codeBits renders a table entry as its bit string, most-significant bit first.
// The table stores a code right-aligned in a uint32, so nbits — not the value's
// own magnitude — decides where it starts; an entry whose value is too small for
// its declared width silently gains leading zeros, which is exactly the corruption
// these tests exist to catch.
func codeBits(c huffmanCode) string {
	b := make([]byte, c.nbits)
	for i := range b {
		shift := uint(int(c.nbits) - 1 - i)
		b[i] = '0' + byte((c.code>>shift)&1)
	}
	return string(b)
}

// TestHuffmanTableIsPrefixFree proves the table is a valid Huffman code without
// consulting RFC 7541 at all: in any prefix code no codeword may be a prefix of
// another, or a decoder cannot tell where one symbol ends and the next begins.
//
// This is the check that matters. A mistyped entry stays plausible to the eye and
// round-trips fine for the 255 symbols it does not touch, so per-symbol example
// tests miss it — but it cannot hide from prefix-freeness, because shortening a
// codeword's value while leaving nbits alone prepends zeros and walks the code
// straight into the subtree of some short, common symbol.
func TestHuffmanTableIsPrefixFree(t *testing.T) {
	t.Parallel()
	codes := make([]string, len(huffmanCodes))
	for i := range huffmanCodes {
		codes[i] = codeBits(huffmanCodes[i])
	}
	for i, a := range codes {
		for j, b := range codes {
			if i == j || len(a) >= len(b) {
				continue
			}
			if b[:len(a)] == a {
				t.Errorf("sym %d (%s) is a prefix of sym %d (%s): the table is not a prefix code, "+
					"so sym %d cannot be decoded", i, a, j, b, j)
			}
		}
	}
}

// TestHuffmanTableWidths guards the table's shape: RFC 7541 Appendix B uses 5..30
// bits, and a code must fit the width it declares.
func TestHuffmanTableWidths(t *testing.T) {
	t.Parallel()
	for i, c := range huffmanCodes {
		if c.nbits < 5 || c.nbits > 30 {
			t.Errorf("sym %d: nbits = %d, outside the 5..30 range RFC 7541 Appendix B uses", i, c.nbits)
		}
		if c.nbits < 32 && c.code>>c.nbits != 0 {
			t.Errorf("sym %d: code %#x does not fit in its declared %d bits", i, c.code, c.nbits)
		}
	}
}

// TestHuffmanRoundTripsEverySSymbol encodes and decodes all 256 byte values, one
// at a time and then as one long string.
//
// Exhaustive rather than sampled, because the failure mode is silent: a corrupt
// entry returns a nil error and simply hands back different bytes. Byte 0xf9 —
// legal obs-text in a field value (RFC 9110 §5.5) — decoded as two bytes for
// exactly this reason, and no sampled test would have picked it.
func TestHuffmanRoundTripsEverySymbol(t *testing.T) {
	t.Parallel()
	for b := 0; b < 256; b++ {
		in := []byte{byte(b)}
		got, err := HuffmanDecode(nil, HuffmanEncode(nil, in))
		if err != nil {
			t.Errorf("sym %d (%#x): decode after encode: %v", b, b, err)
			continue
		}
		if len(got) != 1 || got[0] != byte(b) {
			t.Errorf("sym %d (%#x): round trip returned %#x, want %#x", b, b, got, in)
		}
	}

	// Every symbol in one string: catches a code whose bits are right in isolation
	// but misalign the ones that follow.
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	got, err := HuffmanDecode(nil, HuffmanEncode(nil, all))
	if err != nil {
		t.Fatalf("decoding all 256 symbols: %v", err)
	}
	if string(got) != string(all) {
		t.Errorf("round trip over all 256 symbols returned %d bytes, want 256", len(got))
	}
}

// TestHuffmanEncodedLenMatchesEncode keeps the length oracle honest: callers size
// buffers with HuffmanEncodedLen before encoding (qpack/encoder.go picks Huffman
// only when it predicts a saving), so a disagreement means a wrong length lands
// on the wire ahead of the bytes.
func TestHuffmanEncodedLenMatchesEncode(t *testing.T) {
	t.Parallel()
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	for b := 0; b < 256; b++ {
		in := []byte{byte(b)}
		if got, want := HuffmanEncodedLen(in), len(HuffmanEncode(nil, in)); got != want {
			t.Errorf("sym %d: HuffmanEncodedLen = %d, but HuffmanEncode produced %d bytes", b, got, want)
		}
	}
	if got, want := HuffmanEncodedLen(all), len(HuffmanEncode(nil, all)); got != want {
		t.Errorf("all 256 symbols: HuffmanEncodedLen = %d, but HuffmanEncode produced %d bytes", got, want)
	}
}
