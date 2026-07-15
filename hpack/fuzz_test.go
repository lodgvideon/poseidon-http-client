package hpack

import (
	"encoding/hex"
	"testing"
)

// FuzzHPACKDecode feeds arbitrary bytes to the HPACK block decoder (RFC 7541
// §6). A header block is fully peer-controlled, and HPACK errors are fatal to the
// whole connection (COMPRESSION_ERROR), so the decoder must reject anything
// malformed without panicking.
//
// Beyond that it asserts a round trip: every field list the decoder accepts must
// survive a re-encode and re-decode unchanged. Header compression is only useful
// if it is lossless, and a codec that quietly returns different bytes than it was
// given is worse than one that fails — a mutated header value that survives the
// decoder is a request-smuggling shape.
//
// The round trip does NOT cover Huffman coding, and cannot: this encoder always
// emits raw literals (encoder.go passes huffman=false), so re-encoding never
// reaches the table even when the decoded value would compress. Huffman is
// covered by FuzzHuffmanRoundTrip and huffman_table_test.go instead.
//
// Seed corpus: the RFC 7541 Appendix C vectors.
func FuzzHPACKDecode(f *testing.F) {
	for _, hexStr := range []string{
		"82",                                                   // C.2.4
		"400a637573746f6d2d6b65790d637573746f6d2d686561646572", // C.2.1
		"040c2f73616d706c652f70617468",                         // C.2.2
		"100870617373776f726406736563726574",                   // C.2.3
		"828684410f7777772e6578616d706c652e636f6d",             // C.3.1
		"828684418cf1e3c2e5f23a6ba0ab90f4ff",                   // C.4.1 (Huffman)
	} {
		seed, _ := hex.DecodeString(hexStr)
		f.Add(seed)
	}
	// A Huffman-coded literal without indexing (§6.2.2) holding byte 0xf9 — legal
	// obs-text (RFC 9110 §5.5). Peers do Huffman-code such values, so this seeds
	// the one direction of the table this package reaches from a header block:
	// decoding what someone else encoded.
	huffLiteral := []byte{0x00}
	huffLiteral = encodeStringLiteral(huffLiteral, []byte("x-obs"), true)
	huffLiteral = encodeStringLiteral(huffLiteral, []byte("aaaaaaaaaa\xf9aaaaaaaaaa"), true)
	f.Add(huffLiteral)

	f.Fuzz(func(t *testing.T, data []byte) {
		var fields []HeaderField
		err := NewDecoder().DecodeBlock(data, func(hf HeaderField) error {
			// Name and Value alias the decoder's scratch buffer, which it reuses for
			// the next field, so they must be copied to outlive the callback.
			fields = append(fields, HeaderField{
				Name:  append([]byte(nil), hf.Name...),
				Value: append([]byte(nil), hf.Value...),
			})
			return nil
		})
		if err != nil {
			return // rejecting malformed input is the decoder working correctly
		}

		again := make([]HeaderField, 0, len(fields))
		err = NewDecoder().DecodeBlock(NewEncoder().EncodeBlock(nil, fields), func(hf HeaderField) error {
			again = append(again, HeaderField{
				Name:  append([]byte(nil), hf.Name...),
				Value: append([]byte(nil), hf.Value...),
			})
			return nil
		})
		if err != nil {
			t.Fatalf("re-decoding a block built from accepted fields: %v", err)
		}
		if len(again) != len(fields) {
			t.Fatalf("round trip returned %d fields, want %d", len(again), len(fields))
		}
		for i := range fields {
			if string(again[i].Name) != string(fields[i].Name) ||
				string(again[i].Value) != string(fields[i].Value) {
				t.Fatalf("field %d changed across a round trip: %q: %q -> %q: %q",
					i, fields[i].Name, fields[i].Value, again[i].Name, again[i].Value)
			}
		}
	})
}

// FuzzHuffmanRoundTrip asserts the one property the Huffman coder owes its
// callers: HuffmanDecode(HuffmanEncode(s)) == s, for any s (RFC 7541 §5.2,
// Appendix B).
//
// This is the target that would have caught the symbol-249 bug, and none of the
// others could have. The HPACK block round trip cannot reach it, because this
// package's encoder only ever emits raw literals; the QPACK one found it only
// because QPACK's encoder does choose Huffman. Coding is also where a table
// defect stays invisible: it returns a nil error and simply hands back different
// bytes, so nothing short of comparing them notices.
//
// A failure here means one of: the table is not prefix-free, a code does not fit
// its declared width, or the FSM disagrees with the table it is built from.
// huffman_table_test.go pins those properties directly; this searches for the
// inputs that expose them.
func FuzzHuffmanRoundTrip(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("www.example.com"))
	f.Add([]byte("no-cache"))
	f.Add([]byte("custom-key"))
	f.Add([]byte{0xf9})                            // sym 249, alone: raw is shorter, so nobody would code it
	f.Add([]byte("aaaaaaaaaa\xf9aaaaaaaaaa"))      // sym 249 amid compressible ASCII: Huffman wins, and it broke
	f.Add([]byte{0x00, 0x01, 0x02, 0xfd, 0xfe, 0xff})
	f.Add([]byte{0x0a, 0x0d, 0x16, 0x27})          // symbols with 30-bit codes, the table's longest

	f.Fuzz(func(t *testing.T, src []byte) {
		encoded := HuffmanEncode(nil, src)
		if got, want := HuffmanEncodedLen(src), len(encoded); got != want {
			t.Fatalf("HuffmanEncodedLen said %d bytes, HuffmanEncode produced %d", got, want)
		}
		decoded, err := HuffmanDecode(nil, encoded)
		if err != nil {
			t.Fatalf("decoding our own encoding of %q (%x): %v", src, encoded, err)
		}
		if string(decoded) != string(src) {
			t.Fatalf("round trip turned %q into %q (encoded %x)", src, decoded, encoded)
		}
	})
}

// FuzzDecodeInteger feeds arbitrary bytes to the HPACK integer decoder
// (RFC 7541 §5.1): an N-bit prefix, then 7 bits per continuation byte. Every
// length and index in a header block goes through it, so it is the first thing a
// hostile block reaches — and unbounded continuation bytes are the classic way to
// overflow such a decoder.
//
// The QUIC varint decoder has had a target since the start; this one only ever
// got reached transitively through FuzzHPACKDecode, which left it at 23.8%
// covered by the seed corpus.
//
// Contract: never panics; consumed stays within src; an accepted value fits the
// 2^32-1 bound §5.1 sets; and decode(encode(v)) == v, since the pair must agree
// on the encoding they share.
func FuzzDecodeInteger(f *testing.F) {
	f.Add([]byte{0x0a}, uint8(5))                   // C.1.1: 10 in a 5-bit prefix
	f.Add([]byte{0x1f, 0x9a, 0x0a}, uint8(5))       // C.1.2: 1337, multi-byte
	f.Add([]byte{0x2a}, uint8(8))                   // C.1.3: 42 in an 8-bit prefix
	f.Add([]byte{0x1f}, uint8(5))                   // prefix full, continuation missing
	f.Add([]byte{}, uint8(5))                       // empty
	f.Add([]byte{0xff, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, uint8(8)) // continuation that never ends
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}, uint8(8))                         // over the 2^32-1 bound
	f.Add([]byte{0x1f, 0x80, 0x80, 0x80, 0x80, 0x00}, uint8(5))                         // padded to overflow m

	f.Fuzz(func(t *testing.T, src []byte, nbits uint8) {
		// RFC 7541 uses 5-, 6- and 7-bit prefixes; the decoder's contract spans
		// 1..8. Fuzzing outside that range would test a case no caller can produce.
		n := nbits%8 + 1

		v, consumed, err := DecodeInteger(src, n)
		if err != nil {
			return
		}
		if consumed < 1 || consumed > len(src) {
			t.Fatalf("consumed %d bytes of a %d-byte input", consumed, len(src))
		}
		if v > 0xffff_ffff {
			t.Fatalf("accepted %d, above the 2^32-1 bound RFC 7541 §5.1 sets", v)
		}

		// The encoder is the decoder's only counterparty: whatever one accepts the
		// other must reproduce, or we emit lengths and indices we cannot read back.
		re := EncodeInteger(nil, n, 0x00, v)
		v2, consumed2, err := DecodeInteger(re, n)
		if err != nil {
			t.Fatalf("decoding EncodeInteger(%d, n=%d) = %x: %v", v, n, re, err)
		}
		if v2 != v {
			t.Fatalf("round trip with n=%d turned %d into %d (encoded %x)", n, v, v2, re)
		}
		if consumed2 != len(re) {
			t.Fatalf("round trip with n=%d consumed %d of %d encoded bytes", n, consumed2, len(re))
		}
	})
}
