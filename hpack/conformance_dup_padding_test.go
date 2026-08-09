package hpack

// Two RFC 7541 decoder MUSTs that are correct by construction but were untested:
// duplicate dynamic-table entries are never an error (§2.3.2), and Huffman
// padding that is not the EOS most-significant bits is a decoding error (§5.2).

import (
	"errors"
	"testing"
)

// TestConformance_RFC7541_Sec2_3_2_DuplicateDynamicTableEntriesNotError pins
// §2.3.2: "duplicate entries MUST NOT be treated as an error by a decoder." Two
// "Literal Header Field with Incremental Indexing -- New Name" representations of
// the SAME field are decoded; both are inserted and neither is rejected.
func TestConformance_RFC7541_Sec2_3_2_DuplicateDynamicTableEntriesNotError(t *testing.T) {
	dec := NewDecoder()
	// §6.2.1: 0x40 = literal w/ incremental indexing, index 0 (new name);
	// then name "x-a" (len 3, not Huffman) and value "b" (len 1).
	one := []byte{0x40, 0x03, 'x', '-', 'a', 0x01, 'b'}
	block := append(append([]byte{}, one...), one...)

	var got []HeaderField
	err := dec.DecodeBlock(block, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:  append([]byte(nil), f.Name...),
			Value: append([]byte(nil), f.Value...),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("decode of duplicate entries: %v — §2.3.2: duplicate entries MUST NOT be treated as an error by a decoder", err)
	}
	if len(got) != 2 {
		t.Fatalf("decoded %d fields, want 2", len(got))
	}
	for i, f := range got {
		if string(f.Name) != "x-a" || string(f.Value) != "b" {
			t.Errorf("field %d = %q:%q, want x-a:b", i, f.Name, f.Value)
		}
	}
	if dec.dt.len() != 2 {
		t.Errorf("dynamic table has %d entries, want 2 (both duplicates inserted)", dec.dt.len())
	}
}

// TestConformance_RFC7541_Sec5_2_HuffmanPaddingNotEosMSBs_Error pins §5.2: "A
// padding not corresponding to the most significant bits of the code for the EOS
// symbol MUST be treated as a decoding error."
//
// 0x00 = the 5-bit code for '0' (00000) followed by a 3-bit all-zero partial
// "000". That trailing partial is a valid intermediate trie node (a prefix of
// many codes, so no invalid branch is hit) and is <= 7 bits (so the
// "strictly longer than 7 bits" rule does not fire) — it is rejected ONLY
// because the padding is not the all-ones EOS prefix. This isolates the
// EOS-MSB padding gate, which the existing 0xff.. / 4-byte fixtures do not.
func TestConformance_RFC7541_Sec5_2_HuffmanPaddingNotEosMSBs_Error(t *testing.T) {
	// Sanity: '0' really is the 5-bit code 0x00, so 0x00's leading 5 bits decode
	// cleanly and the failure is genuinely in the 3-bit trailing padding.
	if c := huffmanCodes['0']; c.nbits != 5 || c.code != 0x00 {
		t.Fatalf("huffmanCodes['0'] = {code:%#x nbits:%d}, want {0x0 5} — test premise broken", c.code, c.nbits)
	}
	_, err := HuffmanDecode(nil, []byte{0x00})
	if !errors.Is(err, ErrInvalidHuffman) {
		t.Fatalf("err = %v, want ErrInvalidHuffman — §5.2: a padding not corresponding to the "+
			"most significant bits of the code for the EOS symbol MUST be treated as a decoding error", err)
	}
}

// TestConformance_RFC7541_Sec5_2_PaddingLongerThanSevenBits_Error is the sibling
// of the test above. §5.2 states two padding rules, and that test deliberately
// isolates the first ("padding not corresponding to the EOS prefix") while
// staying under 7 bits so the second cannot fire. The second — "a padding
// strictly longer than 7 bits MUST be treated as a decoding error" — had no test
// at all: changing the decoder's limit from 7 to 8 left the entire suite green.
//
// 0xff is that rule in isolation. Eight 1-bits ARE the EOS prefix, so the first
// rule does not fire, and they emit no symbol — a whole byte carrying nothing
// but padding, which is precisely what the length rule exists to reject.
func TestConformance_RFC7541_Sec5_2_PaddingLongerThanSevenBits_Error(t *testing.T) {
	if _, err := HuffmanDecode(nil, []byte{0xff}); !errors.Is(err, ErrInvalidHuffman) {
		t.Fatalf("HuffmanDecode(0xff) err = %v, want ErrInvalidHuffman — §5.2: a padding "+
			"strictly longer than 7 bits MUST be treated as a decoding error", err)
	}

	// The boundary must still decode. A rule that also rejected exactly 7 bits
	// would turn well-formed peer output into an error, which is the worse
	// failure of the two. Symbol 199 is a 25-bit code, so its encoding occupies
	// 4 bytes and leaves exactly 7 pad bits — the longest padding that is legal.
	const sym = 199
	if c := huffmanCodes[sym]; c.nbits != 25 {
		t.Fatalf("huffmanCodes[%d].nbits = %d, want 25 — test premise broken", sym, c.nbits)
	}
	enc := HuffmanEncode(nil, []byte{sym})
	if len(enc)*8-25 != 7 {
		t.Fatalf("encoding of symbol %d is %d bytes, leaving %d pad bits, want 7",
			sym, len(enc), len(enc)*8-25)
	}
	got, err := HuffmanDecode(nil, enc)
	if err != nil || len(got) != 1 || got[0] != sym {
		t.Fatalf("HuffmanDecode(% x) = (% x, %v), want ([%02x], nil) — exactly 7 pad bits is legal",
			enc, got, err, sym)
	}
}
