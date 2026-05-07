package hpack

// HuffmanEncodedLen returns the byte length of HuffmanEncode(_, src).
func HuffmanEncodedLen(src []byte) int {
	var bits uint64
	for _, b := range src {
		bits += uint64(huffmanCodes[b].nbits)
	}
	return int((bits + 7) / 8)
}

// HuffmanEncode appends the Huffman-coded form of src to dst (padded to a
// byte boundary with the EOS prefix per RFC 7541 §5.2) and returns the
// extended dst.
func HuffmanEncode(dst, src []byte) []byte {
	var (
		buf  uint64 // bit accumulator
		nbuf uint8  // bits currently in buf
	)
	for _, b := range src {
		c := huffmanCodes[b]
		buf = (buf << c.nbits) | uint64(c.code)
		nbuf += c.nbits
		for nbuf >= 8 {
			nbuf -= 8
			dst = append(dst, byte(buf>>nbuf))
		}
	}
	if nbuf > 0 {
		// Pad with the most-significant bits of EOS (all 1s). RFC §5.2.
		buf = (buf << (8 - nbuf)) | (1<<(8-nbuf) - 1)
		dst = append(dst, byte(buf))
	}
	return dst
}

// HuffmanDecode appends the decoded form of src to dst and returns the
// extended dst. Implements RFC 7541 §5.2 padding rules:
//   - Padding strictly longer than 7 bits is a decoding error.
//   - Non-EOS padding (any 0 bits in trailing partial byte) is a decoding error.
//   - The EOS symbol (256) MUST NOT appear in encoded data.
//
// Backed by a 4-bit FSM (huffmanDecodeFSM) built once at init from the
// canonical Huffman table; ~100x faster than the previous bit-by-bit
// linear scan.
func HuffmanDecode(dst, src []byte) ([]byte, error) {
	return huffmanDecodeFSM(dst, src)
}
