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
