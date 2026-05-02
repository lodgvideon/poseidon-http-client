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
// Current implementation is a straightforward bit-by-bit walk over the
// encode table; a 4-bit FSM may be added later as a follow-up.
func HuffmanDecode(dst, src []byte) ([]byte, error) {
	var (
		buf  uint32 // bit accumulator (MSB-first, up to 24 valid bits)
		nbuf uint8  // bits in buf
	)

	for _, b := range src {
		buf = (buf << 8) | uint32(b)
		nbuf += 8
		for {
			match, msym, mn := huffmanLookup(buf, nbuf)
			if !match {
				break
			}
			if msym == 256 {
				return nil, ErrInvalidHuffman // EOS in stream
			}
			dst = append(dst, byte(msym))
			nbuf -= mn
			buf &= (1 << nbuf) - 1
		}
	}

	// Validate trailing padding (RFC §5.2): must be the prefix of EOS, ≤ 7 bits.
	if nbuf > 0 {
		if nbuf > 7 {
			return nil, ErrInvalidHuffman
		}
		expected := uint32(1<<nbuf - 1) // EOS prefix is all 1s
		if buf != expected {
			return nil, ErrInvalidHuffman
		}
	}
	return dst, nil
}

// huffmanLookup tries to decode one symbol from buf's MSB-aligned bits.
// Returns (matched, symbol, bits-consumed). Linear scan over codes.
func huffmanLookup(buf uint32, nbuf uint8) (bool, uint16, uint8) {
	for sym, c := range huffmanCodes {
		if c.nbits > nbuf {
			continue
		}
		shifted := buf >> (nbuf - c.nbits)
		if shifted == c.code {
			return true, uint16(sym), c.nbits
		}
	}
	return false, 0, 0
}
