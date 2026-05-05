package hpack

// encodeStringLiteral appends the HPACK string-literal form of s to dst
// (RFC 7541 §5.2): {H?: 1 bit}{Length: 7-bit prefix int}{Length bytes}.
// huffman selects coding mode.
func encodeStringLiteral(dst, s []byte, huffman bool) []byte {
	if huffman {
		hlen := HuffmanEncodedLen(s)
		dst = EncodeInteger(dst, 7, 0x80, uint64(hlen))
		dst = HuffmanEncode(dst, s)
		return dst
	}
	dst = EncodeInteger(dst, 7, 0x00, uint64(len(s)))
	return append(dst, s...)
}

// decodeStringLiteral decodes a string literal starting at src[0] and
// appends it to dst, returning the extended dst and bytes consumed.
func decodeStringLiteral(dst, src []byte) ([]byte, int, error) {
	if len(src) < 1 {
		return nil, 0, ErrTruncated
	}
	huffman := src[0]&0x80 != 0
	length, n, err := DecodeInteger(src, 7)
	if err != nil {
		return nil, 0, err
	}
	if uint64(len(src)-n) < length {
		return nil, 0, ErrTruncated
	}
	body := src[n : n+int(length)]
	if huffman {
		dst, err = HuffmanDecode(dst, body)
		if err != nil {
			return nil, 0, err
		}
	} else {
		dst = append(dst, body...)
	}
	return dst, n + int(length), nil
}
