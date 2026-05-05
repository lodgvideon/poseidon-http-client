package hpack

// EncodeInteger writes I as an N-bit prefix integer (RFC 7541 §5.1) and
// returns dst with the bytes appended. prefixByte's high (8-N) bits supply
// the representation prefix; its low N bits MUST be zero on entry.
func EncodeInteger(dst []byte, n uint8, prefixByte byte, i uint64) []byte {
	max := uint64(1)<<n - 1
	if i < max {
		return append(dst, prefixByte|byte(i))
	}
	dst = append(dst, prefixByte|byte(max))
	i -= max
	for i >= 128 {
		dst = append(dst, byte(i&0x7f|0x80))
		i >>= 7
	}
	return append(dst, byte(i))
}

// DecodeInteger reads an N-bit prefix integer from src starting at src[0].
// Returns the value, bytes consumed, and an error if truncated or overflowing.
// Caller MUST mask the prefix bits before calling: src[0] is interpreted as
// the encoded byte (the implementation only reads the low N bits of src[0]).
func DecodeInteger(src []byte, n uint8) (uint64, int, error) {
	if len(src) == 0 {
		return 0, 0, ErrTruncated
	}
	mask := byte(1)<<n - 1
	v := uint64(src[0] & mask)
	if v < uint64(mask) {
		return v, 1, nil
	}
	consumed := 1
	var m uint
	for {
		if consumed >= len(src) {
			return 0, 0, ErrTruncated
		}
		b := src[consumed]
		consumed++
		if m >= 32 {
			return 0, 0, ErrIntegerOverflow
		}
		v += uint64(b&0x7f) << m
		if v > 0xffff_ffff {
			return 0, 0, ErrIntegerOverflow
		}
		if b&0x80 == 0 {
			return v, consumed, nil
		}
		m += 7
	}
}
