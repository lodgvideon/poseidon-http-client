package bytesx

// QUIC variable-length integers (RFC 9000 §16).
//
// A QUIC varint stores its own length in the two most-significant bits of the
// first byte: prefix 00 → 1 byte (6-bit value, 0..63), 01 → 2 bytes (14-bit,
// 0..16383), 10 → 4 bytes (30-bit, 0..2^30-1), 11 → 8 bytes (62-bit,
// 0..2^62-1); the value occupies the remaining bits in network (big-endian)
// byte order. This is a DIFFERENT encoding from the HPACK/QPACK prefixed
// integer (RFC 7541 §5.1, see hpack/integer.go) — do not conflate the two.

import "encoding/binary"

// MaxVarint is the largest value a QUIC varint can encode (2^62 - 1).
const MaxVarint uint64 = (1 << 62) - 1

// VarintLen returns the number of bytes the minimal QUIC varint encoding of v
// uses (1, 2, 4, or 8). v MUST be <= MaxVarint — the caller owns that bound.
func VarintLen(v uint64) int {
	switch {
	case v <= 63:
		return 1
	case v <= 16383:
		return 2
	case v <= (1<<30)-1:
		return 4
	default:
		return 8
	}
}

// WriteVarint encodes v as a minimal-length QUIC varint into b and returns the
// number of bytes written (1, 2, 4, or 8). b MUST have length >= VarintLen(v)
// and v MUST be <= MaxVarint — the caller owns both bounds.
func WriteVarint(b []byte, v uint64) int {
	switch {
	case v <= 63:
		_ = b[0] // BCE hint
		b[0] = byte(v)
		return 1
	case v <= 16383:
		_ = b[1]
		b[0] = byte(v>>8) | 0x40
		b[1] = byte(v)
		return 2
	case v <= (1<<30)-1:
		_ = b[3]
		b[0] = byte(v>>24) | 0x80
		b[1] = byte(v >> 16)
		b[2] = byte(v >> 8)
		b[3] = byte(v)
		return 4
	default:
		_ = b[7]
		b[0] = byte(v>>56) | 0xc0
		b[1] = byte(v >> 48)
		b[2] = byte(v >> 40)
		b[3] = byte(v >> 32)
		b[4] = byte(v >> 24)
		b[5] = byte(v >> 16)
		b[6] = byte(v >> 8)
		b[7] = byte(v)
		return 8
	}
}

// ReadVarint decodes a QUIC varint from the front of b, returning the value and
// the number of bytes consumed (1, 2, 4, or 8). If b is empty or shorter than
// the length its first byte indicates, it returns (0, 0) to signal an
// incomplete input, so a streaming parser can retry once more bytes arrive.
//
// The decode is faithful to the on-wire length and does NOT reject non-minimal
// encodings (RFC 9000 §16 permits them). A caller whose field requires the
// minimal encoding compares n against VarintLen(v) itself.
//
// The multi-byte cases assemble the value with encoding/binary instead of a
// byte-at-a-time shift-or chain. The compiler lowers each BigEndian.UintNN to
// one wide load plus BSWAP, and clearing the two length bits from the assembled
// integer is exactly equivalent to clearing them from the first byte. That
// matters for more than instruction count: the shift-or form cost 169 against
// the inliner's budget of 80, so every decode was a real call, and QUIC and
// HTTP/3 decode several varints per packet. This form costs 74 and is inlined
// into its callers — TestReadVarintIsInlinable fails if an edit pushes it back
// over the budget.
func ReadVarint(b []byte) (v uint64, n int) {
	if len(b) == 0 {
		return 0, 0
	}
	switch b[0] >> 6 {
	case 0:
		return uint64(b[0]), 1
	case 1:
		if len(b) < 2 {
			return 0, 0
		}
		return uint64(binary.BigEndian.Uint16(b) & 0x3fff), 2
	case 2:
		if len(b) < 4 {
			return 0, 0
		}
		return uint64(binary.BigEndian.Uint32(b) & 0x3fff_ffff), 4
	default:
		if len(b) < 8 {
			return 0, 0
		}
		return binary.BigEndian.Uint64(b) & 0x3fff_ffff_ffff_ffff, 8
	}
}
