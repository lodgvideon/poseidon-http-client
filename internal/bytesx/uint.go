// Package bytesx provides private byte-level helpers: big-endian Uint24 /
// Uint31 read-write (HTTP/2 frame headers), the QUIC variable-length integer
// codec, a reusable byte-buffer pool, and QUIC padding stripping. Used by the
// frame, quic, and http3 packages. Not part of the public API.
package bytesx

// ReadUint24 reads a big-endian 24-bit unsigned integer from b[:3].
// b MUST have length >= 3 — caller is responsible for the bound.
func ReadUint24(b []byte) uint32 {
	_ = b[2] // BCE hint
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
}

// WriteUint24 writes the low 24 bits of v as big-endian into b[:3].
// b MUST have length >= 3 — caller is responsible for the bound.
func WriteUint24(b []byte, v uint32) {
	_ = b[2] // BCE hint
	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)
}

// ReadUint31 reads a 31-bit big-endian unsigned integer from b[:4],
// masking off the high R bit (RFC 7540 §4.1, §6.1, §6.6, §6.9).
func ReadUint31(b []byte) uint32 {
	_ = b[3]
	return (uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])) &^ 0x8000_0000
}

// WriteUint31 writes v as 31-bit big-endian into b[:4], clearing the high R bit.
func WriteUint31(b []byte, v uint32) {
	_ = b[3]
	v &^= 0x8000_0000
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
