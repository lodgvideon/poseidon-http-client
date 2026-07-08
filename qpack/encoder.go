package qpack

import "github.com/lodgvideon/poseidon-http-client/hpack"

// Encoder compresses HTTP/3 field sections using the static table plus literals
// (RFC 9204 §4.5). It never inserts into a dynamic table, so every section it
// produces has Required Insert Count 0 and is immediately decodable — it never
// causes head-of-line blocking, and it emits nothing on the QPACK encoder
// stream. Not safe for concurrent use.
type Encoder struct{}

// NewEncoder returns a static-table-only QPACK encoder.
func NewEncoder() *Encoder { return &Encoder{} }

// EncodeFieldSection appends the QPACK encoding of fields to dst and returns the
// extended slice. It begins with the Encoded Field Section Prefix (RFC 9204
// §4.5.1), which for this static-only profile is always the two bytes 0x00 0x00
// (Required Insert Count = 0, Base = 0). Field names MUST already be lowercase
// (RFC 9114 §4.2). Allocates nothing beyond growing dst.
func (e *Encoder) EncodeFieldSection(dst []byte, fields []hpack.HeaderField) []byte {
	dst = append(dst, 0x00, 0x00)
	for i := range fields {
		name, value := fields[i].Name, fields[i].Value
		idx, exact, nameMatch := lookupStatic(name, value)
		switch {
		case exact:
			// Indexed Field Line, static table (1Txxxxxx, T=1): §4.5.2.
			dst = hpack.EncodeInteger(dst, 6, 0xc0, uint64(idx))
		case nameMatch:
			// Literal Field Line with Name Reference, static (01NTxxxx,
			// N=0, T=1): §4.5.4.
			dst = hpack.EncodeInteger(dst, 4, 0x50, uint64(idx))
			dst = encodeStringLiteral(dst, value, 7, 0x00)
		default:
			// Literal Field Line with Literal Name (001NHxxx, N=0): §4.5.6.
			dst = encodeLiteralName(dst, name)
			dst = encodeStringLiteral(dst, value, 7, 0x00)
		}
	}
	return dst
}

// encodeLiteralName writes the name field of a Literal-with-Literal-Name
// representation: a 3-bit-prefix string with the 001 prefix, N=0, and the H bit
// at bit 3.
func encodeLiteralName(dst, name []byte) []byte {
	if hlen := hpack.HuffmanEncodedLen(name); hlen < len(name) {
		dst = hpack.EncodeInteger(dst, 3, 0x28, uint64(hlen)) // 001 N=0 H=1
		return hpack.HuffmanEncode(dst, name)
	}
	dst = hpack.EncodeInteger(dst, 3, 0x20, uint64(len(name))) // 001 N=0 H=0
	return append(dst, name...)
}

// encodeStringLiteral writes s as a QPACK string literal with a prefixBits-bit
// length prefix (RFC 9204 §4.5, §5.2 of RFC 7541 reused). baseByte supplies the
// representation bits above the H bit; the H bit is the bit immediately above
// the length prefix. Huffman coding is used when it is strictly shorter.
func encodeStringLiteral(dst, s []byte, prefixBits uint8, baseByte byte) []byte {
	hBit := byte(1) << prefixBits
	if hlen := hpack.HuffmanEncodedLen(s); hlen < len(s) {
		dst = hpack.EncodeInteger(dst, prefixBits, baseByte|hBit, uint64(hlen))
		return hpack.HuffmanEncode(dst, s)
	}
	dst = hpack.EncodeInteger(dst, prefixBits, baseByte, uint64(len(s)))
	return append(dst, s...)
}
