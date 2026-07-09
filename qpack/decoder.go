package qpack

import "github.com/lodgvideon/poseidon-http-client/hpack"

// Decoder reads QPACK-encoded field sections (RFC 9204 §4.5) that reference the
// static table or use literals — the static-only profile. Because the client
// advertises SETTINGS_QPACK_MAX_TABLE_CAPACITY=0, a conformant peer never
// references the dynamic table, so any dynamic reference or a Required Insert
// Count > 0 is rejected as ErrDecompressionFailed. Not safe for concurrent use.
type Decoder struct {
	nameScratch []byte // reused Huffman-decode buffer for a literal name
	valScratch  []byte // reused Huffman-decode buffer for a literal value
}

// NewDecoder returns a static-only QPACK decoder.
func NewDecoder() *Decoder { return &Decoder{} }

// DecodeFieldSection parses the QPACK field section in src and calls emit once
// per header field, in order. The name and value slices passed to emit are
// valid only until emit returns (they alias src or the Decoder's reused
// scratch); copy to retain. It returns ErrDecompressionFailed on malformed or
// unsupported (dynamic-table) input. Allocates nothing beyond growing the
// reused scratch on the first Huffman-coded literal.
func (d *Decoder) DecodeFieldSection(src []byte, emit func(name, value []byte) error) error {
	p, err := parsePrefix(src)
	if err != nil {
		return err
	}
	for p < len(src) {
		b := src[p]
		switch {
		case b&0x80 != 0:
			// Indexed Field Line (1Txxxxxx): §4.5.2.
			if b&0x40 == 0 {
				return ErrDecompressionFailed // T=0: dynamic table, unsupported
			}
			idx, n, ierr := hpack.DecodeInteger(src[p:], 6)
			if ierr != nil || idx >= uint64(len(staticTable)) {
				return ErrDecompressionFailed
			}
			p += n
			if err := emit(staticNameB[idx], staticValueB[idx]); err != nil {
				return err
			}
		case b&0x40 != 0:
			// Literal Field Line with Name Reference (01NTxxxx): §4.5.4.
			if b&0x10 == 0 {
				return ErrDecompressionFailed // T=0: dynamic table, unsupported
			}
			idx, n, ierr := hpack.DecodeInteger(src[p:], 4)
			if ierr != nil || idx >= uint64(len(staticTable)) {
				return ErrDecompressionFailed
			}
			p += n
			val, np, verr := readString(&d.valScratch, src, p, 7)
			if verr != nil {
				return verr
			}
			p = np
			if err := emit(staticNameB[idx], val); err != nil {
				return err
			}
		case b&0x20 != 0:
			// Literal Field Line with Literal Name (001NHxxx): §4.5.6.
			name, np, nerr := readString(&d.nameScratch, src, p, 3)
			if nerr != nil {
				return nerr
			}
			p = np
			val, np2, verr := readString(&d.valScratch, src, p, 7)
			if verr != nil {
				return verr
			}
			p = np2
			if err := emit(name, val); err != nil {
				return err
			}
		default:
			// 0001xxxx Indexed with Post-Base, or 0000Nxxx Literal with
			// Post-Base Name Reference — both reference the dynamic table.
			return ErrDecompressionFailed
		}
	}
	return nil
}

// parsePrefix parses the Encoded Field Section Prefix (RFC 9204 §4.5.1) and
// returns the offset of the first field line. For the static-only profile the
// Required Insert Count MUST be 0 (a non-zero value references the dynamic
// table, which the client's capacity-0 setting forbids); the Base is parsed but
// unused (it only resolves dynamic references).
func parsePrefix(src []byte) (int, error) {
	ric, n, err := hpack.DecodeInteger(src, 8)
	if err != nil {
		return 0, ErrDecompressionFailed
	}
	if ric != 0 {
		return 0, ErrDecompressionFailed
	}
	p := n
	if p >= len(src) {
		return 0, ErrDecompressionFailed // the Base byte must be present
	}
	// The Delta Base byte's bit 7 is the Sign. With the Required Insert Count
	// forced to 0, Base = RIC - DeltaBase - 1 is always negative for Sign=1 (RIC ≤
	// Delta Base), which the decoder MUST reject (RFC 9204 §4.5.1.2).
	if src[p]&0x80 != 0 {
		return 0, ErrDecompressionFailed
	}
	_, m, err := hpack.DecodeInteger(src[p:], 7)
	if err != nil {
		return 0, ErrDecompressionFailed
	}
	return p + m, nil
}

// readString decodes a QPACK string literal at src[p:] whose H bit is at
// position prefixBits and whose length uses a prefixBits-bit prefix integer. A
// raw literal is returned as a sub-slice of src (no copy); a Huffman-coded
// literal is decoded into *scratch (reused, grown as needed). Returns the
// decoded bytes and the offset just past the literal.
func readString(scratch *[]byte, src []byte, p int, prefixBits uint8) (out []byte, newP int, err error) {
	if p >= len(src) {
		return nil, p, ErrDecompressionFailed
	}
	huff := src[p]&(byte(1)<<prefixBits) != 0
	length, n, derr := hpack.DecodeInteger(src[p:], prefixBits)
	if derr != nil {
		return nil, p, ErrDecompressionFailed
	}
	p += n
	if length > uint64(len(src)-p) {
		return nil, p, ErrDecompressionFailed
	}
	raw := src[p : p+int(length)]
	p += int(length)
	if !huff {
		return raw, p, nil
	}
	dec, herr := hpack.HuffmanDecode((*scratch)[:0], raw)
	if herr != nil {
		return nil, p, ErrDecompressionFailed
	}
	*scratch = dec
	return dec, p, nil
}
