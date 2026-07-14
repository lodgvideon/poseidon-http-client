package qpack

import "github.com/lodgvideon/poseidon-http-client/hpack"

// Decoder reads QPACK-encoded field sections (RFC 9204 §4.5). It resolves the
// static table, literals, and — when a non-nil *DynamicTable is supplied to
// DecodeFieldSection — dynamic-table references (indexed, name-reference, and
// their post-Base forms). With a nil table it is the static-only profile: the
// Required Insert Count MUST be 0 and any dynamic reference is rejected as
// ErrDecompressionFailed, matching a client that advertises
// SETTINGS_QPACK_MAX_TABLE_CAPACITY=0. Not safe for concurrent use.
type Decoder struct {
	nameScratch []byte // reused Huffman-decode buffer for a literal name
	valScratch  []byte // reused Huffman-decode buffer for a literal value
}

// NewDecoder returns a QPACK decoder.
func NewDecoder() *Decoder { return &Decoder{} }

// DecodeFieldSection parses the QPACK field section in src, resolving dynamic
// references against dt (nil for the static-only profile), and calls emit once
// per header field, in order. The name and value slices passed to emit are
// valid only until emit returns (they alias src, the Decoder's reused scratch,
// or the table arena); copy to retain. It returns ErrDecompressionFailed on
// malformed input or an unresolvable reference. dt is not mutated. With dt=nil,
// allocates nothing beyond growing the reused scratch on the first Huffman
// literal.
func (d *Decoder) DecodeFieldSection(src []byte, dt *DynamicTable, emit func(name, value []byte) error) error {
	base, ric, p, err := parsePrefix(src, dt)
	if err != nil {
		return err
	}
	// Decoding is synchronous here: every referenced insert must already be in
	// the table. A real blocking decoder would instead wait for the encoder
	// stream to reach the Required Insert Count.
	if ric > 0 && (dt == nil || ric > dt.insertCount) {
		return ErrDecompressionFailed
	}
	for p < len(src) {
		var err error
		switch b := src[p]; {
		case b&0x80 != 0:
			// Indexed Field Line (1Txxxxxx): §4.5.2 (T=1 static, T=0 dynamic).
			p, err = d.decodeIndexed(src, p, base, dt, emit)
		case b&0x40 != 0:
			// Literal Field Line with Name Reference (01NTxxxx): §4.5.3.
			p, err = d.decodeLiteralNameRef(src, p, base, dt, emit)
		case b&0x20 != 0:
			// Literal Field Line with Literal Name (001NHxxx): §4.5.6.
			p, err = d.decodeLiteralName(src, p, emit)
		case b&0x10 != 0:
			// Indexed Field Line with Post-Base Index (0001xxxx): §4.5.4.
			p, err = d.decodePostBaseIndexed(src, p, base, dt, emit)
		default:
			// Literal Field Line with Post-Base Name Reference (0000Nxxx): §4.5.5.
			p, err = d.decodePostBaseNameRef(src, p, base, dt, emit)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// decodeIndexed handles an Indexed Field Line (§4.5.2), static or dynamic
// relative to Base, and returns the offset past it.
func (d *Decoder) decodeIndexed(src []byte, p int, base uint64, dt *DynamicTable, emit func(name, value []byte) error) (int, error) {
	static := src[p]&0x40 != 0
	idx, n, ierr := hpack.DecodeInteger(src[p:], 6)
	if ierr != nil {
		return p, ErrDecompressionFailed
	}
	name, val, ok := resolveIndexed(dt, base, idx, static)
	if !ok {
		return p, ErrDecompressionFailed
	}
	return p + n, emit(name, val)
}

// decodeLiteralNameRef handles a Literal Field Line with Name Reference
// (§4.5.3), static or dynamic relative to Base, followed by a literal value.
func (d *Decoder) decodeLiteralNameRef(src []byte, p int, base uint64, dt *DynamicTable, emit func(name, value []byte) error) (int, error) {
	static := src[p]&0x10 != 0
	idx, n, ierr := hpack.DecodeInteger(src[p:], 4)
	if ierr != nil {
		return p, ErrDecompressionFailed
	}
	name, ok := resolveNameRef(dt, base, idx, static)
	if !ok {
		return p, ErrDecompressionFailed
	}
	val, np, verr := readString(&d.valScratch, src, p+n, 7)
	if verr != nil {
		return p, verr
	}
	return np, emit(name, val)
}

// decodeLiteralName handles a Literal Field Line with Literal Name (§4.5.6).
func (d *Decoder) decodeLiteralName(src []byte, p int, emit func(name, value []byte) error) (int, error) {
	name, np, nerr := readString(&d.nameScratch, src, p, 3)
	if nerr != nil {
		return p, nerr
	}
	val, np2, verr := readString(&d.valScratch, src, np, 7)
	if verr != nil {
		return p, verr
	}
	return np2, emit(name, val)
}

// decodePostBaseIndexed handles an Indexed Field Line with Post-Base Index
// (§4.5.4): the absolute index is Base + idx.
func (d *Decoder) decodePostBaseIndexed(src []byte, p int, base uint64, dt *DynamicTable, emit func(name, value []byte) error) (int, error) {
	idx, n, ierr := hpack.DecodeInteger(src[p:], 4)
	if ierr != nil {
		return p, ErrDecompressionFailed
	}
	if dt == nil {
		return p, ErrDecompressionFailed
	}
	name, val, ok := dt.atPostBase(base, idx)
	if !ok {
		return p, ErrDecompressionFailed
	}
	return p + n, emit(name, val)
}

// decodePostBaseNameRef handles a Literal Field Line with Post-Base Name
// Reference (§4.5.5): the name is at absolute index Base + idx, followed by a
// literal value.
func (d *Decoder) decodePostBaseNameRef(src []byte, p int, base uint64, dt *DynamicTable, emit func(name, value []byte) error) (int, error) {
	idx, n, ierr := hpack.DecodeInteger(src[p:], 3)
	if ierr != nil {
		return p, ErrDecompressionFailed
	}
	if dt == nil {
		return p, ErrDecompressionFailed
	}
	name, _, ok := dt.atPostBase(base, idx)
	if !ok {
		return p, ErrDecompressionFailed
	}
	val, np, verr := readString(&d.valScratch, src, p+n, 7)
	if verr != nil {
		return p, verr
	}
	return np, emit(name, val)
}

// resolveIndexed resolves the name and value of an Indexed Field Line (§4.5.2):
// from the static table when static, otherwise from dt at Base - idx - 1.
func resolveIndexed(dt *DynamicTable, base, idx uint64, static bool) (name, value []byte, ok bool) {
	if static {
		if idx >= uint64(len(staticTable)) {
			return nil, nil, false
		}
		return staticNameB[idx], staticValueB[idx], true
	}
	if dt == nil {
		return nil, nil, false
	}
	return dt.atBaseRelative(base, idx)
}

// resolveNameRef resolves the name of a Literal Field Line with Name Reference
// (§4.5.3): from the static table when static, otherwise from dt at
// Base - idx - 1.
func resolveNameRef(dt *DynamicTable, base, idx uint64, static bool) (name []byte, ok bool) {
	if static {
		if idx >= uint64(len(staticTable)) {
			return nil, false
		}
		return staticNameB[idx], true
	}
	if dt == nil {
		return nil, false
	}
	n, _, o := dt.atBaseRelative(base, idx)
	return n, o
}

// parsePrefix parses the Encoded Field Section Prefix (RFC 9204 §4.5.1): the
// Required Insert Count (§4.5.1.1) and the signed Base (§4.5.1.2). It returns
// the resolved Base, the Required Insert Count, and the offset of the first
// field line. With dt=nil the table is treated as empty (MaxEntries=0), so any
// non-zero encoded Insert Count is rejected — the static-only profile.
func parsePrefix(src []byte, dt *DynamicTable) (base, ric uint64, off int, err error) {
	encIC, n, derr := hpack.DecodeInteger(src, 8)
	if derr != nil {
		return 0, 0, 0, ErrDecompressionFailed
	}
	ric, rerr := decodeRequiredInsertCount(encIC, dt)
	if rerr != nil {
		return 0, 0, 0, rerr
	}
	p := n
	if p >= len(src) {
		return 0, 0, 0, ErrDecompressionFailed // the Base byte must be present
	}
	sign := src[p]&0x80 != 0
	delta, m, derr := hpack.DecodeInteger(src[p:], 7)
	if derr != nil {
		return 0, 0, 0, ErrDecompressionFailed
	}
	p += m
	// Base = RIC + DeltaBase (Sign=0) or RIC - DeltaBase - 1 (Sign=1); a
	// negative Base is malformed (RFC 9204 §4.5.1.2).
	if sign {
		if delta+1 > ric {
			return 0, 0, 0, ErrDecompressionFailed
		}
		base = ric - delta - 1
	} else {
		base = ric + delta
	}
	return base, ric, p, nil
}

// decodeRequiredInsertCount reconstructs the Required Insert Count from its
// wire encoding against the table's insert count (RFC 9204 §4.5.1.1). A nil
// table means MaxEntries=0, so only an encoded value of 0 (RIC 0) is valid.
func decodeRequiredInsertCount(encInsertCount uint64, dt *DynamicTable) (uint64, error) {
	var maxEntries, totalInserts uint64
	if dt != nil {
		maxEntries = dt.maxEntries()
		totalInserts = dt.insertCount
	}
	return reqInsertCountFromEncoded(encInsertCount, maxEntries, totalInserts)
}

// reqInsertCountFromEncoded is the RFC 9204 §4.5.1.1 decoding algorithm: it maps
// the encoded Insert Count back to the absolute Required Insert Count within the
// 2*MaxEntries wraparound window anchored at TotalNumberOfInserts. It returns
// ErrDecompressionFailed for an out-of-range or ambiguous value.
func reqInsertCountFromEncoded(encInsertCount, maxEntries, totalInserts uint64) (uint64, error) {
	if encInsertCount == 0 {
		return 0, nil
	}
	fullRange := 2 * maxEntries
	if fullRange == 0 || encInsertCount > fullRange {
		return 0, ErrDecompressionFailed
	}
	maxValue := totalInserts + maxEntries
	maxWrapped := (maxValue / fullRange) * fullRange
	ric := maxWrapped + encInsertCount - 1
	if ric > maxValue {
		if ric <= fullRange {
			return 0, ErrDecompressionFailed
		}
		ric -= fullRange
	}
	if ric == 0 {
		return 0, ErrDecompressionFailed
	}
	return ric, nil
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
