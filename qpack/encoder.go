package qpack

import (
	"errors"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Encoder compresses HTTP/3 field sections (RFC 9204 §4.5). A zero-value Encoder
// (or one from NewEncoder) is the static-table-only profile: it references the
// static table plus literals, so every section it produces has Required Insert
// Count 0, is immediately decodable, never causes head-of-line blocking, and emits
// nothing on the QPACK encoder stream.
//
// An Encoder from NewDynamicEncoder additionally maintains its OWN dynamic table —
// the table the peer maintains as decoder — inserting repeated request-header
// entries and referencing acknowledged ones (RFC 9204 §2.1). It is a conservative
// encoder: it inserts an entry the first time it is seen and references it only
// once the peer's decoder has acknowledged receiving it (Known Received Count,
// §2.1.4), so a referenced section's Required Insert Count is never ahead of the
// peer's insert count and no request stream ever blocks. It never evicts (it only
// inserts while there is room under the capacity), so an entry referenced by an
// in-flight request cannot be dropped. Inserts produce encoder-stream instructions
// accumulated internally; the owning connection drains them with
// DrainEncoderInstructions and writes them on its encoder stream.
//
// Like frame and hpack, this is an A-layer codec: it does no networking and is NOT
// safe for concurrent use — the owning connection holds one Encoder and serializes
// every method (the field-section encode, the instruction drain, and the
// decoder-stream apply) under a single lock.
type Encoder struct {
	// dt is the encoder's dynamic table, or nil for the static-only profile. When
	// non-nil its maxCapacity is the PEER's advertised SETTINGS_QPACK_MAX_TABLE_
	// CAPACITY (the decoder controls the table size, §3.2.3), which fixes MaxEntries
	// for the Required Insert Count encoding (§4.5.1.1).
	dt *DynamicTable
	// known is the Known Received Count (§2.1.4): the number of the encoder's
	// insertions the peer's decoder has acknowledged. Only entries at an absolute
	// index below it may be referenced without risking a decode failure.
	known uint64
	// pending holds encoder-stream instructions (Set Dynamic Table Capacity, Insert)
	// produced but not yet handed to the connection by DrainEncoderInstructions.
	pending []byte
	// reps is reused per-encode scratch: one representation descriptor per field.
	reps []fieldRep
}

// repKind is the chosen wire representation for one field line.
type repKind uint8

const (
	repStaticIndexed  repKind = iota // Indexed Field Line, static table (§4.5.2)
	repDynamicIndexed                // Indexed Field Line, dynamic Base-relative (§4.5.2)
	repStaticNameRef                 // Literal with static Name Reference (§4.5.4)
	repLiteral                       // Literal with Literal Name (§4.5.6)
)

// fieldRep records the representation decision for field i so the second pass can
// write the field lines after the section prefix (which needs the referenced
// indices to compute Required Insert Count and Base).
type fieldRep struct {
	kind repKind
	idx  uint64 // static index (repStaticIndexed / repStaticNameRef) or ABSOLUTE dynamic index (repDynamicIndexed)
}

// NewEncoder returns a static-table-only QPACK encoder.
func NewEncoder() *Encoder { return &Encoder{} }

// NewDynamicEncoder returns an encoder that maintains a dynamic table (RFC 9204
// §2.1). maxCapacity is the PEER's advertised SETTINGS_QPACK_MAX_TABLE_CAPACITY,
// which bounds the table and fixes MaxEntries for the Required Insert Count
// encoding; tableCapacity is the size the encoder chooses for its table and MUST
// NOT exceed maxCapacity (§3.2.3). The encoder queues a Set Dynamic Table Capacity
// instruction (§4.3.1) as the first thing to send on the encoder stream, retrieved
// with DrainEncoderInstructions. maxCapacity below 32 (no whole entry fits, and
// MaxEntries would be 0) is rejected: use NewEncoder for the static-only profile.
func NewDynamicEncoder(maxCapacity, tableCapacity uint64) (*Encoder, error) {
	if maxCapacity < 32 {
		return nil, ErrEncoderStream
	}
	if tableCapacity > maxCapacity {
		tableCapacity = maxCapacity
	}
	e := &Encoder{dt: NewDynamicTable(maxCapacity)}
	if err := e.dt.setCapacity(tableCapacity); err != nil {
		return nil, err
	}
	// Set Dynamic Table Capacity (001xxxxx, 5-bit prefix): RFC 9204 §4.3.1.
	e.pending = hpack.EncodeInteger(e.pending, 5, 0x20, tableCapacity)
	return e, nil
}

// DrainEncoderInstructions appends every encoder-stream instruction the encoder
// has produced since the last drain to dst and returns the extended slice,
// clearing the internal buffer. The connection calls it under the same lock as the
// field-section encode, so the instructions are handed out in insertion order.
func (e *Encoder) DrainEncoderInstructions(dst []byte) []byte {
	dst = append(dst, e.pending...)
	e.pending = e.pending[:0]
	return dst
}

// KnownReceivedCount reports the Known Received Count (RFC 9204 §2.1.4): the
// number of the encoder's insertions the peer's decoder has acknowledged.
func (e *Encoder) KnownReceivedCount() uint64 { return e.known }

// InsertCount reports the encoder's dynamic-table insert count, or 0 for the
// static-only profile.
func (e *Encoder) InsertCount() uint64 {
	if e.dt == nil {
		return 0
	}
	return e.dt.insertCount
}

// EncodeFieldSection appends the QPACK encoding of fields to dst and returns the
// extended slice. The static-only profile (a nil dynamic table) begins with the
// two-byte prefix 0x00 0x00 (Required Insert Count 0, Base 0) and uses only static
// and literal representations (RFC 9204 §4.5.2/§4.5.4/§4.5.6). The dynamic profile
// additionally references acknowledged dynamic-table entries and inserts new
// repeated entries, producing encoder-stream instructions retrievable with
// DrainEncoderInstructions. Field names MUST already be lowercase (RFC 9114 §4.2).
func (e *Encoder) EncodeFieldSection(dst []byte, fields []hpack.HeaderField) []byte {
	if e.dt == nil {
		dst = append(dst, 0x00, 0x00)
		return appendStaticFieldLines(dst, fields)
	}
	return e.encodeDynamicFieldSection(dst, fields)
}

// appendStaticFieldLines writes each field with a static or literal representation
// (RFC 9204 §4.5.2/§4.5.4/§4.5.6). Shared by the static-only profile and the
// dynamic profile's fallback for a field it does not reference dynamically.
func appendStaticFieldLines(dst []byte, fields []hpack.HeaderField) []byte {
	for i := range fields {
		dst = appendStaticFieldLine(dst, fields[i].Name, fields[i].Value, fields[i].Sensitive)
	}
	return dst
}

// appendStaticFieldLine writes one field as an Indexed Field Line (static),
// a Literal with static Name Reference, or a Literal with Literal Name. A
// sensitive field gets the N bit set on its literal form (§7.1): the peer and any
// intermediary MUST NOT index it. An exact static match carries no secret — the
// value is a fixed public constant — so it stays a plain Indexed Field Line.
func appendStaticFieldLine(dst, name, value []byte, sensitive bool) []byte {
	idx, exact, nameMatch := lookupStatic(name, value)
	switch {
	case exact:
		// Indexed Field Line, static table (1Txxxxxx, T=1): §4.5.2.
		return hpack.EncodeInteger(dst, 6, 0xc0, uint64(idx))
	case nameMatch:
		// Literal Field Line with Name Reference, static (01NTxxxx, T=1): §4.5.4.
		dst = hpack.EncodeInteger(dst, 4, staticNameRefByte(sensitive), uint64(idx))
		return encodeStringLiteral(dst, value, 7, 0x00)
	default:
		// Literal Field Line with Literal Name (001NHxxx): §4.5.6.
		dst = encodeLiteralName(dst, name, sensitive)
		return encodeStringLiteral(dst, value, 7, 0x00)
	}
}

// staticNameRefByte is the Literal Field Line with Name Reference prefix
// (01NTxxxx, §4.5.4) for a static name reference: T=1, plus N=1 when the field is
// marked sensitive.
func staticNameRefByte(sensitive bool) byte {
	if sensitive {
		return 0x70 // 01 N=1 T=1
	}
	return 0x50 // 01 N=0 T=1
}

// encodeDynamicFieldSection encodes fields using the dynamic table where an entry
// is present AND acknowledged, inserting new referenceable entries where the
// capacity permits, and prefixing the section with the resolved Required Insert
// Count and Base (RFC 9204 §4.5.1). Base is chosen equal to the Required Insert
// Count, so every dynamic reference is a plain Base-relative index (no post-Base
// forms are needed).
func (e *Encoder) encodeDynamicFieldSection(dst []byte, fields []hpack.HeaderField) []byte {
	e.reps = e.reps[:0]
	var maxRefAbs uint64
	haveRef := false
	for i := range fields {
		name, value := fields[i].Name, fields[i].Value
		idx, exact, nameMatch := lookupStatic(name, value)
		if exact {
			// A static exact match is already a 1-byte Indexed Field Line; the dynamic
			// table cannot beat it, so never insert or reference dynamically for it.
			e.reps = append(e.reps, fieldRep{repStaticIndexed, uint64(idx)})
			continue
		}
		// A sensitive field never touches the dynamic table — not inserted, not
		// referenced. The dynamic table is one compression context shared by every
		// request on the connection, so mixing a confidential value into it with
		// attacker-controlled data is the BREACH/CRIME opening RFC 9114 §10.3
		// forbids; RFC 9204 §7.1 sends such a field as a literal with the N bit set,
		// which the literal representations below do.
		if !fields[i].Sensitive {
			if abs, ok := e.dt.lookupDynamic(name, value); ok {
				if abs < e.known {
					// Present and acknowledged: reference it (Base-relative Indexed, §4.5.2).
					e.reps = append(e.reps, fieldRep{repDynamicIndexed, abs})
					if !haveRef || abs > maxRefAbs {
						maxRefAbs = abs
						haveRef = true
					}
					continue
				}
				// Present but not yet acknowledged (or inserted earlier in THIS section):
				// fall through to a static/literal representation and do NOT re-insert.
			} else if e.dt.canInsertWithoutEvict(len(name), len(value)) {
				// New repeated-header candidate with room: insert it for future requests.
				// This occurrence is still encoded statically (the entry is not yet
				// acknowledged, so it cannot be referenced here).
				e.insertEntry(name, value, idx, nameMatch)
			}
		}
		if nameMatch {
			e.reps = append(e.reps, fieldRep{repStaticNameRef, uint64(idx)})
		} else {
			e.reps = append(e.reps, fieldRep{repLiteral, 0})
		}
	}

	var ric uint64
	if haveRef {
		ric = maxRefAbs + 1
	}
	dst = e.appendSectionPrefix(dst, ric)
	base := ric
	for i := range fields {
		rep := e.reps[i]
		switch rep.kind {
		case repStaticIndexed:
			dst = hpack.EncodeInteger(dst, 6, 0xc0, rep.idx)
		case repDynamicIndexed:
			// Indexed Field Line, dynamic (1Txxxxxx, T=0), Base-relative index
			// Base - abs - 1 (§4.5.2, §3.2.5).
			dst = hpack.EncodeInteger(dst, 6, 0x80, base-rep.idx-1)
		case repStaticNameRef:
			dst = hpack.EncodeInteger(dst, 4, staticNameRefByte(fields[i].Sensitive), rep.idx)
			dst = encodeStringLiteral(dst, fields[i].Value, 7, 0x00)
		case repLiteral:
			dst = encodeLiteralName(dst, fields[i].Name, fields[i].Sensitive)
			dst = encodeStringLiteral(dst, fields[i].Value, 7, 0x00)
		}
	}
	return dst
}

// appendSectionPrefix writes the Encoded Field Section Prefix (RFC 9204 §4.5.1):
// the Required Insert Count (§4.5.1.1) then the signed Base (§4.5.1.2). Base is
// always chosen equal to the Required Insert Count, so DeltaBase is 0 with Sign 0.
func (e *Encoder) appendSectionPrefix(dst []byte, ric uint64) []byte {
	if ric == 0 {
		// No dynamic reference: RIC 0, Base 0 — identical to the static-only prefix.
		return append(dst, 0x00, 0x00)
	}
	// Required Insert Count, 8-bit prefix (§4.5.1.1). With the eviction-free table
	// TotalInserts <= MaxEntries, so the encoded value is simply RIC+1 and never
	// wraps; encodeRequiredInsertCount computes it generally regardless.
	dst = hpack.EncodeInteger(dst, 8, 0x00, encodeRequiredInsertCount(ric, e.dt.maxEntries()))
	// Base = RIC ⇒ DeltaBase 0, Sign 0 (§4.5.1.2): a single 0x00 byte.
	return append(dst, 0x00)
}

// insertEntry inserts (name, value) into the dynamic table and queues the matching
// encoder-stream Insert instruction (RFC 9204 §4.3.2/§4.3.3). The caller has
// already verified the entry fits without eviction (canInsertWithoutEvict), so the
// insert succeeds; if it somehow did not, no instruction is queued. When the name
// matches a static-table entry an Insert With Name Reference is used (smaller than
// re-encoding the name), otherwise an Insert With Literal Name.
func (e *Encoder) insertEntry(name, value []byte, staticIdx int, nameMatch bool) {
	if !e.dt.insert(name, value) {
		return
	}
	if nameMatch {
		// Insert With Name Reference, static (1Txxxxxx, T=1): §4.3.2.
		e.pending = hpack.EncodeInteger(e.pending, 6, 0xc0, uint64(staticIdx))
		e.pending = encodeStringLiteral(e.pending, value, 7, 0x00)
		return
	}
	// Insert With Literal Name (01Hxxxxx): §4.3.3 — name has a 5-bit-prefix length
	// with the H bit at position 5, then a 7-bit-prefix value literal.
	e.pending = encodeStringLiteral(e.pending, name, 5, 0x40)
	e.pending = encodeStringLiteral(e.pending, value, 7, 0x00)
}

// ParseDecoderInstructions applies as many whole decoder-stream instructions
// (RFC 9204 §4.4) from src as possible — the acknowledgments the peer's decoder
// sends about the encoder's dynamic table — and returns the bytes consumed. A
// trailing partial instruction is left unconsumed for the next call. An Insert
// Count Increment (§4.4.3) advances the Known Received Count; a Section
// Acknowledgment (§4.4.1) and a Stream Cancellation (§4.4.2) release outstanding
// references and are otherwise a no-op in this eviction-free reference-after-ack
// profile (an entry is never referenced before an Insert Count Increment covers
// it, so a Section Acknowledgment can only confirm a Known Received Count already
// reached). It returns ErrDecoderStream (QPACK_DECODER_STREAM_ERROR) for a
// malformed instruction, a zero increment, or an increment past the insert count.
func (e *Encoder) ParseDecoderInstructions(src []byte) (int, error) {
	p := 0
	for p < len(src) {
		b := src[p]
		var prefix uint8
		isIncrement := false
		switch {
		case b&0x80 != 0:
			prefix = 7 // Section Acknowledgment (1xxxxxxx, 7-bit stream id): §4.4.1.
		case b&0x40 != 0:
			prefix = 6 // Stream Cancellation (01xxxxxx, 6-bit stream id): §4.4.2.
		default:
			prefix = 6 // Insert Count Increment (00xxxxxx, 6-bit increment): §4.4.3.
			isIncrement = true
		}
		val, n, err := decodeInt(src[p:], prefix)
		if err != nil {
			if errors.Is(err, hpack.ErrTruncated) {
				break // partial instruction: leave it unconsumed and resume later
			}
			return p, ErrDecoderStream
		}
		if isIncrement {
			// The increment MUST be non-zero and MUST NOT advance the Known Received
			// Count past the entries the encoder has inserted (RFC 9204 §4.4.3, §2.1.4).
			if val == 0 || e.known+val > e.InsertCount() {
				return p, ErrDecoderStream
			}
			e.known += val
		}
		p += n
	}
	return p, nil
}

// encodeRequiredInsertCount is the RFC 9204 §4.5.1.1 encoding of the Required
// Insert Count: 0 stays 0, otherwise (ric mod 2*MaxEntries) + 1. It is the inverse
// of reqInsertCountFromEncoded. maxEntries is guaranteed non-zero here (the dynamic
// encoder requires maxCapacity >= 32), and a non-zero RIC implies a referenced
// entry, which implies at least one insertion fit.
func encodeRequiredInsertCount(ric, maxEntries uint64) uint64 {
	if ric == 0 {
		return 0
	}
	return (ric % (2 * maxEntries)) + 1
}

// encodeLiteralName writes the name field of a Literal-with-Literal-Name
// representation: a 3-bit-prefix string with the 001 prefix, the N bit at bit 4,
// and the H bit at bit 3.
func encodeLiteralName(dst, name []byte, sensitive bool) []byte {
	base := byte(0x20) // 001 N=0
	if sensitive {
		base = 0x30 // 001 N=1
	}
	if hlen := hpack.HuffmanEncodedLen(name); hlen < len(name) {
		dst = hpack.EncodeInteger(dst, 3, base|0x08, uint64(hlen)) // H=1
		return hpack.HuffmanEncode(dst, name)
	}
	dst = hpack.EncodeInteger(dst, 3, base, uint64(len(name))) // H=0
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
