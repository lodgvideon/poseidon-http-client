package qpack

import (
	"errors"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// This file implements the QPACK instruction streams (RFC 9204 §4.3, §4.4): the
// decoder-instruction emitters the client sends on its decoder stream (Section
// Acknowledgment, Stream Cancellation, Insert Count Increment) and the
// encoder-instruction parser it applies to the dynamic table. With the client
// advertising a non-zero SETTINGS_QPACK_MAX_TABLE_CAPACITY these are exchanged on
// the wire for every connection that uses the dynamic table.

// --- Decoder instructions (RFC 9204 §4.4): the decoder emits these ---

// AppendSectionAcknowledgment appends a Section Acknowledgment instruction
// (RFC 9204 §4.4.1) for streamID: a '1' pattern bit followed by the stream ID as
// a 7-bit prefix integer. The decoder sends it once it has fully decoded a field
// section, allowing the encoder to reclaim referenced entries.
func AppendSectionAcknowledgment(dst []byte, streamID uint64) []byte {
	return hpack.EncodeInteger(dst, 7, 0x80, streamID)
}

// AppendStreamCancellation appends a Stream Cancellation instruction (RFC 9204
// §4.4.2) for streamID: a '01' pattern followed by the stream ID as a 6-bit
// prefix integer. The decoder sends it when a stream is abandoned before its
// field section was decoded.
func AppendStreamCancellation(dst []byte, streamID uint64) []byte {
	return hpack.EncodeInteger(dst, 6, 0x40, streamID)
}

// AppendInsertCountIncrement appends an Insert Count Increment instruction
// (RFC 9204 §4.4.3): a '00' pattern followed by the increment as a 6-bit prefix
// integer. The increment MUST be non-zero (a zero increment is a decoder-stream
// error for the peer); callers pass the number of newly acknowledged inserts.
func AppendInsertCountIncrement(dst []byte, increment uint64) []byte {
	return hpack.EncodeInteger(dst, 6, 0x00, increment)
}

// --- Encoder instructions (RFC 9204 §4.3): the decoder parses and applies ---

// ParseEncoderInstructions applies as many whole encoder instructions (RFC 9204
// §4.3) from src to the table as possible and returns the number of bytes
// consumed. A trailing partial instruction is left unconsumed so the caller can
// resume once more bytes of the encoder stream arrive. It returns a non-nil
// error — an ErrEncoderStream (QPACK_ENCODER_STREAM_ERROR) condition — if an
// instruction is malformed or would violate the table capacity; the bytes
// consumed up to that instruction are still returned.
func (dt *DynamicTable) ParseEncoderInstructions(src []byte) (int, error) {
	p := 0
	for p < len(src) {
		n, needMore, err := dt.parseOneEncoderInstruction(src[p:])
		if err != nil {
			return p, err
		}
		if needMore {
			break
		}
		p += n
	}
	return p, nil
}

// parseOneEncoderInstruction parses and applies a single encoder instruction at
// src[0:]. It returns the bytes consumed, or needMore=true (with n=0) when src
// holds only a partial instruction, or an error for a malformed instruction.
func (dt *DynamicTable) parseOneEncoderInstruction(src []byte) (n int, needMore bool, err error) {
	b := src[0]
	switch {
	case b&0x80 != 0:
		// Insert With Name Reference (1Txxxxxx): RFC 9204 §4.3.2.
		return dt.parseInsertWithNameRef(src, b&0x40 != 0)
	case b&0x40 != 0:
		// Insert With Literal Name (01Hxxxxx): RFC 9204 §4.3.3.
		return dt.parseInsertWithLiteralName(src)
	case b&0x20 != 0:
		// Set Dynamic Table Capacity (001xxxxx): RFC 9204 §4.3.1.
		capacity, nc, ierr := hpack.DecodeInteger(src, 5)
		if ierr != nil {
			return needMoreOrErr(ierr)
		}
		if serr := dt.setCapacity(capacity); serr != nil {
			return 0, false, serr
		}
		return nc, false, nil
	default:
		// Duplicate (000xxxxx): RFC 9204 §4.3.4.
		return dt.parseDuplicate(src)
	}
}

// parseInsertWithNameRef handles Insert With Name Reference (RFC 9204 §4.3.2).
// The name comes from the static table (static=true) or the dynamic table by an
// index relative to the current insert count; the value is a string literal.
func (dt *DynamicTable) parseInsertWithNameRef(src []byte, static bool) (int, bool, error) {
	idx, ni, ierr := hpack.DecodeInteger(src, 6)
	if ierr != nil {
		return needMoreOrErr(ierr)
	}
	var name []byte
	if static {
		if idx >= uint64(len(staticTable)) {
			return 0, false, ErrEncoderStream
		}
		name = staticNameB[idx]
	} else {
		nm, _, ok := dt.atInsertCountRelative(idx)
		if !ok {
			return 0, false, ErrEncoderStream
		}
		// Copy: the referenced entry may be evicted or the arena compacted by
		// the insert below, invalidating a slice that aliases it.
		dt.nameScratch = append(dt.nameScratch[:0], nm...)
		name = dt.nameScratch
	}
	val, nv, more, verr := readInstrString(&dt.valScratch, src[ni:], 7)
	if verr != nil {
		return 0, false, verr
	}
	if more {
		return 0, true, nil
	}
	if !dt.insert(name, val) {
		return 0, false, ErrEncoderStream
	}
	return ni + nv, false, nil
}

// parseInsertWithLiteralName handles Insert With Literal Name (RFC 9204 §4.3.3):
// a literal name followed by a literal value, both string literals.
func (dt *DynamicTable) parseInsertWithLiteralName(src []byte) (int, bool, error) {
	name, nn, more, nerr := readInstrString(&dt.nameScratch, src, 5)
	if nerr != nil {
		return 0, false, nerr
	}
	if more {
		return 0, true, nil
	}
	val, nv, more, verr := readInstrString(&dt.valScratch, src[nn:], 7)
	if verr != nil {
		return 0, false, verr
	}
	if more {
		return 0, true, nil
	}
	if !dt.insert(name, val) {
		return 0, false, ErrEncoderStream
	}
	return nn + nv, false, nil
}

// parseDuplicate handles Duplicate (RFC 9204 §4.3.4): re-insert an existing
// dynamic-table entry named by an index relative to the current insert count.
func (dt *DynamicTable) parseDuplicate(src []byte) (int, bool, error) {
	idx, ni, ierr := hpack.DecodeInteger(src, 5)
	if ierr != nil {
		return needMoreOrErr(ierr)
	}
	name, value, ok := dt.atInsertCountRelative(idx)
	if !ok {
		return 0, false, ErrEncoderStream
	}
	// Both alias the arena and the older of the two entries may be evicted by
	// the insert, so copy before inserting.
	dt.nameScratch = append(dt.nameScratch[:0], name...)
	dt.valScratch = append(dt.valScratch[:0], value...)
	if !dt.insert(dt.nameScratch, dt.valScratch) {
		return 0, false, ErrEncoderStream
	}
	return ni, false, nil
}

// readInstrString reads a QPACK string literal on an instruction stream: an H
// bit at position prefixBits, a prefixBits-wide length, then that many octets.
// A raw literal is returned aliasing src; a Huffman literal is decoded into
// *scratch (reused, grown as needed). needMore is true when src holds only part
// of the literal.
func readInstrString(scratch *[]byte, src []byte, prefixBits uint8) (out []byte, n int, needMore bool, err error) {
	if len(src) == 0 {
		return nil, 0, true, nil
	}
	huff := src[0]&(byte(1)<<prefixBits) != 0
	length, ni, derr := hpack.DecodeInteger(src, prefixBits)
	if derr != nil {
		if errors.Is(derr, hpack.ErrTruncated) {
			return nil, 0, true, nil
		}
		return nil, 0, false, ErrEncoderStream
	}
	if length > uint64(len(src)-ni) {
		return nil, 0, true, nil // literal not fully arrived yet
	}
	end := ni + int(length)
	raw := src[ni:end]
	if !huff {
		return raw, end, false, nil
	}
	dec, herr := hpack.HuffmanDecode((*scratch)[:0], raw)
	if herr != nil {
		return nil, 0, false, ErrEncoderStream
	}
	*scratch = dec
	return dec, end, false, nil
}

// needMoreOrErr maps a prefix-integer decode error to the instruction-parser
// convention: a truncated integer means more encoder-stream bytes are needed,
// anything else (overflow) is a malformed instruction.
func needMoreOrErr(err error) (int, bool, error) {
	if errors.Is(err, hpack.ErrTruncated) {
		return 0, true, nil
	}
	return 0, false, ErrEncoderStream
}
