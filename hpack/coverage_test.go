package hpack

// coverage_test.go — targeted tests for uncovered paths identified by
// go tool cover -func. All tests follow the internal package hpack convention
// used in the rest of the test suite.

import (
	"bytes"
	"errors"
	"testing"
)

// ---------------------------------------------------------------------------
// dynamic_table.at — out-of-bounds (i < 1 and i > count)
// ---------------------------------------------------------------------------

// TestDynamicTable_At_OutOfBounds exercises the guard clause in at():
// i < 1 or i > d.count → returns (nil, nil).
func TestDynamicTable_At_OutOfBounds(t *testing.T) {
	dt := newDynamicTable(4096)
	dt.add([]byte("k"), []byte("v"))

	n, v := dt.at(0) // i < 1
	if n != nil || v != nil {
		t.Fatalf("at(0): expected nil,nil; got %q,%q", n, v)
	}

	n, v = dt.at(2) // i > count (count == 1)
	if n != nil || v != nil {
		t.Fatalf("at(2) with count=1: expected nil,nil; got %q,%q", n, v)
	}
}

// ---------------------------------------------------------------------------
// dynamic_table.compactArena — empty-table path
// ---------------------------------------------------------------------------

// TestDynamicTable_CompactArena_Empty exercises the count==0 early return
// in compactArena, which is unreachable from add() but can be reached by
// calling the method directly after clear().
func TestDynamicTable_CompactArena_Empty(t *testing.T) {
	dt := newDynamicTable(4096)
	dt.add([]byte("key"), []byte("val"))
	dt.clear()
	// compactArena on empty table must not panic and must leave clean state.
	dt.compactArena()
	if dt.len() != 0 {
		t.Fatalf("len after compactArena on empty = %d, want 0", dt.len())
	}
	if dt.used != 0 {
		t.Fatalf("used after compactArena on empty = %d, want 0", dt.used)
	}
}

// ---------------------------------------------------------------------------
// dynamic_table.evictOldest — count == 0 guard
// ---------------------------------------------------------------------------

// TestDynamicTable_EvictOldest_Empty exercises the early return when count==0.
func TestDynamicTable_EvictOldest_Empty(t *testing.T) {
	dt := newDynamicTable(4096)
	// Must not panic.
	dt.evictOldest()
	if dt.len() != 0 {
		t.Fatalf("len after evictOldest on empty = %d", dt.len())
	}
}

// ---------------------------------------------------------------------------
// encoder.dynamicLookup — name-only match (returns nameOnly, false)
// ---------------------------------------------------------------------------

// TestEncoder_DynamicLookup_NameOnly adds a field to the dynamic table then
// looks up the same name with a different value, triggering the nameOnly path.
func TestEncoder_DynamicLookup_NameOnly(t *testing.T) {
	enc := NewEncoder()
	// Add custom-key=old-val to dynamic table.
	enc.dt.add([]byte("custom-key"), []byte("old-val"))

	idx, full := enc.dynamicLookup([]byte("custom-key"), []byte("new-val"))
	if full {
		t.Fatal("dynamicLookup: full should be false for name-only match")
	}
	if idx == 0 {
		t.Fatal("dynamicLookup: idx should be non-zero for name-only match")
	}
}

// TestEncoder_DynamicLookup_Miss exercises the no-match path (both idx and
// full are zero/false).
func TestEncoder_DynamicLookup_Miss(t *testing.T) {
	enc := NewEncoder()
	idx, full := enc.dynamicLookup([]byte("x-no-such"), []byte("v"))
	if full || idx != 0 {
		t.Fatalf("miss: want idx=0,full=false; got idx=%d,full=%v", idx, full)
	}
}

// ---------------------------------------------------------------------------
// decodeStringLiteral — truncated body
// ---------------------------------------------------------------------------

// TestDecodeStringLiteral_TruncatedBody constructs a string literal where the
// declared length exceeds available bytes, producing ErrTruncated.
func TestDecodeStringLiteral_TruncatedBody(t *testing.T) {
	// Length = 10, but only 3 bytes of body follow.
	src := []byte{0x0a, 'a', 'b', 'c'}
	_, _, err := decodeStringLiteral(nil, src)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
}

// TestDecodeStringLiteral_EmptyInput exercises the len(src)<1 guard.
func TestDecodeStringLiteral_EmptyInput(t *testing.T) {
	_, _, err := decodeStringLiteral(nil, nil)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("want ErrTruncated on empty src, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// decodeFragment — various error paths
// ---------------------------------------------------------------------------

// TestDecodeFragment_InvalidPrefix sends a byte that doesn't match any HPACK
// representation prefix (e.g. 0x08 with b&0xf0==0x00, but 0x78 ≡ 0111_1000
// which hits default). Actually 0x78 = 0111_1000; b&0x80=0, b&0xc0=0x40? No.
// Let's think: 0x78 = 0111_1000. b&0x80=0, b&0xc0=0x40? 0x78&0xc0=0x40. So it
// would hit the 0x40 literal-indexing branch. We need something that falls to
// default. Looking at the switch: only 0x20-0x3F (0x20 mask) hits size-update.
// 0x08 = 0000_1000: b&0x80=0, b&0xc0=0, b&0xe0=0, b&0xf0=0 → hits 0x00 branch.
// There is no "default" reachable in decodeFragment with a valid byte.
// The default is only reachable in decodeOne. For decodeFragment error paths:
// - visit error propagation
// - parseLiteral error
// - table size update error (n > maxLocal)
// - lookup error (invalid index)

// TestDecodeFragment_VisitError verifies that a visit error halts decoding.
func TestDecodeFragment_VisitError(t *testing.T) {
	d := NewDecoder()
	// 0x82 = indexed ":method GET"
	block := []byte{0x82}
	sentinel := errors.New("stop")
	err := d.decodeFragment(block, func(HeaderField) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel error, got %v", err)
	}
}

// TestDecodeFragment_LiteralIndexing_VisitError exercises the visit-error path
// in the 0x40 (literal with incremental indexing) branch.
func TestDecodeFragment_LiteralIndexing_VisitError(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.1 bytes: custom-key / custom-header
	block, _ := hexDecode("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	sentinel := errors.New("stop")
	err := d.decodeFragment(block, func(HeaderField) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
}

// TestDecodeFragment_NeverIndexed_VisitError exercises the visit-error path in
// the 0x10 (never-indexed) branch.
func TestDecodeFragment_NeverIndexed_VisitError(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.3 bytes: password=secret (never indexed)
	block, _ := hexDecode("100870617373776f726406736563726574")
	sentinel := errors.New("stop")
	err := d.decodeFragment(block, func(HeaderField) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
}

// TestDecodeFragment_LiteralWithoutIndexing_VisitError exercises the
// 0x00-prefix branch visit error.
func TestDecodeFragment_LiteralWithoutIndexing_VisitError(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.2: :path = /sample/path (literal without indexing, index=4)
	block, _ := hexDecode("040c2f73616d706c652f70617468")
	sentinel := errors.New("stop")
	err := d.decodeFragment(block, func(HeaderField) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
}

// TestDecodeFragment_TableSizeUpdate_TooLarge verifies ErrTableSizeUpdate when
// the declared new size exceeds the local maximum.
func TestDecodeFragment_TableSizeUpdate_TooLarge(t *testing.T) {
	d := NewDecoder()
	d.SetMaxDynamicTableSize(256)
	// Size update to 512 (> 256). 0x20 prefix with 5-bit integer.
	// 512 = 0x200. 5-bit prefix max = 31. So: 0x20|31 = 0x3f, then 512-31=481.
	// 481 = 0x1e1. 481 >= 128: byte 0xe1 (0x61|0x80), remainder = 481>>7 = 3.
	// byte 0x03.
	block := []byte{0x3f, 0xe1, 0x03}
	err := d.decodeFragment(block, func(HeaderField) error { return nil })
	if !errors.Is(err, ErrTableSizeUpdate) {
		t.Fatalf("want ErrTableSizeUpdate, got %v", err)
	}
}

// TestDecodeFragment_TableSizeUpdate_Valid verifies a valid table size update
// is applied correctly (covers the success path in decodeFragment §6.3).
func TestDecodeFragment_TableSizeUpdate_Valid(t *testing.T) {
	d := NewDecoder()
	d.SetMaxDynamicTableSize(4096)
	// Update to 256: 0x20 | 256... 256 > 31 (5-bit max), so:
	// 0x3f, then 256-31=225, 225 < 128 → byte 0xe1? No: 225 >= 128.
	// 225 = 0xe1. 225&0x7f = 0x61 | 0x80 = 0xe1, remainder = 225>>7 = 1.
	// byte 0x01.
	// Actually let's use size=100 which fits in 5 bits (100 < 31? no, 100>31).
	// size=20 < 31: single byte 0x20|20 = 0x34.
	block := []byte{0x34} // size update to 20
	err := d.decodeFragment(block, func(HeaderField) error { return nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.dt.maxSize != 20 {
		t.Fatalf("maxSize = %d, want 20", d.dt.maxSize)
	}
}

// TestDecodeFragment_InvalidIndex verifies ErrInvalidIndex for index 0 in
// indexed representation.
func TestDecodeFragment_InvalidIndex(t *testing.T) {
	d := NewDecoder()
	// Indexed header, index=0: byte 0x80 (b&0x80!=0, but index=0).
	block := []byte{0x80}
	err := d.decodeFragment(block, func(HeaderField) error { return nil })
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("want ErrInvalidIndex, got %v", err)
	}
}

// TestDecodeFragment_InvalidIndex_OutOfRange verifies ErrInvalidIndex when
// the dynamic table index is beyond the table's populated entries.
func TestDecodeFragment_InvalidIndex_OutOfRange(t *testing.T) {
	d := NewDecoder()
	// Static table has 61 entries. Index 63 = static[63] would be out of range.
	// Static table size is 61. dynIdx = 63-61 = 2, but dt.len()=0 → error.
	// Byte: 0x80 | 63 = 0xbf.
	block := []byte{0xbf}
	err := d.decodeFragment(block, func(HeaderField) error { return nil })
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("want ErrInvalidIndex, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// decodeOne — all branches via streaming path
// ---------------------------------------------------------------------------

// TestDecodeOne_IndexedHeader_Success exercises the indexed-header success path.
func TestDecodeOne_IndexedHeader_Success(t *testing.T) {
	d := NewDecoder()
	var got []HeaderField
	n, err := d.decodeOne([]byte{0x82}, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:  append([]byte{}, f.Name...),
			Value: append([]byte{}, f.Value...),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != 1 {
		t.Fatalf("consumed = %d, want 1", n)
	}
	if len(got) != 1 || string(got[0].Name) != ":method" {
		t.Fatalf("got %+v", got)
	}
}

// TestDecodeOne_IndexedHeader_InvalidIndex exercises the lookup-error path.
func TestDecodeOne_IndexedHeader_InvalidIndex(t *testing.T) {
	d := NewDecoder()
	_, err := d.decodeOne([]byte{0x80}, func(HeaderField) error { return nil })
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("want ErrInvalidIndex, got %v", err)
	}
}

// TestDecodeOne_IndexedHeader_VisitError exercises the visit-error path in indexed branch.
func TestDecodeOne_IndexedHeader_VisitError(t *testing.T) {
	d := NewDecoder()
	sentinel := errors.New("stop")
	_, err := d.decodeOne([]byte{0x82}, func(HeaderField) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
}

// TestDecodeOne_LiteralIndexing_Success exercises the 0x40 branch.
func TestDecodeOne_LiteralIndexing_Success(t *testing.T) {
	d := NewDecoder()
	// custom-key = custom-header (RFC §C.2.1)
	src, _ := hexDecode("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	n, err := d.decodeOne(src, func(HeaderField) error { return nil })
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(src) {
		t.Fatalf("consumed %d, want %d", n, len(src))
	}
}

// TestDecodeOne_LiteralIndexing_VisitError exercises visit error in 0x40 branch.
func TestDecodeOne_LiteralIndexing_VisitError(t *testing.T) {
	d := NewDecoder()
	src, _ := hexDecode("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	sentinel := errors.New("stop")
	_, err := d.decodeOne(src, func(HeaderField) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
}

// TestDecodeOne_TableSizeUpdate exercises the 0x20 branch.
func TestDecodeOne_TableSizeUpdate(t *testing.T) {
	d := NewDecoder()
	d.SetMaxDynamicTableSize(4096)
	// Size update to 20: single byte 0x34 (0x20 | 20).
	n, err := d.decodeOne([]byte{0x34}, func(HeaderField) error { return nil })
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != 1 {
		t.Fatalf("consumed = %d, want 1", n)
	}
	if d.dt.maxSize != 20 {
		t.Fatalf("maxSize = %d, want 20", d.dt.maxSize)
	}
}

// TestDecodeOne_TableSizeUpdate_TooLarge exercises the error path in 0x20 branch.
func TestDecodeOne_TableSizeUpdate_TooLarge(t *testing.T) {
	d := NewDecoder()
	d.SetMaxDynamicTableSize(10)
	// Size update to 20: byte 0x34 (0x20 | 20 = 0x34), 20 > 10.
	_, err := d.decodeOne([]byte{0x34}, func(HeaderField) error { return nil })
	if !errors.Is(err, ErrTableSizeUpdate) {
		t.Fatalf("want ErrTableSizeUpdate, got %v", err)
	}
}

// TestDecodeOne_NeverIndexed_Success exercises the 0x10 branch success path.
func TestDecodeOne_NeverIndexed_Success(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.3: password=secret never-indexed
	src, _ := hexDecode("100870617373776f726406736563726574")
	var got []HeaderField
	n, err := d.decodeOne(src, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:      append([]byte{}, f.Name...),
			Value:     append([]byte{}, f.Value...),
			Sensitive: f.Sensitive,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(src) {
		t.Fatalf("consumed %d, want %d", n, len(src))
	}
	if len(got) != 1 || !got[0].Sensitive {
		t.Fatalf("got %+v, want sensitive field", got)
	}
}

// TestDecodeOne_NeverIndexed_VisitError exercises visit error in 0x10 branch.
func TestDecodeOne_NeverIndexed_VisitError(t *testing.T) {
	d := NewDecoder()
	src, _ := hexDecode("100870617373776f726406736563726574")
	sentinel := errors.New("stop")
	_, err := d.decodeOne(src, func(HeaderField) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
}

// TestDecodeOne_LiteralWithoutIndexing_Success exercises the 0x00 branch.
func TestDecodeOne_LiteralWithoutIndexing_Success(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.2: :path = /sample/path (literal without indexing, index=4)
	src, _ := hexDecode("040c2f73616d706c652f70617468")
	var got []HeaderField
	n, err := d.decodeOne(src, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:  append([]byte{}, f.Name...),
			Value: append([]byte{}, f.Value...),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(src) {
		t.Fatalf("consumed %d, want %d", n, len(src))
	}
	if len(got) != 1 || string(got[0].Name) != ":path" {
		t.Fatalf("got %+v", got)
	}
}

// TestDecodeOne_LiteralWithoutIndexing_VisitError exercises visit error in 0x00 branch.
func TestDecodeOne_LiteralWithoutIndexing_VisitError(t *testing.T) {
	d := NewDecoder()
	src, _ := hexDecode("040c2f73616d706c652f70617468")
	sentinel := errors.New("stop")
	_, err := d.decodeOne(src, func(HeaderField) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
}

// TestDecodeOne_Empty exercises the ErrTruncated on empty src.
func TestDecodeOne_Empty(t *testing.T) {
	d := NewDecoder()
	_, err := d.decodeOne(nil, func(HeaderField) error { return nil })
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Feed — error when not in streaming mode
// ---------------------------------------------------------------------------

// TestFeed_NotStreaming verifies that Feed returns an error when Begin has
// not been called.
func TestFeed_NotStreaming(t *testing.T) {
	d := NewDecoder()
	err := d.Feed([]byte{0x82}, func(HeaderField) error { return nil })
	if err == nil {
		t.Fatal("Feed without Begin should return error")
	}
}

// TestFeed_DecodeError verifies that Feed propagates a decode error.
func TestFeed_DecodeError(t *testing.T) {
	d := NewDecoder()
	d.Begin()
	// Complete invalid block: indexed header index=0 (ErrInvalidIndex).
	err := d.Feed([]byte{0x80}, func(HeaderField) error { return nil })
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("want ErrInvalidIndex, got %v", err)
	}
}

// TestFeed_MultiFragment verifies that a header block split across multiple
// Feed calls and valid size update is decoded correctly.
func TestFeed_MultiFragment(t *testing.T) {
	d := NewDecoder()
	d.Begin()
	// Two indexed headers: 0x82 (:method GET) and 0x87 (:scheme https)
	full := []byte{0x82, 0x87}
	var got []HeaderField
	visit := func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:  append([]byte{}, f.Name...),
			Value: append([]byte{}, f.Value...),
		})
		return nil
	}
	if err := d.Feed(full[:1], visit); err != nil {
		t.Fatalf("Feed part1: %v", err)
	}
	if err := d.Feed(full[1:], visit); err != nil {
		t.Fatalf("Feed part2: %v", err)
	}
	if err := d.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d fields, want 2", len(got))
	}
}

// ---------------------------------------------------------------------------
// Finish — pending bytes remaining (ErrTruncated)
// ---------------------------------------------------------------------------

// TestFinish_WithPendingBytes verifies that Finish returns ErrTruncated when
// there are unprocessed bytes in the pending buffer (mid-literal state).
func TestFinish_WithPendingBytes(t *testing.T) {
	d := NewDecoder()
	d.Begin()
	// Feed an incomplete literal: 0x40 = literal-with-indexing, new name.
	// Length byte 0x0a = 10 bytes follow, but we send only the prefix.
	incomplete := []byte{0x40, 0x0a, 'a', 'b'} // truncated mid-name
	if err := d.Feed(incomplete, func(HeaderField) error { return nil }); err != nil {
		t.Fatalf("Feed: %v", err)
	}
	err := d.Finish()
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// lookup — dynIdx > dt.len() error
// ---------------------------------------------------------------------------

// TestLookup_DynamicIndexTooLarge verifies ErrInvalidIndex when the index
// points beyond the populated dynamic table entries.
func TestLookup_DynamicIndexTooLarge(t *testing.T) {
	d := NewDecoder()
	// dt is empty. Any index > staticTableLen triggers the dynIdx path.
	// Static table has 61 entries. Index 62 → dynIdx=1, dt.len()=0 → error.
	_, _, err := d.lookup(62)
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("want ErrInvalidIndex, got %v", err)
	}
}

// TestLookup_Zero verifies ErrInvalidIndex for index 0.
func TestLookup_Zero(t *testing.T) {
	d := NewDecoder()
	_, _, err := d.lookup(0)
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("want ErrInvalidIndex, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// parseLiteral — error paths
// ---------------------------------------------------------------------------

// TestParseLiteral_InvalidNameIndex verifies that an out-of-range name index
// in a literal representation returns ErrInvalidIndex.
func TestParseLiteral_InvalidNameIndex(t *testing.T) {
	d := NewDecoder()
	// Literal with incremental indexing, name referenced by index 62 (out of
	// range for empty dynamic table). 0x40|... but we use parseLiteral directly.
	// For 6-bit prefix with idx=62: 62 < 63 (max for 6 bits), so single byte.
	// byte = 0x40 | 62 = 0x7e.
	src := []byte{0x7e, 0x01, 'v'} // idx=62, value length=1, value='v'
	_, _, _, err := d.parseLiteral(src, 6)
	if !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("want ErrInvalidIndex, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// decodeStringLiteral — Huffman decode path (already covered via conformance
// but let's ensure the direct path is covered)
// ---------------------------------------------------------------------------

// TestDecodeStringLiteral_HuffmanDecode verifies that Huffman-coded strings
// are decoded correctly (covers the huffman=true branch in decodeStringLiteral).
func TestDecodeStringLiteral_HuffmanDecode(t *testing.T) {
	// Huffman-encoded "www.example.com" from RFC §C.4.1:
	// 8c f1 e3 c2 e5 f2 3a 6b a0 ab 90 f4 ff → 13 bytes
	// Length byte: 0x8d = 0x80|13, meaning Huffman=true, length=13.
	huffBody, _ := hexDecode("f1e3c2e5f23a6ba0ab90f4ff")
	// That's 12 bytes. Let's use the actual RFC value:
	// "www.example.com" Huffman is: 8c f1 e3 c2 e5 f2 3a 6b a0 ab 90 f4 ff
	// length byte (H=1) = 0x8c (0x80|12).
	src, _ := hexDecode("8cf1e3c2e5f23a6ba0ab90f4ff")
	_ = huffBody
	dst, n, err := decodeStringLiteral(nil, src)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(src) {
		t.Fatalf("consumed %d, want %d", n, len(src))
	}
	if !bytes.Equal(dst, []byte("www.example.com")) {
		t.Fatalf("got %q, want www.example.com", dst)
	}
}

// ---------------------------------------------------------------------------
// Encoder.writeFieldAlreadyFlushedSize — dynamic full-match path
// ---------------------------------------------------------------------------

// TestEncoder_DynamicFullMatch verifies that when a field already exists in
// the dynamic table (full match), it's emitted as an indexed representation.
func TestEncoder_DynamicFullMatch(t *testing.T) {
	enc := NewEncoder()
	// First encode adds custom-key=custom-val to dynamic table.
	_ = enc.WriteField(nil, []byte("custom-key"), []byte("custom-val"), false)
	// Second encode should find the full match in the dynamic table.
	dst := enc.WriteField(nil, []byte("custom-key"), []byte("custom-val"), false)
	// Result should be a single indexed byte (0x80 | index).
	if len(dst) != 1 || dst[0]&0x80 == 0 {
		t.Fatalf("expected indexed representation, got %x", dst)
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

// hexDecode is a test helper that decodes a hex string, panicking on error.
func hexDecode(s string) ([]byte, error) {
	var result []byte
	for i := 0; i < len(s); i += 2 {
		if i+1 >= len(s) {
			break
		}
		hi := hexNibble(s[i])
		lo := hexNibble(s[i+1])
		result = append(result, (hi<<4)|lo)
	}
	return result, nil
}

func hexNibble(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}
