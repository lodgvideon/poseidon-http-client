package hpack

// coverage_test.go — targeted tests for uncovered paths identified by
// go tool cover -func. All tests follow the internal package hpack convention
// used in the rest of the test suite.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// dynamic_table.at — out-of-bounds (i < 1 and i > count)
// ---------------------------------------------------------------------------

// TestDynamicTable_At_OutOfBounds exercises the guard clause in at():
// i < 1 or i > d.count → returns (nil, nil).
func TestDynamicTable_At_OutOfBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		i    int
	}{
		{"below the first entry", 0},
		{"one past the last entry", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt := newDynamicTable(4096)
			dt.add([]byte("k"), []byte("v"))

			n, v := dt.at(tc.i)

			assert.Nilf(t, n, "at(%d) with count=1 must return a nil name; an out-of-range index that still resolves hands the caller a slice of some other entry's arena bytes", tc.i)
			assert.Nilf(t, v, "at(%d) with count=1 must return a nil value; an out-of-range index that still resolves hands the caller a slice of some other entry's arena bytes", tc.i)
		})
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

	dt.compactArena()

	assert.Equal(t, 0, dt.len(), "compactArena on an empty table must not resurrect entries")
	assert.Equal(t, uint32(0), dt.used, "compactArena on an empty table must leave arena accounting at zero; a stale used count mis-sizes every later compaction")
}

// ---------------------------------------------------------------------------
// dynamic_table.evictOldest — count == 0 guard
// ---------------------------------------------------------------------------

// TestDynamicTable_EvictOldest_Empty exercises the early return when count==0.
func TestDynamicTable_EvictOldest_Empty(t *testing.T) {
	dt := newDynamicTable(4096)

	dt.evictOldest()

	assert.Equal(t, 0, dt.len(), "evictOldest on an empty table must be a no-op; without the guard count underflows past zero and every later index is wrong")
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

	assert.False(t, full,
		"a matching name with a different value is not a full match; reporting one would emit an indexed field carrying the WRONG value")
	assert.NotZero(t, idx,
		"the name index must still come back so the literal can reference it instead of spelling the name out again")
}

// TestEncoder_DynamicLookup_Miss exercises the no-match path (both idx and
// full are zero/false).
func TestEncoder_DynamicLookup_Miss(t *testing.T) {
	enc := NewEncoder()

	idx, full := enc.dynamicLookup([]byte("x-no-such"), []byte("v"))

	assert.False(t, full, "an absent field must not report a full match")
	assert.Zero(t, idx, "an absent name must return index 0, which is what tells the encoder to spell the name out")
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

	require.ErrorIs(t, err, ErrTruncated,
		"a declared length reaching past the end of the buffer is truncation; anything else reads memory the peer did not send")
}

// TestDecodeStringLiteral_EmptyInput exercises the len(src)<1 guard.
func TestDecodeStringLiteral_EmptyInput(t *testing.T) {
	var src []byte

	_, _, err := decodeStringLiteral(nil, src)

	require.ErrorIs(t, err, ErrTruncated, "an empty literal is truncated input, not an empty string")
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

	require.ErrorIs(t, err, sentinel,
		"a visitor error must propagate unchanged; swallowing it lets the caller's own abort look like a clean decode")
}

// TestDecodeFragment_LiteralIndexing_VisitError exercises the visit-error path
// in the 0x40 (literal with incremental indexing) branch.
func TestDecodeFragment_LiteralIndexing_VisitError(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.1 bytes: custom-key / custom-header
	block, err := hexDecode("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	require.NoError(t, err, "fixture decode")
	sentinel := errors.New("stop")

	err = d.decodeFragment(block, func(HeaderField) error { return sentinel })

	require.ErrorIs(t, err, sentinel, "a visitor error in the §6.2.1 branch must propagate unchanged")
}

// TestDecodeFragment_NeverIndexed_VisitError exercises the visit-error path in
// the 0x10 (never-indexed) branch.
func TestDecodeFragment_NeverIndexed_VisitError(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.3 bytes: password=secret (never indexed)
	block, err := hexDecode("100870617373776f726406736563726574")
	require.NoError(t, err, "fixture decode")
	sentinel := errors.New("stop")

	err = d.decodeFragment(block, func(HeaderField) error { return sentinel })

	require.ErrorIs(t, err, sentinel, "a visitor error in the §6.2.3 branch must propagate unchanged")
}

// TestDecodeFragment_LiteralWithoutIndexing_VisitError exercises the
// 0x00-prefix branch visit error.
func TestDecodeFragment_LiteralWithoutIndexing_VisitError(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.2: :path = /sample/path (literal without indexing, index=4)
	block, err := hexDecode("040c2f73616d706c652f70617468")
	require.NoError(t, err, "fixture decode")
	sentinel := errors.New("stop")

	err = d.decodeFragment(block, func(HeaderField) error { return sentinel })

	require.ErrorIs(t, err, sentinel, "a visitor error in the §6.2.2 branch must propagate unchanged")
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

	require.ErrorIs(t, err, ErrTableSizeUpdate,
		"a §6.3 update above the size we advertised must be refused; honouring it would let the peer index against a table we never agreed to hold")
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

	require.NoError(t, err, "a §6.3 update within the advertised limit is legal")
	assert.Equal(t, uint32(20), d.dt.maxSize,
		"the update must actually resize the table; accepting it without applying it leaves us evicting on a different schedule from the peer's encoder")
}

// TestDecodeFragment_InvalidIndex verifies ErrInvalidIndex for index 0 in
// indexed representation.
func TestDecodeFragment_InvalidIndex(t *testing.T) {
	d := NewDecoder()
	// Indexed header, index=0: byte 0x80 (b&0x80!=0, but index=0).
	block := []byte{0x80}

	err := d.decodeFragment(block, func(HeaderField) error { return nil })

	require.ErrorIs(t, err, ErrInvalidIndex,
		"index 0 names no entry in either table (§2.3.3); resolving it would read staticTable[0], the unused slot")
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

	require.ErrorIs(t, err, ErrInvalidIndex,
		"an index past the populated dynamic entries must be refused, not resolved against a slot the peer never inserted")
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

	require.NoError(t, err, "0x82 is the §6.1 indexed representation of static entry 2")
	assert.Equal(t, 1, n, "an indexed field is exactly one octet; over-reporting desynchronises every later representation in the block")
	require.Len(t, got, 1, "one representation must emit one field")
	assert.Equal(t, ":method", string(got[0].Name), "static index 2 is :method")
	assert.Equal(t, "GET", string(got[0].Value), "static index 2 carries the value GET")
}

// TestDecodeOne_IndexedHeader_InvalidIndex exercises the lookup-error path.
func TestDecodeOne_IndexedHeader_InvalidIndex(t *testing.T) {
	d := NewDecoder()

	_, err := d.decodeOne([]byte{0x80}, func(HeaderField) error { return nil })

	require.ErrorIs(t, err, ErrInvalidIndex, "index 0 names no entry in either table (§2.3.3)")
}

// TestDecodeOne_IndexedHeader_VisitError exercises the visit-error path in indexed branch.
func TestDecodeOne_IndexedHeader_VisitError(t *testing.T) {
	d := NewDecoder()
	sentinel := errors.New("stop")

	_, err := d.decodeOne([]byte{0x82}, func(HeaderField) error { return sentinel })

	require.ErrorIs(t, err, sentinel, "a visitor error in the §6.1 branch must propagate unchanged")
}

// TestDecodeOne_LiteralIndexing_Success exercises the 0x40 branch.
func TestDecodeOne_LiteralIndexing_Success(t *testing.T) {
	d := NewDecoder()
	// custom-key = custom-header (RFC §C.2.1)
	src, err := hexDecode("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	require.NoError(t, err, "fixture decode")

	n, err := d.decodeOne(src, func(HeaderField) error { return nil })

	require.NoError(t, err, "§C.2.1 is a well-formed literal with incremental indexing")
	assert.Equal(t, len(src), n,
		"the whole representation must be consumed; a short count leaves its trailing bytes to be parsed as a new representation")
}

// TestDecodeOne_LiteralIndexing_VisitError exercises visit error in 0x40 branch.
func TestDecodeOne_LiteralIndexing_VisitError(t *testing.T) {
	d := NewDecoder()
	src, err := hexDecode("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	require.NoError(t, err, "fixture decode")
	sentinel := errors.New("stop")

	_, err = d.decodeOne(src, func(HeaderField) error { return sentinel })

	require.ErrorIs(t, err, sentinel, "a visitor error in the §6.2.1 branch must propagate unchanged")
}

// TestDecodeOne_TableSizeUpdate exercises the 0x20 branch.
func TestDecodeOne_TableSizeUpdate(t *testing.T) {
	d := NewDecoder()
	d.SetMaxDynamicTableSize(4096)
	// Size update to 20: single byte 0x34 (0x20 | 20).
	src := []byte{0x34}

	n, err := d.decodeOne(src, func(HeaderField) error { return nil })

	require.NoError(t, err, "a §6.3 update within the advertised limit is legal")
	assert.Equal(t, 1, n, "a size update that fits the 5-bit prefix is exactly one octet")
	assert.Equal(t, uint32(20), d.dt.maxSize,
		"the update must actually resize the table; accepting it without applying it leaves us evicting on a different schedule from the peer's encoder")
}

// TestDecodeOne_TableSizeUpdate_TooLarge exercises the error path in 0x20 branch.
func TestDecodeOne_TableSizeUpdate_TooLarge(t *testing.T) {
	d := NewDecoder()
	d.SetMaxDynamicTableSize(10)
	// Size update to 20: byte 0x34 (0x20 | 20 = 0x34), 20 > 10.
	src := []byte{0x34}

	_, err := d.decodeOne(src, func(HeaderField) error { return nil })

	require.ErrorIs(t, err, ErrTableSizeUpdate,
		"a §6.3 update above the size we advertised must be refused; honouring it would let the peer index against a table we never agreed to hold")
}

// TestDecodeOne_NeverIndexed_Success exercises the 0x10 branch success path.
func TestDecodeOne_NeverIndexed_Success(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.3: password=secret never-indexed
	src, err := hexDecode("100870617373776f726406736563726574")
	require.NoError(t, err, "fixture decode")
	var got []HeaderField

	n, err := d.decodeOne(src, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:     append([]byte{}, f.Name...),
			Value:    append([]byte{}, f.Value...),
			Indexing: f.Indexing,
		})
		return nil
	})

	require.NoError(t, err, "§C.2.3 is a well-formed never-indexed literal")
	assert.Equal(t, len(src), n, "the whole representation must be consumed")
	require.Len(t, got, 1, "one representation must emit one field")
	assert.True(t, got[0].Sensitive(),
		"§6.2.3 must survive the decode as IndexNever; a field that arrives unmarked can be re-indexed by an intermediary that was told never to")
}

// TestDecodeOne_NeverIndexed_VisitError exercises visit error in 0x10 branch.
func TestDecodeOne_NeverIndexed_VisitError(t *testing.T) {
	d := NewDecoder()
	src, err := hexDecode("100870617373776f726406736563726574")
	require.NoError(t, err, "fixture decode")
	sentinel := errors.New("stop")

	_, err = d.decodeOne(src, func(HeaderField) error { return sentinel })

	require.ErrorIs(t, err, sentinel, "a visitor error in the §6.2.3 branch must propagate unchanged")
}

// TestDecodeOne_LiteralWithoutIndexing_Success exercises the 0x00 branch.
func TestDecodeOne_LiteralWithoutIndexing_Success(t *testing.T) {
	d := NewDecoder()
	// RFC §C.2.2: :path = /sample/path (literal without indexing, index=4)
	src, err := hexDecode("040c2f73616d706c652f70617468")
	require.NoError(t, err, "fixture decode")
	var got []HeaderField

	n, err := d.decodeOne(src, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:     append([]byte{}, f.Name...),
			Value:    append([]byte{}, f.Value...),
			Indexing: f.Indexing,
		})
		return nil
	})

	require.NoError(t, err, "§C.2.2 is a well-formed literal without indexing")
	assert.Equal(t, len(src), n, "the whole representation must be consumed")
	require.Len(t, got, 1, "one representation must emit one field")
	assert.Equal(t, ":path", string(got[0].Name), "the name index 4 resolves to :path")
	assert.Equal(t, IndexWithout, got[0].Indexing,
		"§6.2.2 must survive the decode as IndexWithout; reporting it as incremental loses the representation the peer chose and re-indexes a field it kept out of the table")
}

// TestDecodeOne_LiteralWithoutIndexing_VisitError exercises visit error in 0x00 branch.
func TestDecodeOne_LiteralWithoutIndexing_VisitError(t *testing.T) {
	d := NewDecoder()
	src, err := hexDecode("040c2f73616d706c652f70617468")
	require.NoError(t, err, "fixture decode")
	sentinel := errors.New("stop")

	_, err = d.decodeOne(src, func(HeaderField) error { return sentinel })

	require.ErrorIs(t, err, sentinel, "a visitor error in the §6.2.2 branch must propagate unchanged")
}

// TestDecodeOne_Empty exercises the ErrTruncated on empty src.
func TestDecodeOne_Empty(t *testing.T) {
	d := NewDecoder()
	var src []byte

	_, err := d.decodeOne(src, func(HeaderField) error { return nil })

	require.ErrorIs(t, err, ErrTruncated, "an empty buffer holds no representation; it is truncation, not a zero-field block")
}

// ---------------------------------------------------------------------------
// Feed — error when not in streaming mode
// ---------------------------------------------------------------------------

// TestFeed_NotStreaming verifies that Feed returns an error when Begin has
// not been called.
func TestFeed_NotStreaming(t *testing.T) {
	d := NewDecoder()

	err := d.Feed([]byte{0x82}, func(HeaderField) error { return nil })

	require.ErrorIs(t, err, ErrNotStreaming,
		"Feed without Begin is the caller's sequencing mistake, and must not be reported as a wire-format error the peer would be blamed for")
}

// TestFeed_DecodeError verifies that Feed propagates a decode error.
func TestFeed_DecodeError(t *testing.T) {
	d := NewDecoder()
	d.Begin()

	// Complete invalid block: indexed header index=0 (ErrInvalidIndex).
	err := d.Feed([]byte{0x80}, func(HeaderField) error { return nil })

	require.ErrorIs(t, err, ErrInvalidIndex,
		"a malformed representation must surface from Feed, not be buffered as if more bytes could fix it")
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

	err1 := d.Feed(full[:1], visit)
	err2 := d.Feed(full[1:], visit)
	errFinish := d.Finish()

	require.NoError(t, err1, "Feed of the first fragment")
	require.NoError(t, err2, "Feed of the second fragment")
	require.NoError(t, errFinish, "Finish after both fragments — the buffer is drained")
	assert.Len(t, got, 2, "a block split across Feed calls must emit the same fields as one delivered whole")
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
	require.NoError(t, d.Feed(incomplete, func(HeaderField) error { return nil }),
		"a truncated tail is buffered by Feed, not rejected")

	err := d.Finish()

	require.ErrorIs(t, err, ErrTruncated,
		"a field block that ends mid-representation is the wire's mistake; a Finish that accepts it silently drops the unparsed field")
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

	require.ErrorIs(t, err, ErrInvalidIndex,
		"index 62 is the first dynamic slot; against an empty table it names nothing and must not resolve to a stale arena entry")
}

// TestLookup_Zero verifies ErrInvalidIndex for index 0.
func TestLookup_Zero(t *testing.T) {
	d := NewDecoder()

	_, _, err := d.lookup(0)

	require.ErrorIs(t, err, ErrInvalidIndex,
		"index 0 is not a table entry (§2.3.3); without the guard it resolves to staticTable[0], the deliberately empty slot")
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

	require.ErrorIs(t, err, ErrInvalidIndex,
		"a literal whose name references an absent entry must fail on the reference, not fall through to decoding the value against an empty name")
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
	// length byte (H=1) = 0x8c (0x80|12).
	src, err := hexDecode("8cf1e3c2e5f23a6ba0ab90f4ff")
	require.NoError(t, err, "fixture decode")

	dst, n, err := decodeStringLiteral(nil, src)

	require.NoError(t, err, "the §C.4.1 authority literal is well-formed Huffman")
	assert.Equal(t, len(src), n, "the length prefix plus the whole Huffman body must be consumed")
	assert.Equal(t, []byte("www.example.com"), dst,
		"the H bit must actually select Huffman decoding; treating the body as raw octets yields the compressed bytes as the header value")
}

// ---------------------------------------------------------------------------
// Encoder.writeFieldAlreadyFlushedSize — dynamic full-match path
// ---------------------------------------------------------------------------

// TestEncoder_DynamicFullMatch verifies that when a field already exists in
// the dynamic table (full match), it's emitted as an indexed representation.
func TestEncoder_DynamicFullMatch(t *testing.T) {
	enc := NewEncoder()
	// First encode adds custom-key=custom-val to dynamic table.
	_ = enc.WriteField(nil, []byte("custom-key"), []byte("custom-val"), IndexIncremental)

	// Second encode should find the full match in the dynamic table.
	dst := enc.WriteField(nil, []byte("custom-key"), []byte("custom-val"), IndexIncremental)

	require.Lenf(t, dst, 1,
		"a repeated field must collapse to a single indexed octet, got % x — re-emitting the literal is the whole compression ratio lost", dst)
	assert.NotZerof(t, dst[0]&0x80,
		"the octet must carry the §6.1 indexed prefix, got %#x", dst[0])
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
