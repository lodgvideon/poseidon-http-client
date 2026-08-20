package hpack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStaticTable_AppendixA is a SECOND transcription of RFC 7541 Appendix A,
// checked against the first one in static_table.go. It replaced an eight-row
// sample: 53 of the 61 normative rows were asserted by nothing, here or anywhere
// else, and mistyping one is silent and wire-visible — the encoder maps a field
// to that index, the peer resolves the index against its own correct copy, and
// the two sides disagree about which header was sent. No round-trip test can see
// it, because both directions of this codec read the same wrong row.
//
// The rows below were transcribed from the RFC text, deliberately NOT generated
// from the table under test: expectations derived from the subject cannot fail.
// The sibling normative table in this package (huffman_table_test.go) has had
// this treatment since it was written; only the check for this one was missing.
func TestStaticTable_AppendixA(t *testing.T) {
	cases := []struct {
		idx       int
		wantName  string
		wantValue string
	}{
		{1, ":authority", ""},
		{2, ":method", "GET"},
		{3, ":method", "POST"},
		{4, ":path", "/"},
		{5, ":path", "/index.html"},
		{6, ":scheme", "http"},
		{7, ":scheme", "https"},
		{8, ":status", "200"},
		{9, ":status", "204"},
		{10, ":status", "206"},
		{11, ":status", "304"},
		{12, ":status", "400"},
		{13, ":status", "404"},
		{14, ":status", "500"},
		{15, "accept-charset", ""},
		{16, "accept-encoding", "gzip, deflate"},
		{17, "accept-language", ""},
		{18, "accept-ranges", ""},
		{19, "accept", ""},
		{20, "access-control-allow-origin", ""},
		{21, "age", ""},
		{22, "allow", ""},
		{23, "authorization", ""},
		{24, "cache-control", ""},
		{25, "content-disposition", ""},
		{26, "content-encoding", ""},
		{27, "content-language", ""},
		{28, "content-length", ""},
		{29, "content-location", ""},
		{30, "content-range", ""},
		{31, "content-type", ""},
		{32, "cookie", ""},
		{33, "date", ""},
		{34, "etag", ""},
		{35, "expect", ""},
		{36, "expires", ""},
		{37, "from", ""},
		{38, "host", ""},
		{39, "if-match", ""},
		{40, "if-modified-since", ""},
		{41, "if-none-match", ""},
		{42, "if-range", ""},
		{43, "if-unmodified-since", ""},
		{44, "last-modified", ""},
		{45, "link", ""},
		{46, "location", ""},
		{47, "max-forwards", ""},
		{48, "proxy-authenticate", ""},
		{49, "proxy-authorization", ""},
		{50, "range", ""},
		{51, "referer", ""},
		{52, "refresh", ""},
		{53, "retry-after", ""},
		{54, "server", ""},
		{55, "set-cookie", ""},
		{56, "strict-transport-security", ""},
		{57, "transfer-encoding", ""},
		{58, "user-agent", ""},
		{59, "vary", ""},
		{60, "via", ""},
		{61, "www-authenticate", ""},
	}
	require.Len(t, cases, staticTableLen,
		"the fixture itself must carry all 61 Appendix A rows; a short table would assert a sample again, which is the gap this test exists to close")

	for _, tc := range cases {
		e := staticTable[tc.idx]

		assert.Equalf(t, tc.wantName, string(e.name),
			"static index %d name; Appendix A is normative and the peer resolves this index against its own copy, so a mistyped row means the two sides disagree about which header was sent", tc.idx)
		assert.Equalf(t, tc.wantValue, string(e.value),
			"static index %d value; a wrong value here is emitted as an indexed field and read back by the peer as something else", tc.idx)
	}
}

// TestStaticTable_ShapeMatchesAppendixA is the structural half, and needs no
// second copy of the RFC: it is what catches a duplicated or transposed row.
// Every row must be findable as a full match by its own name and value, at its
// own index — two rows that swapped places, or one copied over another, break
// that identity even though each row read on its own still looks plausible.
func TestStaticTable_ShapeMatchesAppendixA(t *testing.T) {
	require.Len(t, staticTable, staticTableLen+1,
		"index 0 is the unused slot and 1..61 are Appendix A; a different length shifts every index a peer sends")
	assert.Truef(t, staticTable[0].name == nil,
		"index 0 must stay empty: HPACK indices are 1-based (§2.3.3), and a name there would be reachable by an index the RFC does not define")

	for i := 1; i <= staticTableLen; i++ {
		e := staticTable[i]

		require.NotEmptyf(t, e.name, "static index %d has no name; Appendix A defines all 61 rows", i)
		idx, full := staticIndex(e.name, e.value)
		assert.Truef(t, full, "row %d (%s) must be findable as a full match by its own name and value, or the encoder spells out a field the table already holds", i, e.name)
		assert.Equalf(t, uint64(i), idx,
			"row %d (%s) resolved to index %d; a row that resolves anywhere but to itself means two rows were duplicated or transposed, and the peer reads the other one", i, e.name, idx)
	}
}

func TestStaticIndex_FullMatch(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		wantIdx     uint64
	}{
		{":method", "GET", 2},
		{":path", "/index.html", 5},
	} {
		t.Run(tc.name+"="+tc.value, func(t *testing.T) {
			name, value := []byte(tc.name), []byte(tc.value)

			idx, full := staticIndex(name, value)

			assert.Truef(t, full, "(%s,%s) must be reported as a FULL match so it collapses to a §6.1 indexed octet", tc.name, tc.value)
			assert.Equalf(t, tc.wantIdx, idx, "(%s,%s) must resolve to Appendix A index %d", tc.name, tc.value, tc.wantIdx)
		})
	}
}

func TestStaticIndex_NameOnlyMatch(t *testing.T) {
	t.Run("name with several values returns the lowest index", func(t *testing.T) {
		// :path appears at 4 ("/") and 5 ("/index.html"); "/foo" matches neither.
		name, value := []byte(":path"), []byte("/foo")

		idx, full := staticIndex(name, value)

		assert.False(t, full, "a name match with an unlisted value is not a full match; emitting an indexed field would send the wrong value")
		assert.Equal(t, uint64(4), idx,
			"a name-only match must return the FIRST (lowest) index carrying that name, which is the convention every HPACK encoder follows")
	})

	t.Run("name with no value returns its own index", func(t *testing.T) {
		name, value := []byte("user-agent"), []byte("anything")

		idx, full := staticIndex(name, value)

		assert.False(t, full, "user-agent carries no value in Appendix A, so nothing can be a full match")
		assert.NotZero(t, idx, "the name index must come back so the literal references it instead of spelling user-agent out on every request")
	})
}

func TestStaticIndex_NoMatch(t *testing.T) {
	name, value := []byte("x-custom"), []byte("v")

	idx, full := staticIndex(name, value)

	assert.False(t, full, "a name absent from Appendix A cannot be a full match")
	assert.Zero(t, idx, "index 0 is what tells the encoder to spell the name out; any other value references an unrelated static row")
}

// The staticIndex benchmarks live in staticindex_bench_test.go, next to the
// fixture rationale that says which of them is representative.
