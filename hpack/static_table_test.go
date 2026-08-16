package hpack

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Sample static table entries from RFC 7541 App. A.
func TestStaticTable_KnownEntries(t *testing.T) {
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
		{8, ":status", "200"},
		{16, "accept-encoding", "gzip, deflate"},
		{61, "www-authenticate", ""},
	}
	for _, tc := range cases {
		e := staticTable[tc.idx]

		assert.Equalf(t, tc.wantName, string(e.name),
			"static index %d name; Appendix A is normative and the peer resolves this index against its own copy, so a mistyped row means the two sides disagree about which header was sent", tc.idx)
		assert.Equalf(t, tc.wantValue, string(e.value),
			"static index %d value; a wrong value here is emitted as an indexed field and read back by the peer as something else", tc.idx)
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
