package hpack

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC7541_Sec622_LiteralWithoutIndexing pins that IndexWithout
// emits the §6.2.2 representation — a 0000 prefix with a 4-bit name index — and
// inserts nothing into the dynamic table.
func TestConformance_RFC7541_Sec622_LiteralWithoutIndexing(t *testing.T) {
	enc := NewEncoder()

	// :method has static index 2, so the name index fits the 4-bit prefix.
	dst := enc.WriteField(nil, []byte(":method"), []byte("VARIES"), IndexWithout)

	require.NotEmpty(t, dst, "the field must be encoded to something")
	assert.Equalf(t, byte(0x02), dst[0],
		"prefix = %#x, want 0x02 (0000 + name index 2); any other prefix tells the peer to parse a different §6.2 representation", dst[0])
	assert.Equal(t, 0, enc.dt.len(),
		"without-indexing must not insert; an insert here evicts entries the peer still indexes and desynchronises the two tables")
}

// TestConformance_RFC7541_Sec622_NoEvictionUnderChurn is the reason the mode
// exists. A field whose value differs on every call — a timeout, a request id —
// encoded with the default IndexIncremental inserts an entry that can never be
// matched again, evicting entries that could be. IndexWithout leaves the table
// untouched.
func TestConformance_RFC7541_Sec622_NoEvictionUnderChurn(t *testing.T) {
	churn := func(mode IndexingMode) int {
		enc := NewEncoder()
		// Seed an entry that later requests would want to keep matching.
		enc.WriteField(nil, []byte("cookie"), []byte("session=stable"), IndexIncremental)
		require.Equal(t, 1, enc.dt.len(), "seed failed: the entry later requests want to keep matching must be in the table")
		for i := 0; i < 200; i++ {
			enc.WriteField(nil, []byte("grpc-timeout"), []byte(strconv.Itoa(i)+"m"), mode)
		}
		return enc.dt.len()
	}

	withoutIndexing := churn(IndexWithout)
	incremental := churn(IndexIncremental)

	assert.Equal(t, 1, withoutIndexing,
		"IndexWithout must leave only the seed; that is the whole point of the mode — a per-request value inserted 200 times evicts everything worth indexing")
	assert.Greaterf(t, incremental, 1,
		"IndexIncremental left dyn table len = %d; the churn baseline must exceed the seed, or the comparison above is measuring nothing", incremental)
}

// TestConformance_RFC7541_Sec622_FullMatchStillIndexed pins that a field that
// matches an existing entry in full is still emitted as an indexed field (§6.1)
// under IndexWithout. Referencing an entry inserts nothing and evicts nothing,
// so it honours the caller's intent while being strictly smaller.
func TestConformance_RFC7541_Sec622_FullMatchStillIndexed(t *testing.T) {
	enc := NewEncoder()

	// :method GET is static index 2 — a full match.
	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), IndexWithout)

	assert.Equalf(t, []byte{0x82}, dst,
		"full static match = %#v, want the single indexed byte 0x82; referencing an entry inserts and evicts nothing, so IndexWithout has no reason to spend a literal", dst)
	assert.Equal(t, 0, enc.dt.len(), "collapsing to an index must not add anything to the table either")
}

// TestConformance_RFC7541_Sec713_NeverIndexedNotCollapsed pins the exception:
// §7.1.3 requires the never-indexed representation be preserved, so a
// never-indexed field is NOT collapsed to an index even on a full match.
func TestConformance_RFC7541_Sec713_NeverIndexedNotCollapsed(t *testing.T) {
	enc := NewEncoder()

	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), IndexNever)

	require.NotEmpty(t, dst, "the field must be encoded to something")
	require.NotEqual(t, 1, len(dst),
		"never-indexed field collapsed to an index; §7.1.3 requires the representation be preserved so an intermediary is told not to index it either")
	assert.Equalf(t, byte(0x10), dst[0]&0xf0,
		"prefix = %#x, want 0001 (never indexed)", dst[0])
}

// TestConformance_RFC7541_Sec622_DecoderReportsMode pins the decode direction:
// a §6.2.2 literal round-trips as IndexWithout, a §6.2.3 one as IndexNever, and
// neither reaches the decoder's dynamic table — so indices the peer assigned do
// not shift.
func TestConformance_RFC7541_Sec622_DecoderReportsMode(t *testing.T) {
	enc := NewEncoder()
	var block []byte
	block = enc.WriteField(block, []byte("custom"), []byte("indexed"), IndexIncremental)
	block = enc.WriteField(block, []byte("grpc-timeout"), []byte("1m"), IndexWithout)
	block = enc.WriteField(block, []byte("authorization"), []byte("Bearer x"), IndexNever)
	dec := NewDecoder()
	var got []HeaderField

	err := dec.DecodeBlock(block, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:     append([]byte(nil), f.Name...),
			Value:    append([]byte(nil), f.Value...),
			Indexing: f.Indexing,
		})
		return nil
	})

	want := []IndexingMode{IndexIncremental, IndexWithout, IndexNever}
	require.NoError(t, err, "decode of a block carrying all three §6.2 representations")
	require.Len(t, got, len(want), "each representation must emit exactly one field")
	for i := range want {
		assert.Equalf(t, want[i], got[i].Indexing,
			"field %d (%q) Indexing = %d, want %d; the representation the peer chose has to survive the decode or a caller cannot forward it faithfully",
			i, got[i].Name, got[i].Indexing, want[i])
	}
	// Only the incrementally-indexed field entered the decoder's table.
	assert.Equal(t, 1, dec.dt.len(),
		"only the §6.2.1 field may enter the decoder's table; inserting the other two shifts every index the peer's encoder assigned")
	assert.Equal(t, "authorization", string(got[2].Name), "the never-indexed field is the third one")
	assert.True(t, got[2].Sensitive(),
		"never-indexed field lost its Sensitive() reading, so an intermediary would be free to index a credential it was told never to")
}

// BenchmarkEncoder_WriteFieldWithoutIndexing keeps the new representation inside
// the package's zero-allocation guarantee.
func BenchmarkEncoder_WriteFieldWithoutIndexing(b *testing.B) {
	enc := NewEncoder()
	dst := make([]byte, 0, 256)
	name, value := []byte("grpc-timeout"), []byte("99999999u")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = enc.WriteField(dst[:0], name, value, IndexWithout)
	}
}
