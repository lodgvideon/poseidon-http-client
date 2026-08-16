package hpack

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RFC 7541 §C.2.4: indexed header field representation (":method GET").
func TestEncoder_IndexedFromStaticTable(t *testing.T) {
	enc := NewEncoder()
	want, err := hex.DecodeString("82")
	require.NoError(t, err, "hex decode of the expected wire bytes")

	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), IndexIncremental)

	assert.Equal(t, want, dst,
		"a full static-table match must collapse to the §6.1 indexed octet; emitting a literal instead is the whole compression ratio lost on the commonest field there is")
}

// RFC 7541 §C.2.1: literal with incremental indexing — new name/value.
func TestEncoder_LiteralWithIncrementalIndexing_NewName(t *testing.T) {
	enc := NewEncoder()

	dst := enc.WriteField(nil, []byte("custom-key"), []byte("custom-header"), IndexIncremental)

	require.NotEmpty(t, dst, "the field must be encoded to something")
	assert.Equalf(t, byte(0x40), dst[0],
		"prefix = %#x; §6.2.1 with an unknown name is 0x40 (6-bit prefix, index 0), and any other prefix tells the peer to parse a different representation", dst[0])
	assert.Equal(t, 1, enc.dt.len(),
		"IndexIncremental must insert; skipping the insert means the next occurrence cannot be indexed and the peer's table drifts from ours")
}

// Sensitive=true must emit Never-Indexed (RFC §6.2.3, prefix 0001 NNNN).
// Using :method (static idx 2) so the index fits in the 4-bit prefix.
func TestEncoder_NeverIndexed_OnSensitive(t *testing.T) {
	enc := NewEncoder()

	dst := enc.WriteField(nil, []byte(":method"), []byte("SECRET"), IndexNever)

	require.NotEmpty(t, dst, "the field must be encoded to something")
	assert.Equalf(t, byte(0x12), dst[0],
		"prefix = %#x; §6.2.3 never-indexed with name index 2 is 0x12, and §7.1.3 requires that representation be preserved rather than downgraded", dst[0])
	assert.Equal(t, 0, enc.dt.len(),
		"a never-indexed field must not enter the table; indexing it would hand an intermediary the very value it was told never to index")
}

func TestEncoder_EncodeBlock_MultipleFields(t *testing.T) {
	enc := NewEncoder()
	fields := []HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	want, err := hex.DecodeString("828784")
	require.NoError(t, err, "hex decode of the expected wire bytes")

	dst := enc.EncodeBlock(nil, fields)

	assert.Equal(t, want, dst,
		"three fully-static fields must encode to three indexed octets and nothing else; extra bytes here are emitted on every single request")
}

func TestEncoder_Reset_ClearsState(t *testing.T) {
	enc := NewEncoder()
	enc.SetMaxDynamicTableSizeLimit(1024)
	_ = enc.WriteField(nil, []byte("custom-key"), []byte("v"), IndexIncremental)
	require.NotZero(t, enc.dt.len(), "precondition: dynamic table should have an entry")

	enc.Reset()

	assert.Equal(t, 0, enc.dt.len(), "Reset must empty the dynamic table so the encoder can be reused on a new connection")
	assert.Equal(t, defaultMaxDynamicTableSize, enc.localLimit,
		"Reset means NewEncoder: the peer's advertised limit and the caller's cap are both discarded")
	assert.False(t, enc.hasPendingUpdate,
		"Reset must clear the pending size update, or the next block opens with a §6.3 update describing a limit from the previous connection")
}

func TestEncoder_PendingSizeUpdateEmitted(t *testing.T) {
	enc := NewEncoder()
	enc.SetMaxDynamicTableSizeLimit(512)

	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), IndexIncremental)

	require.NotEmpty(t, dst, "the field must be encoded to something")
	// First byte must be a Dynamic Table Size Update (prefix 001x_xxxx).
	assert.Equalf(t, byte(0x20), dst[0]&0xe0,
		"first byte %#x must carry the §6.3 size-update prefix; §4.2 requires the update to reach the peer before we index against the new size", dst[0])
	assert.False(t, enc.hasPendingUpdate,
		"the pending flag must clear on emit, or every later block repeats the same size update")
}

func TestEncoder_SetMaxDynamicTableSize_PeerIncreaseHonored(t *testing.T) {
	enc := NewEncoder()

	enc.SetMaxDynamicTableSize(1000)
	got1000 := enc.localLimit
	enc.SetMaxDynamicTableSize(4096)
	got4096 := enc.localLimit

	assert.Equal(t, uint32(1000), got1000, "the peer's advertised limit becomes the effective limit")
	assert.Equal(t, uint32(4096), got4096,
		"a peer INCREASE must lift the cap; capping at the first value seen leaves the compression ratio degraded for the connection's whole life")
}

func TestEncoder_SetMaxDynamicTableSize_CallerLimitWins(t *testing.T) {
	enc := NewEncoder()

	enc.SetMaxDynamicTableSizeLimit(512)
	enc.SetMaxDynamicTableSize(8192)

	assert.Equal(t, uint32(512), enc.localLimit,
		"the effective limit is min(peer, caller); taking the peer's larger value ignores the cap the caller asked for")
}

func BenchmarkEncoder_EncodeBlock_3req_static(b *testing.B) {
	enc := NewEncoder()
	fields := []HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = enc.EncodeBlock(dst[:0], fields)
	}
	_ = dst
}

// TestEncoder_Reset_RestoresTableCap is the gate for the invariant every other
// path maintains: localLimit and the table's own cap must agree.
//
// dynamicTable.clear resets the entries and the arena but not maxSize, so Reset
// setting localLimit back to the default was not enough — the encoder went on
// indexing against a budget of 4096 into a table that still evicted at the old
// cap. Nothing reports that: compression just gets worse for the life of the
// connection, and no size update is pending to explain it.
func TestEncoder_Reset_RestoresTableCap(t *testing.T) {
	enc := NewEncoder()
	enc.SetMaxDynamicTableSizeLimit(100)
	require.Equal(t, uint32(100), enc.dt.maxSize, "precondition: the caller cap reached the table")

	enc.Reset()

	assert.Equalf(t, defaultMaxDynamicTableSize, enc.dt.maxSize,
		"table cap after Reset = %d, want %d — the encoder believes it has %d bytes to index into while the table evicts at %d",
		enc.dt.maxSize, defaultMaxDynamicTableSize, enc.localLimit, enc.dt.maxSize)
	assert.Equalf(t, enc.dt.maxSize, enc.localLimit,
		"localLimit %d != table cap %d after Reset; every other path keeps them equal",
		enc.localLimit, enc.dt.maxSize)
}

// TestEncoder_Reset_TableActuallyHoldsTheDefault is the behavioural half. The
// field check above can be satisfied by setting a number; this checks the table
// really does accept entries again, which is what the caller notices.
func TestEncoder_Reset_TableActuallyHoldsTheDefault(t *testing.T) {
	enc := NewEncoder()
	enc.SetMaxDynamicTableSizeLimit(64) // too small for the entry below
	enc.Reset()
	// ~90 bytes with the 32-byte per-entry overhead: it does not fit in 64 and
	// does fit in 4096, so whether it is retained says which cap is in force.
	name := []byte("x-a-reasonably-long-header-name")
	value := []byte("a-value-of-some-length-too")

	_ = enc.WriteField(nil, name, value, IndexIncremental)

	assert.NotZero(t, enc.dt.len(),
		"the table dropped an entry that fits in the default cap — Reset left it capped at the discarded caller limit")
}
