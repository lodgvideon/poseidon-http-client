package hpack

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// RFC 7541 §C.2.4: indexed header field representation (":method GET").
func TestEncoder_IndexedFromStaticTable(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), IndexIncremental)
	want, _ := hex.DecodeString("82")
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

// RFC 7541 §C.2.1: literal with incremental indexing — new name/value.
func TestEncoder_LiteralWithIncrementalIndexing_NewName(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte("custom-key"), []byte("custom-header"), IndexIncremental)
	if dst[0] != 0x40 {
		t.Fatalf("prefix = %#x, want 0x40", dst[0])
	}
	if enc.dt.len() != 1 {
		t.Fatalf("dyn table len = %d, want 1 after incremental", enc.dt.len())
	}
}

// Sensitive=true must emit Never-Indexed (RFC §6.2.3, prefix 0001 NNNN).
// Using :method (static idx 2) so the index fits in the 4-bit prefix.
func TestEncoder_NeverIndexed_OnSensitive(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte(":method"), []byte("SECRET"), IndexNever)
	if dst[0] != 0x12 {
		t.Fatalf("prefix = %#x, want 0x12", dst[0])
	}
	if enc.dt.len() != 0 {
		t.Fatalf("dyn table len = %d, want 0 for never-indexed", enc.dt.len())
	}
}

func TestEncoder_EncodeBlock_MultipleFields(t *testing.T) {
	enc := NewEncoder()
	fields := []HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	dst := enc.EncodeBlock(nil, fields)
	want, _ := hex.DecodeString("828784")
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

func TestEncoder_Reset_ClearsState(t *testing.T) {
	enc := NewEncoder()
	enc.SetMaxDynamicTableSizeLimit(1024)
	_ = enc.WriteField(nil, []byte("custom-key"), []byte("v"), IndexIncremental)
	if enc.dt.len() == 0 {
		t.Fatal("precondition: dynamic table should have an entry")
	}
	enc.Reset()
	if enc.dt.len() != 0 {
		t.Fatal("Reset must empty dynamic table")
	}
	if enc.localLimit != defaultMaxDynamicTableSize {
		t.Fatalf("localLimit after Reset = %d, want %d", enc.localLimit, defaultMaxDynamicTableSize)
	}
	if enc.hasPendingUpdate {
		t.Fatal("Reset must clear pending size update")
	}
}

func TestEncoder_PendingSizeUpdateEmitted(t *testing.T) {
	enc := NewEncoder()
	enc.SetMaxDynamicTableSizeLimit(512)
	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), IndexIncremental)
	// First byte must be a Dynamic Table Size Update (prefix 001x_xxxx).
	if dst[0]&0xe0 != 0x20 {
		t.Fatalf("expected size update prefix 0x20, got %#x", dst[0])
	}
	// After emit, pending must be cleared.
	if enc.hasPendingUpdate {
		t.Fatal("pending size update flag must clear after emit")
	}
}

func TestEncoder_SetMaxDynamicTableSize_PeerIncreaseHonored(t *testing.T) {
	enc := NewEncoder()
	enc.SetMaxDynamicTableSize(1000)
	if enc.localLimit != 1000 {
		t.Fatalf("after peer 1000, localLimit = %d, want 1000", enc.localLimit)
	}
	enc.SetMaxDynamicTableSize(4096)
	if enc.localLimit != 4096 {
		t.Fatalf("after peer 4096, localLimit = %d, want 4096 (peer increase must lift cap)", enc.localLimit)
	}
}

func TestEncoder_SetMaxDynamicTableSize_CallerLimitWins(t *testing.T) {
	enc := NewEncoder()
	enc.SetMaxDynamicTableSizeLimit(512)
	enc.SetMaxDynamicTableSize(8192)
	if enc.localLimit != 512 {
		t.Fatalf("localLimit = %d, want 512 (caller cap below peer)", enc.localLimit)
	}
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
	if enc.dt.maxSize != 100 {
		t.Fatalf("precondition: table cap = %d, want 100", enc.dt.maxSize)
	}

	enc.Reset()

	if enc.dt.maxSize != defaultMaxDynamicTableSize {
		t.Errorf("table cap after Reset = %d, want %d — the encoder believes it has %d "+
			"bytes to index into while the table evicts at %d",
			enc.dt.maxSize, defaultMaxDynamicTableSize, enc.localLimit, enc.dt.maxSize)
	}
	if enc.localLimit != enc.dt.maxSize {
		t.Errorf("localLimit %d != table cap %d after Reset; every other path keeps them equal",
			enc.localLimit, enc.dt.maxSize)
	}
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

	if enc.dt.len() == 0 {
		t.Error("the table dropped an entry that fits in the default cap — Reset left it " +
			"capped at the discarded caller limit")
	}
}
