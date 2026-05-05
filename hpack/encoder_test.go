package hpack

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// RFC 7541 §C.2.4: indexed header field representation (":method GET").
func TestEncoder_IndexedFromStaticTable(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), false)
	want, _ := hex.DecodeString("82")
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

// RFC 7541 §C.2.1: literal with incremental indexing — new name/value.
func TestEncoder_LiteralWithIncrementalIndexing_NewName(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte("custom-key"), []byte("custom-header"), false)
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
	dst := enc.WriteField(nil, []byte(":method"), []byte("SECRET"), true)
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
