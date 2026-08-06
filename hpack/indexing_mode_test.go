package hpack

import (
	"bytes"
	"strconv"
	"testing"
)

// TestConformance_RFC7541_Sec622_LiteralWithoutIndexing pins that IndexWithout
// emits the §6.2.2 representation — a 0000 prefix with a 4-bit name index — and
// inserts nothing into the dynamic table.
func TestConformance_RFC7541_Sec622_LiteralWithoutIndexing(t *testing.T) {
	enc := NewEncoder()
	// :method has static index 2, so the name index fits the 4-bit prefix.
	dst := enc.WriteField(nil, []byte(":method"), []byte("VARIES"), IndexWithout)
	if dst[0] != 0x02 {
		t.Fatalf("prefix = %#x, want 0x02 (0000 + name index 2)", dst[0])
	}
	if enc.dt.len() != 0 {
		t.Fatalf("dyn table len = %d, want 0 — without-indexing must not insert", enc.dt.len())
	}
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
		seeded := enc.dt.len()
		if seeded != 1 {
			t.Fatalf("seed failed: dyn table len = %d, want 1", seeded)
		}
		for i := 0; i < 200; i++ {
			enc.WriteField(nil, []byte("grpc-timeout"), []byte(strconv.Itoa(i)+"m"), mode)
		}
		return enc.dt.len()
	}
	if got := churn(IndexWithout); got != 1 {
		t.Fatalf("IndexWithout left dyn table len = %d, want 1 (only the seed)", got)
	}
	if got := churn(IndexIncremental); got <= 1 {
		t.Fatalf("IndexIncremental left dyn table len = %d; the churn baseline must exceed the seed", got)
	}
}

// TestConformance_RFC7541_Sec622_FullMatchStillIndexed pins that a field that
// matches an existing entry in full is still emitted as an indexed field (§6.1)
// under IndexWithout. Referencing an entry inserts nothing and evicts nothing,
// so it honours the caller's intent while being strictly smaller.
func TestConformance_RFC7541_Sec622_FullMatchStillIndexed(t *testing.T) {
	enc := NewEncoder()
	// :method GET is static index 2 — a full match.
	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), IndexWithout)
	if len(dst) != 1 || dst[0] != 0x82 {
		t.Fatalf("full static match = %#v, want single indexed byte 0x82", dst)
	}
	if enc.dt.len() != 0 {
		t.Fatalf("dyn table len = %d, want 0", enc.dt.len())
	}
}

// TestConformance_RFC7541_Sec713_NeverIndexedNotCollapsed pins the exception:
// §7.1.3 requires the never-indexed representation be preserved, so a
// never-indexed field is NOT collapsed to an index even on a full match.
func TestConformance_RFC7541_Sec713_NeverIndexedNotCollapsed(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), IndexNever)
	if len(dst) == 1 {
		t.Fatal("never-indexed field collapsed to an index; §7.1.3 requires the representation be preserved")
	}
	if dst[0]&0xf0 != 0x10 {
		t.Fatalf("prefix = %#x, want 0001 (never indexed)", dst[0])
	}
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

	var got []HeaderField
	dec := NewDecoder()
	if err := dec.DecodeBlock(block, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:     append([]byte(nil), f.Name...),
			Value:    append([]byte(nil), f.Value...),
			Indexing: f.Indexing,
		})
		return nil
	}); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := []IndexingMode{IndexIncremental, IndexWithout, IndexNever}
	if len(got) != len(want) {
		t.Fatalf("decoded %d fields, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Indexing != want[i] {
			t.Errorf("field %d (%q) Indexing = %d, want %d", i, got[i].Name, got[i].Indexing, want[i])
		}
	}
	// Only the incrementally-indexed field entered the decoder's table.
	if dec.dt.len() != 1 {
		t.Fatalf("decoder dyn table len = %d, want 1", dec.dt.len())
	}
	if !bytes.Equal(got[2].Name, []byte("authorization")) || !got[2].Sensitive() {
		t.Fatal("never-indexed field lost its Sensitive() reading")
	}
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
