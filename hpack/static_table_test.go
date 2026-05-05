package hpack

import (
	"testing"
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
		if string(e.name) != tc.wantName {
			t.Fatalf("idx %d: name = %q, want %q", tc.idx, e.name, tc.wantName)
		}
		if string(e.value) != tc.wantValue {
			t.Fatalf("idx %d: value = %q, want %q", tc.idx, e.value, tc.wantValue)
		}
	}
}

func TestStaticIndex_FullMatch(t *testing.T) {
	idx, full := staticIndex([]byte(":method"), []byte("GET"))
	if !full || idx != 2 {
		t.Fatalf("(:method,GET) = (%d, %v), want (2, true)", idx, full)
	}
	idx, full = staticIndex([]byte(":path"), []byte("/index.html"))
	if !full || idx != 5 {
		t.Fatalf("(:path,/index.html) = (%d, %v), want (5, true)", idx, full)
	}
}

func TestStaticIndex_NameOnlyMatch(t *testing.T) {
	idx, full := staticIndex([]byte(":path"), []byte("/foo"))
	if full || idx != 4 { // first ":path" entry
		t.Fatalf("(:path,/foo) = (%d, %v), want (4, false)", idx, full)
	}
	idx, full = staticIndex([]byte("user-agent"), []byte("anything"))
	if full {
		t.Fatalf("user-agent should not be a full match")
	}
	if idx == 0 {
		t.Fatalf("user-agent should match name-only")
	}
}

func TestStaticIndex_NoMatch(t *testing.T) {
	idx, full := staticIndex([]byte("x-custom"), []byte("v"))
	if idx != 0 || full {
		t.Fatalf("unknown name returned (%d, %v)", idx, full)
	}
}

func BenchmarkStaticIndex_Hit(b *testing.B) {
	name := []byte(":method")
	value := []byte("GET")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = staticIndex(name, value)
	}
}
