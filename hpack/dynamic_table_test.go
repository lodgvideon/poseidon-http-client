package hpack

import (
	"bytes"
	"testing"
)

func TestDynamicTable_AddAndAt(t *testing.T) {
	dt := newDynamicTable(4096)
	dt.add([]byte("custom-key"), []byte("custom-header"))
	if dt.len() != 1 {
		t.Fatalf("len = %d, want 1", dt.len())
	}
	name, value := dt.at(1)
	if !bytes.Equal(name, []byte("custom-key")) || !bytes.Equal(value, []byte("custom-header")) {
		t.Fatalf("at(1) = (%q, %q), want (custom-key, custom-header)", name, value)
	}
	if dt.byteSize() != uint32(10+13+32) {
		t.Fatalf("byteSize = %d, want %d", dt.byteSize(), 10+13+32)
	}
}

func TestDynamicTable_FIFOAddOrder(t *testing.T) {
	dt := newDynamicTable(4096)
	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))
	dt.add([]byte("c"), []byte("3"))
	got := func(i int) string {
		n, v := dt.at(i)
		return string(n) + "=" + string(v)
	}
	if got(1) != "c=3" || got(2) != "b=2" || got(3) != "a=1" {
		t.Fatalf("ordering wrong: 1=%s, 2=%s, 3=%s", got(1), got(2), got(3))
	}
}

func TestDynamicTable_EvictOnSize(t *testing.T) {
	// Each entry: 1+1+32 = 34 bytes. Capacity 70 holds 2.
	dt := newDynamicTable(70)
	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))
	dt.add([]byte("c"), []byte("3")) // evicts oldest (a=1)
	if dt.len() != 2 {
		t.Fatalf("len = %d, want 2", dt.len())
	}
	n, v := dt.at(2)
	if string(n) != "b" || string(v) != "2" {
		t.Fatalf("oldest should be b=2, got %s=%s", n, v)
	}
}

func TestDynamicTable_AddOversizedClearsAll(t *testing.T) {
	dt := newDynamicTable(50)
	dt.add([]byte("x"), []byte("1"))
	bigVal := make([]byte, 100)
	dt.add([]byte("big"), bigVal)
	if dt.len() != 0 {
		t.Fatalf("len = %d, want 0 (oversized add clears)", dt.len())
	}
}

func TestDynamicTable_SetMaxSizeShrinks(t *testing.T) {
	dt := newDynamicTable(200)
	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))
	dt.add([]byte("c"), []byte("3"))
	dt.setMaxSize(35) // holds at most 1 entry of size 34
	if dt.len() != 1 {
		t.Fatalf("len after shrink = %d, want 1", dt.len())
	}
}
