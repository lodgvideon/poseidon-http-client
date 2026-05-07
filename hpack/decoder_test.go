package hpack

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func collectFields(t *testing.T, d *Decoder, blockHex string) []HeaderField {
	t.Helper()
	block, _ := hex.DecodeString(blockHex)
	var got []HeaderField
	err := d.DecodeBlock(block, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:      append([]byte{}, f.Name...),
			Value:     append([]byte{}, f.Value...),
			Sensitive: f.Sensitive,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("decode err: %v", err)
	}
	return got
}

// RFC 7541 §C.2.1.
func TestDecoder_LiteralIncrementalIndexing(t *testing.T) {
	d := NewDecoder()
	got := collectFields(t, d, "400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	want := HeaderField{Name: []byte("custom-key"), Value: []byte("custom-header")}
	if len(got) != 1 || !bytes.Equal(got[0].Name, want.Name) || !bytes.Equal(got[0].Value, want.Value) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if d.dt.len() != 1 {
		t.Fatalf("dyn table len = %d, want 1", d.dt.len())
	}
}

// RFC 7541 §C.2.2.
func TestDecoder_LiteralWithoutIndexing(t *testing.T) {
	d := NewDecoder()
	got := collectFields(t, d, "040c2f73616d706c652f70617468")
	want := HeaderField{Name: []byte(":path"), Value: []byte("/sample/path")}
	if !bytes.Equal(got[0].Name, want.Name) || !bytes.Equal(got[0].Value, want.Value) {
		t.Fatalf("got %+v, want %+v", got[0], want)
	}
	if d.dt.len() != 0 {
		t.Fatalf("dyn table modified by literal-without-indexing")
	}
}

// RFC 7541 §C.2.3: never-indexed with literal name "password" + value "secret".
func TestDecoder_LiteralNeverIndexed(t *testing.T) {
	d := NewDecoder()
	got := collectFields(t, d, "100870617373776f72640673656372657420")
	if !got[0].Sensitive {
		t.Fatalf("Sensitive flag not set on never-indexed")
	}
}

// RFC 7541 §C.2.4: indexed header field.
func TestDecoder_IndexedHeaderField(t *testing.T) {
	d := NewDecoder()
	got := collectFields(t, d, "82")
	if string(got[0].Name) != ":method" || string(got[0].Value) != "GET" {
		t.Fatalf("got %q=%q, want :method=GET", got[0].Name, got[0].Value)
	}
}

func BenchmarkDecoder_DecodeBlock_3req_static(b *testing.B) {
	d := NewDecoder()
	block, _ := hex.DecodeString("828784")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.DecodeBlock(block, func(f HeaderField) error { return nil })
	}
}

func TestDecoder_Streaming_SplitMidField(t *testing.T) {
	full, _ := hex.DecodeString("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	for splitAt := 1; splitAt < len(full); splitAt++ {
		d := NewDecoder()
		d.Begin()
		var got []HeaderField
		visit := func(f HeaderField) error {
			got = append(got, HeaderField{
				Name:  append([]byte{}, f.Name...),
				Value: append([]byte{}, f.Value...),
			})
			return nil
		}
		if err := d.Feed(full[:splitAt], visit); err != nil {
			t.Fatalf("split=%d feed1: %v", splitAt, err)
		}
		if err := d.Feed(full[splitAt:], visit); err != nil {
			t.Fatalf("split=%d feed2: %v", splitAt, err)
		}
		if err := d.Finish(); err != nil {
			t.Fatalf("split=%d finish: %v", splitAt, err)
		}
		if len(got) != 1 || string(got[0].Name) != "custom-key" || string(got[0].Value) != "custom-header" {
			t.Fatalf("split=%d got %+v", splitAt, got)
		}
	}
}

func TestDecoder_Streaming_FinishWithoutBegin(t *testing.T) {
	d := NewDecoder()
	if err := d.Finish(); err == nil {
		t.Fatalf("Finish without Begin should error")
	}
}

func TestDecoder_MaxHeaderListSize_RejectsOversize(t *testing.T) {
	// Encode 4 fields (custom-key, custom-header) — RFC §C.2.1 layout
	// produces 26 bytes per. Each field's HeaderField.Size() is
	// len(name)+len(value)+32 = 10+13+32 = 55. Total 4 fields = 220.
	d := NewDecoder()
	d.SetMaxHeaderListSize(100) // less than 4 × 55
	enc := NewEncoder()
	var buf []byte
	for i := 0; i < 4; i++ {
		buf = enc.EncodeBlock(buf, []HeaderField{
			{Name: []byte("custom-key"), Value: []byte("custom-header")},
		})
	}
	err := d.DecodeBlock(buf, func(HeaderField) error { return nil })
	if err != ErrHeaderListTooLarge {
		t.Fatalf("err = %v, want ErrHeaderListTooLarge", err)
	}
}

func TestDecoder_MaxHeaderListSize_ZeroDisablesGate(t *testing.T) {
	d := NewDecoder()
	enc := NewEncoder()
	buf := enc.EncodeBlock(nil, []HeaderField{
		{Name: []byte("k"), Value: []byte("v")},
	})
	if err := d.DecodeBlock(buf, func(HeaderField) error { return nil }); err != nil {
		t.Fatalf("DecodeBlock with maxListSize=0 should pass: %v", err)
	}
}
