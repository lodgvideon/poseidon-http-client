package hpack

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// RFC 7541 Appendix C — conformance vectors. Names follow the pattern
// TestConformance_RFC7541_CX_Y so the CI gate can map them to RFC sections.

type fxField struct {
	name, value string
	sensitive   bool
}

func runVector(t *testing.T, name string, blockHex string, want []fxField, tableSize uint32) {
	t.Helper()
	d := NewDecoder()
	if tableSize != 0 {
		d.SetMaxDynamicTableSize(tableSize)
	}
	block, err := hex.DecodeString(blockHex)
	if err != nil {
		t.Fatalf("hex decode: %v", err)
	}
	var got []fxField
	if err := d.DecodeBlock(block, func(f HeaderField) error {
		got = append(got, fxField{name: string(f.Name), value: string(f.Value), sensitive: f.Sensitive})
		return nil
	}); err != nil {
		t.Fatalf("%s decode: %v", name, err)
	}
	if len(got) != len(want) {
		t.Fatalf("%s: len got=%d want=%d (%+v vs %+v)", name, len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s field[%d]: got %+v, want %+v", name, i, got[i], want[i])
		}
	}
}

func runEncodeRoundtrip(t *testing.T, name string, fields []fxField) {
	t.Helper()
	enc := NewEncoder()
	hf := make([]HeaderField, len(fields))
	for i, f := range fields {
		hf[i] = HeaderField{Name: []byte(f.name), Value: []byte(f.value), Sensitive: f.sensitive}
	}
	block := enc.EncodeBlock(nil, hf)
	dec := NewDecoder()
	var got []fxField
	if err := dec.DecodeBlock(block, func(f HeaderField) error {
		got = append(got, fxField{name: string(f.Name), value: string(f.Value), sensitive: f.Sensitive})
		return nil
	}); err != nil {
		t.Fatalf("%s roundtrip decode: %v", name, err)
	}
	if len(got) != len(fields) {
		t.Fatalf("%s roundtrip len mismatch: got %d want %d", name, len(got), len(fields))
	}
	for i := range got {
		if got[i] != fields[i] {
			t.Fatalf("%s roundtrip field[%d]: got %+v, want %+v", name, i, got[i], fields[i])
		}
	}
	_ = bytes.NewReader(nil) // keep import
}

// C.2.1: Literal Header Field with Indexing
func TestConformance_RFC7541_C2_1_LiteralIndexing(t *testing.T) {
	runVector(t, "C.2.1",
		"400a637573746f6d2d6b65790d637573746f6d2d686561646572",
		[]fxField{{name: "custom-key", value: "custom-header"}}, 0)
}

// C.2.2: Literal Header Field without Indexing
func TestConformance_RFC7541_C2_2_LiteralNoIndexing(t *testing.T) {
	runVector(t, "C.2.2",
		"040c2f73616d706c652f70617468",
		[]fxField{{name: ":path", value: "/sample/path"}}, 0)
}

// C.2.3: Literal Header Field Never Indexed
func TestConformance_RFC7541_C2_3_NeverIndexed(t *testing.T) {
	runVector(t, "C.2.3",
		"100870617373776f726406736563726574",
		[]fxField{{name: "password", value: "secret", sensitive: true}}, 0)
}

// C.2.4: Indexed Header Field
func TestConformance_RFC7541_C2_4_Indexed(t *testing.T) {
	runVector(t, "C.2.4", "82",
		[]fxField{{name: ":method", value: "GET"}}, 0)
}

// C.3.1: First request (without Huffman)
func TestConformance_RFC7541_C3_1_FirstRequest(t *testing.T) {
	runVector(t, "C.3.1",
		"828684410f7777772e6578616d706c652e636f6d",
		[]fxField{
			{name: ":method", value: "GET"},
			{name: ":scheme", value: "http"},
			{name: ":path", value: "/"},
			{name: ":authority", value: "www.example.com"},
		}, 0)
}

// C.4.1: First request (with Huffman)
func TestConformance_RFC7541_C4_1_FirstRequestHuffman(t *testing.T) {
	runVector(t, "C.4.1",
		"828684418cf1e3c2e5f23a6ba0ab90f4ff",
		[]fxField{
			{name: ":method", value: "GET"},
			{name: ":scheme", value: "http"},
			{name: ":path", value: "/"},
			{name: ":authority", value: "www.example.com"},
		}, 0)
}

// Encode→decode roundtrip across all the field sequences.
func TestConformance_RFC7541_RoundTrip_C3_FirstRequest(t *testing.T) {
	runEncodeRoundtrip(t, "C.3 roundtrip", []fxField{
		{name: ":method", value: "GET"},
		{name: ":scheme", value: "http"},
		{name: ":path", value: "/"},
		{name: ":authority", value: "www.example.com"},
	})
}

func TestConformance_RFC7541_RoundTrip_RequestSequence(t *testing.T) {
	enc := NewEncoder()
	dec := NewDecoder()
	requests := [][]fxField{
		{
			{name: ":method", value: "GET"},
			{name: ":scheme", value: "http"},
			{name: ":path", value: "/"},
			{name: ":authority", value: "www.example.com"},
		},
		{
			{name: ":method", value: "GET"},
			{name: ":scheme", value: "http"},
			{name: ":path", value: "/"},
			{name: ":authority", value: "www.example.com"},
			{name: "cache-control", value: "no-cache"},
		},
		{
			{name: ":method", value: "GET"},
			{name: ":scheme", value: "https"},
			{name: ":path", value: "/index.html"},
			{name: ":authority", value: "www.example.com"},
			{name: "custom-key", value: "custom-value"},
		},
	}
	for i, req := range requests {
		hf := make([]HeaderField, len(req))
		for j, f := range req {
			hf[j] = HeaderField{Name: []byte(f.name), Value: []byte(f.value)}
		}
		block := enc.EncodeBlock(nil, hf)
		var got []fxField
		if err := dec.DecodeBlock(block, func(f HeaderField) error {
			got = append(got, fxField{name: string(f.Name), value: string(f.Value)})
			return nil
		}); err != nil {
			t.Fatalf("req %d decode: %v", i, err)
		}
		if len(got) != len(req) {
			t.Fatalf("req %d len mismatch: got %d want %d", i, len(got), len(req))
		}
		for j := range got {
			if got[j] != req[j] {
				t.Fatalf("req %d field[%d]: got %+v, want %+v", i, j, got[j], req[j])
			}
		}
	}
}
