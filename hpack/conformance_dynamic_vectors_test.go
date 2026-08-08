package hpack

import (
	"encoding/hex"
	"testing"
)

// RFC 7541 Appendix C.3 and C.4, the requests AFTER the first.
//
// The suite already replayed C.3.1 and C.4.1 — and stopped there. Those two
// blocks contain no dynamic-table reference at all: every field is either a
// static index or a literal. The first indexed reference into the dynamic table
// appears in the SECOND request of each sequence, as the octet 0xbe (index 62),
// and again as 0xbf (index 63) in the third.
//
// That octet is the only external check on how this package numbers the two
// tables. The encoder emits a dynamic entry as dynIdx+staticTableLen and the
// decoder subtracts the same constant, so a symmetric off-by-one round-trips
// perfectly through our own code and produces a block no other HTTP/2
// implementation can read. Nothing here was pinning it.
//
// A fresh decoder per block cannot express this: the whole point is the table
// state each request leaves for the next, so these replay the sequence through
// ONE decoder, in order, exactly as a connection would.

// runSequence decodes consecutive blocks with a single decoder, so each block
// sees the dynamic table the previous one built.
func runSequence(t *testing.T, name string, steps []struct {
	hexBlock string
	want     []fxField
},
) {
	t.Helper()
	d := NewDecoder()
	for i, step := range steps {
		block, err := hex.DecodeString(step.hexBlock)
		if err != nil {
			t.Fatalf("%s step %d: hex decode: %v", name, i+1, err)
		}
		var got []fxField
		if err := d.DecodeBlock(block, func(f HeaderField) error {
			got = append(got, fxField{name: string(f.Name), value: string(f.Value), sensitive: f.Sensitive()})
			return nil
		}); err != nil {
			t.Fatalf("%s step %d: decode: %v", name, i+1, err)
		}
		if len(got) != len(step.want) {
			t.Fatalf("%s step %d: %d fields, want %d (%+v)", name, i+1, len(got), len(step.want), got)
		}
		for j := range got {
			if got[j] != step.want[j] {
				t.Fatalf("%s step %d field[%d]: got %+v, want %+v;\n"+
					"a dynamic-table index that decodes to the wrong field means this codec numbers "+
					"the static and dynamic tables differently from every peer", name, i+1, j, got[j], step.want[j])
			}
		}
	}
}

type seqStep = struct {
	hexBlock string
	want     []fxField
}

var c3Common = []fxField{
	{name: ":method", value: "GET"},
	{name: ":scheme", value: "http"},
	{name: ":path", value: "/"},
	{name: ":authority", value: "www.example.com"},
}

// TestConformance_RFC7541_C3_RequestSequence replays C.3.1 through C.3.3 in
// order (no Huffman). C.3.2's 0xbe and C.3.3's 0xbf are the dynamic-table
// references this file exists for.
func TestConformance_RFC7541_C3_RequestSequence(t *testing.T) {
	runSequence(t, "C.3", []seqStep{
		{
			hexBlock: "828684410f7777772e6578616d706c652e636f6d",
			want:     c3Common,
		},
		{
			hexBlock: "828684be58086e6f2d6361636865",
			want: append(append([]fxField(nil), c3Common...),
				fxField{name: "cache-control", value: "no-cache"}),
		},
		{
			hexBlock: "828785bf400a637573746f6d2d6b65790c637573746f6d2d76616c7565",
			want: []fxField{
				{name: ":method", value: "GET"},
				{name: ":scheme", value: "https"},
				{name: ":path", value: "/index.html"},
				{name: ":authority", value: "www.example.com"},
				{name: "custom-key", value: "custom-value"},
			},
		},
	})
}

// TestConformance_RFC7541_C4_RequestSequenceHuffman replays C.4.1 through
// C.4.3, the same three requests with Huffman-coded literals. The indexed
// references are identical — Huffman changes only how the literals are spelled,
// so this pins that the table indexing is unaffected by the string coding.
func TestConformance_RFC7541_C4_RequestSequenceHuffman(t *testing.T) {
	runSequence(t, "C.4", []seqStep{
		{
			hexBlock: "828684418cf1e3c2e5f23a6ba0ab90f4ff",
			want:     c3Common,
		},
		{
			hexBlock: "828684be5886a8eb10649cbf",
			want: append(append([]fxField(nil), c3Common...),
				fxField{name: "cache-control", value: "no-cache"}),
		},
		{
			hexBlock: "828785bf408825a849e95ba97d7f8925a849e95bb8e8b4bf",
			want: []fxField{
				{name: ":method", value: "GET"},
				{name: ":scheme", value: "https"},
				{name: ":path", value: "/index.html"},
				{name: ":authority", value: "www.example.com"},
				{name: "custom-key", value: "custom-value"},
			},
		},
	})
}
