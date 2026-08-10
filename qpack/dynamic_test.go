package qpack

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// assertDecodeDyn decodes in against dt and checks the emitted fields.
func assertDecodeDyn(t *testing.T, dec *Decoder, in []byte, dt *DynamicTable, want []hpack.HeaderField) {
	t.Helper()
	var got []hpack.HeaderField
	err := dec.DecodeFieldSection(in, dt, func(name, value []byte) error {
		got = append(got, hpack.HeaderField{
			Name:  append([]byte(nil), name...),
			Value: append([]byte(nil), value...),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("DecodeFieldSection(%x): %v", in, err)
	}
	if len(got) != len(want) {
		t.Fatalf("decoded %d fields, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if !bytes.Equal(got[i].Name, want[i].Name) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Errorf("field %d = {%s: %s}, want {%s: %s}", i,
				got[i].Name, got[i].Value, want[i].Name, want[i].Value)
		}
	}
}

// TestConformance_RFC9204_AppB_DynamicTable replays RFC 9204 Appendix B.2–B.5
// verbatim on one dynamic table: the exact encoder-instruction byte streams are
// applied, then the field sections on the request streams are decoded and the
// decoder-instruction responses are re-encoded, and the table state (insert
// count, size, eviction) is checked against the RFC's annotations at each step.
// This is the golden proof of the absolute/relative/post-base index arithmetic
// and the Required Insert Count reconstruction.
func TestConformance_RFC9204_AppB_DynamicTable(t *testing.T) {
	dt := NewDynamicTable(220) // MaxEntries = floor(220/32) = 6
	dec := NewDecoder()

	// --- B.2: Set Dynamic Table Capacity + two Insert With Name Reference ---
	mustApply(t, dt, []byte{
		0x3f, 0xbd, 0x01, // Set Dynamic Table Capacity = 220
		0xc0, 0x0f, // Insert With Name Reference, static idx 0 (:authority)
		'w', 'w', 'w', '.', 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
		0xc1, 0x0c, // Insert With Name Reference, static idx 1 (:path)
		'/', 's', 'a', 'm', 'p', 'l', 'e', '/', 'p', 'a', 't', 'h',
	})
	wantState(t, dt, "B.2", 2, 106)
	// Request stream 4: RIC=2, Base=0, two Post-Base Indexed field lines.
	assertDecodeDyn(t, dec, []byte{0x03, 0x81, 0x10, 0x11}, dt, []hpack.HeaderField{
		hf(":authority", "www.example.com"),
		hf(":path", "/sample/path"),
	})
	wantBytes(t, "B.2 Section Ack", AppendSectionAcknowledgment(nil, 4), 0x84)

	// --- B.3: Speculative Insert With Literal Name (custom-key=custom-value) ---
	mustApply(t, dt, []byte{
		0x4a, // Insert With Literal Name, name len 10
		'c', 'u', 's', 't', 'o', 'm', '-', 'k', 'e', 'y',
		0x0c, // value len 12
		'c', 'u', 's', 't', 'o', 'm', '-', 'v', 'a', 'l', 'u', 'e',
	})
	wantState(t, dt, "B.3", 3, 160)
	wantBytes(t, "B.3 ICI", AppendInsertCountIncrement(nil, 1), 0x01)

	// --- B.4: Duplicate (rel 2 -> abs 0), decode indexed dynamic + static ---
	mustApply(t, dt, []byte{0x02}) // Duplicate, Relative Index 2
	wantState(t, dt, "B.4", 4, 217)
	// Request stream 8: RIC=4, Base=4; dynamic idx0->abs3, static :path, dynamic idx1->abs2.
	assertDecodeDyn(t, dec, []byte{0x05, 0x00, 0x80, 0xc1, 0x81}, dt, []hpack.HeaderField{
		hf(":authority", "www.example.com"),
		hf(":path", "/"),
		hf("custom-key", "custom-value"),
	})
	wantBytes(t, "B.4 Stream Cancel", AppendStreamCancellation(nil, 8), 0x48)

	// --- B.5: Insert With Name Reference (dynamic rel 1) triggering eviction ---
	mustApply(t, dt, []byte{
		0x81, 0x0d, // Insert With Name Reference, dynamic rel 1 (abs 2 = custom-key)
		'c', 'u', 's', 't', 'o', 'm', '-', 'v', 'a', 'l', 'u', 'e', '2',
	})
	// abs 0 (:authority) evicted; entries are now abs 1..4, size 215.
	wantState(t, dt, "B.5", 5, 215)
	if dt.Len() != 4 {
		t.Fatalf("B.5 len=%d, want 4", dt.Len())
	}
	if _, _, ok := dt.at(0); ok {
		t.Fatalf("B.5: abs 0 should have been evicted")
	}
	wantEntry(t, dt, 4, "custom-key", "custom-value2")
}

// wantState asserts the table's insert count and size at a labelled step.
func wantState(t *testing.T, dt *DynamicTable, label string, insertCount, size uint64) {
	t.Helper()
	if dt.InsertCount() != insertCount {
		t.Fatalf("%s insertCount=%d, want %d", label, dt.InsertCount(), insertCount)
	}
	if dt.Size() != size {
		t.Fatalf("%s size=%d, want %d", label, dt.Size(), size)
	}
}

// wantEntry asserts the absolute-index entry holds (name, value).
func wantEntry(t *testing.T, dt *DynamicTable, abs uint64, name, value string) {
	t.Helper()
	n, v, ok := dt.at(abs)
	if !ok || string(n) != name || string(v) != value {
		t.Fatalf("abs %d = {%s:%s ok=%v}, want {%s:%s}", abs, n, v, ok, name, value)
	}
}

// wantBytes asserts got equals the given bytes.
func wantBytes(t *testing.T, label string, got []byte, want ...byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("%s = %x, want %x", label, got, want)
	}
}

// mustApply applies a full encoder-instruction stream, failing on any error or
// a short consume.
func mustApply(t *testing.T, dt *DynamicTable, enc []byte) {
	t.Helper()
	n, err := dt.ParseEncoderInstructions(enc)
	if err != nil {
		t.Fatalf("ParseEncoderInstructions(%x): %v", enc, err)
	}
	if n != len(enc) {
		t.Fatalf("ParseEncoderInstructions(%x): consumed %d/%d", enc, n, len(enc))
	}
}

// TestConformance_RFC9204_Sec452_IndexedDynamicDecode decodes an Indexed Field
// Line referencing the dynamic table relative to the Base (§4.5.2).
func TestConformance_RFC9204_Sec452_IndexedDynamicDecode(t *testing.T) {
	dt := NewDynamicTable(4096)
	applyInserts(t, dt,
		[]byte{0x3f, 0xe1, 0x1f}, // Set Dynamic Table Capacity = 4096
	)
	// Insert (custom-a=1), (custom-b=2) via literal-name inserts.
	applyInserts(t, dt, insertLiteral("custom-a", "1"), insertLiteral("custom-b", "2"))
	if dt.InsertCount() != 2 {
		t.Fatalf("insertCount=%d, want 2", dt.InsertCount())
	}
	// RIC=2, Base=2 (Sign=0, Delta=0): prefix 03 00. Then Indexed dynamic idx 0
	// -> abs Base(2)-0-1 = 1 = custom-b; idx 1 -> abs 0 = custom-a.
	assertDecodeDyn(t, NewDecoder(), []byte{0x03, 0x00, 0x80, 0x81}, dt, []hpack.HeaderField{
		hf("custom-b", "2"),
		hf("custom-a", "1"),
	})
}

// TestConformance_RFC9204_Sec453_LiteralNameRefDynamicDecode decodes a Literal
// Field Line with a dynamic Name Reference relative to the Base (§4.5.3).
func TestConformance_RFC9204_Sec453_LiteralNameRefDynamicDecode(t *testing.T) {
	dt := NewDynamicTable(4096)
	applyInserts(t, dt, []byte{0x3f, 0xe1, 0x1f})
	applyInserts(t, dt, insertLiteral("x-token", "old"))
	// RIC=1, Base=1 (prefix 02 00). Literal with Name Ref, dynamic (01N=0 T=0),
	// idx 0 -> abs Base(1)-0-1 = 0 = name "x-token"; value "new" (len 3).
	sec := []byte{0x02, 0x00, 0x40, 0x03, 'n', 'e', 'w'}
	assertDecodeDyn(t, NewDecoder(), sec, dt, []hpack.HeaderField{hf("x-token", "new")})
}

// TestConformance_RFC9204_Sec455_PostBaseNameRefDecode decodes a Literal Field
// Line with a Post-Base Name Reference (§4.5.5).
func TestConformance_RFC9204_Sec455_PostBaseNameRefDecode(t *testing.T) {
	dt := NewDynamicTable(4096)
	applyInserts(t, dt, []byte{0x3f, 0xe1, 0x1f})
	applyInserts(t, dt, insertLiteral("x-post", "a"))
	// RIC=1, Base=0 (Sign=1, Delta=0: prefix 02 80). Literal Post-Base Name Ref
	// (0000 N=0, 3-bit idx 0) -> abs Base(0)+0 = 0 name "x-post"; value "b".
	sec := []byte{0x02, 0x80, 0x00, 0x01, 'b'}
	assertDecodeDyn(t, NewDecoder(), sec, dt, []hpack.HeaderField{hf("x-post", "b")})
}

// TestQPACK_RequiredInsertCount_Decode covers the §4.5.1.1 reconstruction,
// including the wraparound past 2*MaxEntries and the rejected encodings.
func TestQPACK_RequiredInsertCount_Decode(t *testing.T) {
	cases := []struct {
		enc, maxEntries, totalInserts uint64
		want                          uint64
		wantErr                       bool
	}{
		{0, 6, 100, 0, false},  // encoded 0 is always RIC 0
		{3, 6, 2, 2, false},    // Appendix B.2
		{5, 6, 4, 4, false},    // Appendix B.4
		{11, 6, 10, 10, false}, // wraparound: (10 mod 12)+1 = 11 -> 10
		{1, 6, 12, 12, false},  // (12 mod 12)+1 = 1 -> 12
		{1, 0, 5, 0, true},     // MaxEntries 0 (nil-table equiv): non-zero invalid
		{13, 6, 4, 0, true},    // encoded > FullRange(12)
	}
	for _, c := range cases {
		got, err := reqInsertCountFromEncoded(c.enc, c.maxEntries, c.totalInserts)
		if c.wantErr {
			if err == nil {
				t.Errorf("reqInsertCount(enc=%d,me=%d,ti=%d) = %d, want error",
					c.enc, c.maxEntries, c.totalInserts, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("reqInsertCount(enc=%d,me=%d,ti=%d) = %d,%v; want %d",
				c.enc, c.maxEntries, c.totalInserts, got, err, c.want)
		}
	}
}

// TestQPACK_DecoderInstructions_Encode checks the §4.4 prefixed-integer encodings
// for stream IDs and increments spanning the single-byte and multi-byte forms.
func TestQPACK_DecoderInstructions_Encode(t *testing.T) {
	if got := AppendSectionAcknowledgment(nil, 4); !bytes.Equal(got, []byte{0x84}) {
		t.Errorf("SectionAck(4) = %x, want 84", got)
	}
	// 7-bit prefix maxes at 127; 200 continues: 0xff, 200-127=73 -> 0x49.
	if got := AppendSectionAcknowledgment(nil, 200); !bytes.Equal(got, []byte{0xff, 0x49}) {
		t.Errorf("SectionAck(200) = %x, want ff49", got)
	}
	if got := AppendStreamCancellation(nil, 8); !bytes.Equal(got, []byte{0x48}) {
		t.Errorf("StreamCancel(8) = %x, want 48", got)
	}
	// 6-bit prefix maxes at 63; 100 continues: 0x7f, 100-63=37 -> 0x25.
	if got := AppendStreamCancellation(nil, 100); !bytes.Equal(got, []byte{0x7f, 0x25}) {
		t.Errorf("StreamCancel(100) = %x, want 7f25", got)
	}
	if got := AppendInsertCountIncrement(nil, 1); !bytes.Equal(got, []byte{0x01}) {
		t.Errorf("ICI(1) = %x, want 01", got)
	}
	if got := AppendInsertCountIncrement(nil, 10); !bytes.Equal(got, []byte{0x0a}) {
		t.Errorf("ICI(10) = %x, want 0a", got)
	}
}

// TestQPACK_EncoderInstructions_Partial feeds an encoder stream one byte at a
// time; ParseEncoderInstructions must consume only whole instructions and leave
// a trailing partial one for the next call.
func TestQPACK_EncoderInstructions_Partial(t *testing.T) {
	full := []byte{
		0x3f, 0xbd, 0x01, // Set Dynamic Table Capacity = 220
		0xc0, 0x0f, // Insert With Name Reference, static 0 (:authority)
		'w', 'w', 'w', '.', 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
	}
	dt := NewDynamicTable(220)
	consumed := 0
	for i := 1; i <= len(full); i++ {
		// Re-parse the whole prefix each grow; already-applied bytes are skipped
		// by starting from `consumed`.
		n, err := dt.ParseEncoderInstructions(full[consumed:i])
		if err != nil {
			t.Fatalf("partial parse at %d: %v", i, err)
		}
		consumed += n
	}
	if consumed != len(full) {
		t.Fatalf("consumed %d, want %d", consumed, len(full))
	}
	if dt.InsertCount() != 1 || dt.Capacity() != 220 {
		t.Fatalf("state: insertCount=%d cap=%d, want 1/220", dt.InsertCount(), dt.Capacity())
	}
	if name, val, ok := dt.at(0); !ok || string(name) != ":authority" || string(val) != "www.example.com" {
		t.Fatalf("abs 0 = {%s:%s ok=%v}", name, val, ok)
	}
}

// TestQPACK_EncoderInstructions_Errors rejects instructions that violate the
// table capacity or reference a missing name.
func TestQPACK_EncoderInstructions_Errors(t *testing.T) {
	t.Run("set_capacity_over_max", func(t *testing.T) {
		dt := NewDynamicTable(100)
		// Set Dynamic Table Capacity = 200 (> max 100): 0x3f, 200-31=169 -> 0xa9 0x01.
		if _, err := dt.ParseEncoderInstructions([]byte{0x3f, 0xa9, 0x01}); !errors.Is(err, ErrEncoderStream) {
			t.Fatalf("err = %v, want ErrEncoderStream", err)
		}
	})
	t.Run("name_ref_static_out_of_range", func(t *testing.T) {
		dt := NewDynamicTable(4096)
		applyInserts(t, dt, []byte{0x3f, 0xe1, 0x1f})
		// Insert With Name Reference, static idx 99 (out of 0..98): 0xc0|63=0xff, 99-63=36 -> 0x24, value len 0.
		if _, err := dt.ParseEncoderInstructions([]byte{0xff, 0x24, 0x00}); !errors.Is(err, ErrEncoderStream) {
			t.Fatalf("err = %v, want ErrEncoderStream", err)
		}
	})
	t.Run("name_ref_dynamic_missing", func(t *testing.T) {
		dt := NewDynamicTable(4096)
		applyInserts(t, dt, []byte{0x3f, 0xe1, 0x1f})
		// Insert With Name Reference, dynamic rel 0, but the table is empty.
		if _, err := dt.ParseEncoderInstructions([]byte{0x80, 0x00}); !errors.Is(err, ErrEncoderStream) {
			t.Fatalf("err = %v, want ErrEncoderStream", err)
		}
	})
	t.Run("entry_larger_than_capacity", func(t *testing.T) {
		dt := NewDynamicTable(64)
		applyInserts(t, dt, []byte{0x3f, 0x21}) // Set capacity = 64
		// Insert With Literal Name of a 40-byte value: entry size > 64.
		big := insertLiteral("x", string(bytes.Repeat([]byte("a"), 40)))
		if _, err := dt.ParseEncoderInstructions(big); !errors.Is(err, ErrEncoderStream) {
			t.Fatalf("err = %v, want ErrEncoderStream", err)
		}
	})
}

// TestQPACK_DecodeDynamic_Errors rejects field sections whose dynamic references
// cannot be resolved against the supplied table.
func TestQPACK_DecodeDynamic_Errors(t *testing.T) {
	dt := NewDynamicTable(4096)
	applyInserts(t, dt, []byte{0x3f, 0xe1, 0x1f}, insertLiteral("a", "b"))
	dec := NewDecoder()
	cases := []struct {
		name string
		in   []byte
	}{
		// RIC=5 but only 1 insert -> unreachable insert count.
		{"ric_exceeds_inserts", []byte{0x06, 0x00, 0x80}},
		// RIC=1 Base=1, Indexed dynamic idx 5 -> abs underflow (Base-5-1).
		{"indexed_base_underflow", []byte{0x02, 0x00, 0x85}},
		// RIC=1 Base=1, Post-Base idx 3 -> abs 4 not present.
		{"post_base_absent", []byte{0x02, 0x00, 0x13}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := dec.DecodeFieldSection(c.in, dt, func([]byte, []byte) error { return nil })
			if !errors.Is(err, ErrDecompressionFailed) {
				t.Fatalf("DecodeFieldSection(%x) = %v, want ErrDecompressionFailed", c.in, err)
			}
		})
	}
}

// TestConformance_RFC9204_Sec212_RequiredInsertCountSmallerThanExpected rejects a
// field section whose declared Required Insert Count is below the lowest value
// with which the section can be decoded. RFC 9204 §2.1.2 fixes that value at the
// largest referenced absolute index plus one, and §2.2.1 makes a smaller count a
// connection error of type QPACK_DECOMPRESSION_FAILED. Every dynamic
// representation must enforce it, so all four are exercised against a table that
// holds the referenced entries — without the bound they would decode happily.
func TestConformance_RFC9204_Sec212_RequiredInsertCountSmallerThanExpected(t *testing.T) {
	dt := NewDynamicTable(4096)
	applyInserts(t, dt, []byte{0x3f, 0xe1, 0x1f},
		insertLiteral("a", "1"), insertLiteral("b", "2"), insertLiteral("c", "3"))
	dec := NewDecoder()
	cases := []struct {
		name string
		in   []byte
	}{
		// RIC=1, Base=3 (Sign=0, DeltaBase=2); Indexed dynamic idx 0 -> abs 2 >= RIC.
		{"indexed", []byte{0x02, 0x02, 0x80}},
		// Same prefix; Literal with dynamic Name Reference idx 0 -> abs 2 >= RIC.
		{"name_ref", []byte{0x02, 0x02, 0x40, 0x00}},
		// RIC=1, Base=0 (Sign=1, DeltaBase=0); Post-Base idx 1 -> abs 1 >= RIC.
		{"post_base", []byte{0x02, 0x80, 0x11}},
		// Same prefix; Post-Base Name Reference idx 1 -> abs 1 >= RIC.
		{"post_base_name_ref", []byte{0x02, 0x80, 0x01, 0x00}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := dec.DecodeFieldSection(c.in, dt, func([]byte, []byte) error { return nil })
			if !errors.Is(err, ErrDecompressionFailed) {
				t.Fatalf("DecodeFieldSection(%x) = %v, want ErrDecompressionFailed", c.in, err)
			}
		})
	}
}

// TestQPACK_StaticOnly_WithTable confirms a section that uses only static and
// literal representations (RIC=0) decodes identically whether a dynamic table is
// present or nil — the INERT path is unaffected.
func TestQPACK_StaticOnly_WithTable(t *testing.T) {
	fields := []hpack.HeaderField{
		hf(":method", "GET"),
		hf(":path", "/index.html"),
		hf("x-custom-header", "custom-value"),
	}
	buf := NewEncoder().EncodeFieldSection(nil, fields)
	// nil table (existing behavior).
	assertDecode(t, buf, fields)
	// non-nil table, but RIC=0 so it is never consulted.
	assertDecodeDyn(t, NewDecoder(), buf, NewDynamicTable(4096), fields)
}

// applyInserts applies each encoder-instruction chunk fully, failing the test on
// error or a short consume.
func applyInserts(t *testing.T, dt *DynamicTable, chunks ...[]byte) {
	t.Helper()
	for _, c := range chunks {
		mustApply(t, dt, c)
	}
}

// insertLiteral builds an Insert With Literal Name instruction (RFC 9204 §4.3.3)
// with raw (non-Huffman) name and value, for building table state in tests.
func insertLiteral(name, value string) []byte {
	dst := hpack.EncodeInteger(nil, 5, 0x40, uint64(len(name))) // 01 H=0, 5-bit name len
	dst = append(dst, name...)
	dst = hpack.EncodeInteger(dst, 7, 0x00, uint64(len(value))) // H=0, 7-bit value len
	return append(dst, value...)
}

// FuzzRequiredInsertCount round-trips the §4.5.1.1 encode/decode over the valid
// window (ReqInsertCount in (TotalInserts-MaxEntries, TotalInserts]); the
// reconstruction must recover the original value across the 2*MaxEntries wrap.
func FuzzRequiredInsertCount(f *testing.F) {
	f.Add(uint64(6), uint64(2), uint64(0))
	f.Add(uint64(6), uint64(10), uint64(0))
	f.Add(uint64(6), uint64(12), uint64(5))
	f.Add(uint64(1), uint64(1), uint64(0))
	f.Fuzz(func(t *testing.T, maxEntries, totalInserts, sel uint64) {
		if maxEntries == 0 || maxEntries > 1<<20 {
			t.Skip()
		}
		if totalInserts > 1<<40 {
			t.Skip()
		}
		d := sel % maxEntries // offset back from totalInserts, in [0, maxEntries-1]
		if d > totalInserts {
			t.Skip()
		}
		ric := totalInserts - d
		if ric == 0 {
			t.Skip()
		}
		enc := encodeRequiredInsertCount(ric, maxEntries)
		got, err := reqInsertCountFromEncoded(enc, maxEntries, totalInserts)
		if err != nil {
			t.Fatalf("decode(enc=%d me=%d ti=%d) err=%v (ric=%d)", enc, maxEntries, totalInserts, err, ric)
		}
		if got != ric {
			t.Fatalf("round trip: got %d want %d (enc=%d me=%d ti=%d)", got, ric, enc, maxEntries, totalInserts)
		}
	})
}

// FuzzDynamicIndexResolution checks the relative<->absolute<->post-base identities
// hold for an arbitrary table: the helper resolvers must agree with a direct
// absolute lookup for every derived index.
func FuzzDynamicIndexResolution(f *testing.F) {
	f.Add(uint64(5), uint64(3), uint64(1))
	f.Add(uint64(1), uint64(0), uint64(0))
	f.Add(uint64(20), uint64(7), uint64(4))
	f.Fuzz(func(t *testing.T, nInsert, baseSel, idxSel uint64) {
		nInsert %= 64
		dt := NewDynamicTable(1 << 20)
		if err := dt.setCapacity(1 << 20); err != nil {
			t.Fatal(err)
		}
		for i := uint64(0); i < nInsert; i++ {
			s := strconv.FormatUint(i, 10)
			if !dt.insert([]byte("n"+s), []byte("v"+s)) {
				break
			}
		}
		n := dt.insertCount
		if n == 0 {
			return
		}
		eq := func(gotN, gotV []byte, gotOK bool, wantN, wantV []byte, wantOK bool) {
			t.Helper()
			if gotOK != wantOK || !bytes.Equal(gotN, wantN) || !bytes.Equal(gotV, wantV) {
				t.Fatalf("mismatch: got {%s:%s ok=%v} want {%s:%s ok=%v}", gotN, gotV, gotOK, wantN, wantV, wantOK)
			}
		}
		// Insert-count-relative == absolute (InsertCount-1-r).
		r := idxSel % n
		gN, gV, gOK := dt.atInsertCountRelative(r)
		wN, wV, wOK := dt.at(n - 1 - r)
		eq(gN, gV, gOK, wN, wV, wOK)

		base := (baseSel % n) + 1 // Base in [1, n]
		// Base-relative == absolute (Base-rel-1).
		rel := idxSel % base
		gN, gV, gOK = dt.atBaseRelative(base, rel)
		wN, wV, wOK = dt.at(base - rel - 1)
		eq(gN, gV, gOK, wN, wV, wOK)

		// Post-base == absolute (Base+post), including out-of-range (both !ok).
		post := idxSel % 8
		gN, gV, gOK = dt.atPostBase(base, post)
		wN, wV, wOK = dt.at(base + post)
		eq(gN, gV, gOK, wN, wV, wOK)
	})
}

// TestQPACK_EncoderInstructions_RingBoundedByCapacity pins the fix for the entry
// ring growing with the peer's INSTRUCTION COUNT rather than with the table's
// capacity (found by FuzzParseEncoderInstructions; the minimised input is the
// committed seed c17449cd555bc213).
//
// A 1-byte Duplicate instruction (RFC 9204 §4.3.4) re-inserts the newest entry,
// which at a capacity of one entry immediately evicts its predecessor: the live
// table never grows, but each insertion used to append a ring slot that eviction
// never reclaimed. That is ~16 bytes of permanent heap per wire byte, unbounded —
// while SETTINGS_QPACK_MAX_TABLE_CAPACITY is precisely the value that is supposed
// to bound it. Reclamation was left to compactArena, which is gated on ARENA bytes
// and so never fires for an entry whose name and value are both empty: such an
// entry costs 32 bytes of §3.2.1 accounting size but zero arena bytes.
func TestQPACK_EncoderInstructions_RingBoundedByCapacity(t *testing.T) {
	const capacity = 32                                  // exactly one empty ("", "") entry: every insert evicts
	instr := hpack.EncodeInteger(nil, 5, 0x20, capacity) // Set Dynamic Table Capacity
	instr = append(instr, 0x40, 0x00)                    // Insert With Literal Name ("", "")
	const duplicates = 4096
	for i := 0; i < duplicates; i++ {
		instr = append(instr, 0x00) // Duplicate, relative index 0
	}

	dt := NewDynamicTable(capacity)
	n, err := dt.ParseEncoderInstructions(instr)
	if err != nil {
		t.Fatalf("ParseEncoderInstructions: %v", err)
	}
	if n != len(instr) {
		t.Fatalf("consumed %d bytes, want all %d", n, len(instr))
	}

	// The logical state stays exactly as the RFC requires: absolute indices are
	// permanent so the insert count tracks every insertion (§3.2.4), while the live
	// window holds the single entry the capacity affords (§3.2.2).
	if got, want := dt.InsertCount(), uint64(1+duplicates); got != want {
		t.Fatalf("insert count %d, want %d", got, want)
	}
	if dt.Len() != 1 {
		t.Fatalf("live entries %d, want 1", dt.Len())
	}
	if dt.Size() > dt.Capacity() {
		t.Fatalf("size %d exceeds capacity %d", dt.Size(), dt.Capacity())
	}
	// The newest entry must still resolve, so bounding the ring did not cost
	// correctness: this is what makes the ring a ring rather than an append-only log.
	if name, val, ok := dt.at(dt.InsertCount() - 1); !ok || len(name) != 0 || len(val) != 0 {
		t.Fatalf("newest entry = {%q:%q ok=%v}, want the empty entry", name, val, ok)
	}
	if _, _, ok := dt.at(0); ok {
		t.Fatalf("absolute index 0 still resolves after %d evictions", duplicates)
	}

	// The point of the fix: storage tracks the capacity, not the instruction count.
	// Before it, this ring held 4097 slots. The bound is deliberately loose and
	// stated in absolute terms rather than against the ring's sizing constant — it
	// pins the property (a 1-entry table keeps a small ring however many
	// instructions arrive), not today's growth policy.
	if len(dt.entries) > 64 {
		t.Fatalf("entry ring grew to %d slots for a 1-entry table after %d instructions; "+
			"storage must track the capacity, not the peer's instruction count",
			len(dt.entries), 1+duplicates)
	}
}
