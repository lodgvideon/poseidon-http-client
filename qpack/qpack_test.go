package qpack

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hf(name, value string) header.Field {
	return header.Field{Name: []byte(name), Value: []byte(value)}
}

// TestStaticTable_Shape checks the table is the full 99 entries with the
// RFC 9204 Appendix A anchor indices.
func TestStaticTable_Shape(t *testing.T) {
	anchors := map[int]staticEntry{
		0:  {":authority", ""},
		1:  {":path", "/"},
		17: {":method", "GET"},
		20: {":method", "POST"},
		23: {":scheme", "https"},
		25: {":status", "200"},
		98: {"x-frame-options", "sameorigin"},
	}

	size := len(staticTable)

	require.Equal(t, 99, size,
		"RFC 9204 Appendix A fixes the static table at 99 entries; a different length moves every index a peer sends")
	for i, want := range anchors {
		assert.Equalf(t, want, staticTable[i],
			"staticTable[%d] is an Appendix A anchor index; a shift here silently rewrites the header behind that index and every one after it", i)
	}
}

// TestConformance_RFC9204_Sec45_StaticIndexedEncode encodes a request whose
// every field is an exact static-table entry; the wire bytes are fully
// determined (Encoded Field Section Prefix 0x00 0x00 + one Indexed Field Line
// per field, §4.5.1–§4.5.2).
func TestConformance_RFC9204_Sec45_StaticIndexedEncode(t *testing.T) {
	fields := []header.Field{
		hf(":method", "GET"),   // static 17 -> 0xc0|17 = 0xd1
		hf(":path", "/"),       // static 1  -> 0xc1
		hf(":scheme", "https"), // static 23 -> 0xd7
		hf(":authority", ""),   // static 0  -> 0xc0
	}
	want := []byte{0x00, 0x00, 0xd1, 0xc1, 0xd7, 0xc0}

	got := NewEncoder().EncodeFieldSection(nil, fields)

	require.Equal(t, want, got,
		"with every field an exact static match the section is fully determined by §4.5.1–§4.5.2; any other byte string is a representation the peer resolves to different headers")
}

// TestConformance_RFC9204_Sec454_LiteralNameRefDecode decodes a Literal Field
// Line with a static Name Reference and a raw value (§4.5.4).
func TestConformance_RFC9204_Sec454_LiteralNameRefDecode(t *testing.T) {
	// prefix 00 00 | 0x50 (name ref static idx 0 = :authority) | value "example.com"
	// raw: H=0, len=11 -> 0x0b, then the octets.
	in := []byte{0x00, 0x00, 0x50, 0x0b, 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm'}
	want := []header.Field{hf(":authority", "example.com")}

	assertDecode(t, in, want)
}

// TestConformance_RFC9204_Sec456_LiteralNameDecode decodes a Literal Field Line
// with a raw literal name and value (§4.5.6).
func TestConformance_RFC9204_Sec456_LiteralNameDecode(t *testing.T) {
	// prefix 00 00 | 0x26 (001 N=0 H=0, name len 6) "x-test" | 0x01 (value len 1) "1"
	in := []byte{0x00, 0x00, 0x26, 'x', '-', 't', 'e', 's', 't', 0x01, '1'}
	want := []header.Field{hf("x-test", "1")}

	assertDecode(t, in, want)
}

// TestQPACK_RoundTrip exercises indexed, name-reference, and literal-name
// representations (with Huffman on the longer values) through encode then
// decode, and checks the fields survive.
func TestQPACK_RoundTrip(t *testing.T) {
	fields := []header.Field{
		hf(":method", "GET"),                          // indexed
		hf(":path", "/index.html"),                    // name ref (:path)
		hf(":scheme", "https"),                        // indexed
		hf(":authority", "www.example.com"),           // name ref (:authority)
		hf("accept", "*/*"),                           // indexed
		hf("user-agent", "poseidon-http-client/0.8"),  // name ref (user-agent)
		hf("x-custom-header", "custom-value-1234567"), // literal name + value
		hf("content-type", "application/json"),        // indexed
	}

	buf := NewEncoder().EncodeFieldSection(nil, fields)

	assertDecode(t, buf, fields)
}

// TestQPACK_EmptySection round-trips a zero-field section (just the prefix).
func TestQPACK_EmptySection(t *testing.T) {
	buf := NewEncoder().EncodeFieldSection(nil, nil)

	require.Equal(t, []byte{0x00, 0x00}, buf,
		"a zero-field section is exactly the §4.5.1 prefix (Required Insert Count 0, Base 0); a trailing byte would be read as a field line")
	assertDecode(t, buf, nil)
}

// TestConformance_RFC9204_Sec45_DecodeErrors rejects malformed and
// dynamic-table-referencing sections (unsupported at capacity 0) as
// ErrDecompressionFailed.
func TestConformance_RFC9204_Sec45_DecodeErrors(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"required_insert_count_nonzero", []byte{0x01, 0x00}},
		{"base_sign_bit_set", []byte{0x00, 0x80}}, // §4.5.1.2: Sign=1 with RIC 0 → negative Base
		{"indexed_dynamic_T0", []byte{0x00, 0x00, 0x80}},
		{"name_ref_dynamic_T0", []byte{0x00, 0x00, 0x40}},
		{"indexed_post_base", []byte{0x00, 0x00, 0x10}},
		{"literal_post_base_name_ref", []byte{0x00, 0x00, 0x00}},
		{"static_index_out_of_range", []byte{0x00, 0x00, 0xff, 0x24}}, // indexed idx 99
		// The Name Reference twin of the case above (§4.5.3): a 4-bit-prefix index
		// 0x50|15 = 0x5f then 99-15 = 84 (0x54), followed by an empty value literal.
		// Index 99 is one past the last Appendix A row, and this guard is what stands
		// between a peer-chosen index and a read one past staticNameB.
		{"name_ref_static_index_out_of_range", []byte{0x00, 0x00, 0x5f, 0x54, 0x00}},
		// A Huffman literal that does not decode, on the field-section path: a name
		// reference to static index 0, then H=1, length 1, payload 0xff. Eight 1-bits
		// are the leading bits of EOS and emit no symbol (RFC 7541 §5.2, reused by
		// RFC 9204 §4.1.2), so the value is undecodable.
		{"huffman_undecodable_value", []byte{0x00, 0x00, 0x50, 0x81, 0xff}},
		{"truncated_value", []byte{0x00, 0x00, 0x50, 0x0b, 'e', 'x'}}, // says 11, has 2
		{"missing_base_byte", []byte{0x00}},
		{"empty_input", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dec := NewDecoder()

			err := dec.DecodeFieldSection(tc.in, nil, func([]byte, []byte) error { return nil })

			require.ErrorIsf(t, err, ErrDecompressionFailed,
				"DecodeFieldSection(%x): at SETTINGS_QPACK_MAX_TABLE_CAPACITY 0 this is unresolvable, and a caller that cannot classify it as QPACK_DECOMPRESSION_FAILED cannot close the connection with the right code", tc.in)
		})
	}
}

// TestDecode_EmitError propagates an error returned by the emit callback.
func TestDecode_EmitError(t *testing.T) {
	boom := errors.New("boom")
	in := NewEncoder().EncodeFieldSection(nil, []header.Field{hf(":method", "GET")})

	err := NewDecoder().DecodeFieldSection(in, nil, func([]byte, []byte) error { return boom })

	require.ErrorIs(t, err, boom,
		"the caller's own rejection must reach it unchanged; swallowing it turns a refused header into a silently accepted one")
}

func assertDecode(t *testing.T, in []byte, want []header.Field) {
	t.Helper()

	var got []header.Field
	err := NewDecoder().DecodeFieldSection(in, nil, func(name, value []byte) error {
		got = append(got, header.Field{
			Name:  append([]byte(nil), name...),
			Value: append([]byte(nil), value...),
		})
		return nil
	})

	require.NoErrorf(t, err, "DecodeFieldSection(%x)", in)
	require.Lenf(t, got, len(want),
		"decoded %d fields from %x, want %d — a field-count mismatch means a representation boundary was misread, so no per-field comparison below would mean anything", len(got), in, len(want))
	for i := range want {
		assert.Equalf(t, want[i].Name, got[i].Name,
			"field %d name: the decoder resolved a different table entry or literal than the encoder wrote", i)
		assert.Equalf(t, want[i].Value, got[i].Value,
			"field %d (%s) value: the decoder resolved a different table entry or literal than the encoder wrote", i, got[i].Name)
	}
}

var benchFields = []header.Field{
	hf(":method", "GET"),
	hf(":path", "/index.html"),
	hf(":scheme", "https"),
	hf(":authority", "www.example.com"),
	hf("accept", "*/*"),
	hf("user-agent", "poseidon-http-client/0.8"),
}

func BenchmarkEncodeFieldSection(b *testing.B) {
	enc := NewEncoder()
	dst := make([]byte, 0, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = enc.EncodeFieldSection(dst[:0], benchFields)
	}
}

func BenchmarkDecodeFieldSection(b *testing.B) {
	buf := NewEncoder().EncodeFieldSection(nil, benchFields)
	dec := NewDecoder()
	emit := func([]byte, []byte) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := dec.DecodeFieldSection(buf, nil, emit); err != nil {
			b.Fatal(err)
		}
	}
}
