package qpack

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// FuzzDecodeFieldSection feeds arbitrary bytes to the QPACK field-section decoder
// (RFC 9204 §4.5), the header-decompression entry point on every HTTP/3 response.
// A peer's field section is fully attacker-controlled, so the decoder must reject
// any malformed input (or unresolvable dynamic reference) with
// ErrDecompressionFailed and never panic or hang. It is exercised twice per input:
//   - with a nil table (the static-only profile), where any dynamic reference or
//     non-zero Required Insert Count MUST be rejected; and
//   - with a populated dynamic table, so the dynamic indexed / name-reference /
//     post-base decode paths are covered too.
func FuzzDecodeFieldSection(f *testing.F) {
	// A valid static-only field section built by the encoder (:method GET, :path /).
	enc := NewEncoder()
	valid := enc.EncodeFieldSection(nil, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte("x-custom"), Value: []byte("value")},
	})
	f.Add(valid)

	f.Add([]byte{})                             // empty: missing the prefix
	f.Add([]byte{0x00, 0x00})                   // valid prefix (RIC 0, Base 0), no field lines
	f.Add([]byte{0x00, 0x00, 0xd1})             // static Indexed Field Line, :method GET (idx 17)
	f.Add([]byte{0x00})                         // RIC present, Base byte missing
	f.Add([]byte{0x00, 0x00, 0x80})             // dynamic Indexed ref with an empty table -> reject
	f.Add([]byte{0xff, 0x00})                   // non-zero encoded RIC against a nil table -> reject
	f.Add([]byte{0x00, 0x00, 0x20, 0x00, 0x00}) // literal name+value, both empty

	f.Fuzz(func(_ *testing.T, src []byte) {
		emit := func(_, _ []byte) error { return nil }

		// Static-only profile: dynamic references and any non-zero RIC must be
		// rejected, never panic.
		NewDecoder().DecodeFieldSection(src, nil, emit)

		// Populated dynamic table: exercise the dynamic-reference resolution paths.
		dt := NewDynamicTable(4096)
		if err := dt.setCapacity(4096); err == nil {
			dt.insert([]byte("custom-key"), []byte("custom-value"))
			dt.insert([]byte("x-foo"), []byte("bar"))
			NewDecoder().DecodeFieldSection(src, dt, emit)
		}
	})
}
