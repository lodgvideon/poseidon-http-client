package hpack

import "testing"

// TestConformance_RFC7541_StreamingDecode_TruncatedLiteralDoesNotPanic pins that
// a peer-crafted HPACK block with a truncated string literal following a complete
// literal field does not crash the streaming decoder. parseLiteral wrote
// decodeStringLiteral's result straight back to d.scratch; on truncation that
// result is nil, so decodePartial's rollback `d.scratch = d.scratch[:scratchSnap]`
// resliced a nil (cap 0) scratch with scratchSnap > 0 — a panic (slice bounds out
// of range), a remote crash from a single decode.
func TestConformance_RFC7541_StreamingDecode_TruncatedLiteralDoesNotPanic(t *testing.T) {
	d := NewDecoder()
	d.Begin()

	// Field 1 (literal without indexing): 0x00, name len 1 "a", value len 1 "b".
	// Field 2 (literal without indexing): 0x00, name string literal declaring
	// length 5 (0x05) with no body — truncated. scratchSnap is 2 (the length after
	// field 1) when field 2's decode fails, which is what triggered the panic.
	block := []byte{0x00, 0x01, 'a', 0x01, 'b', 0x00, 0x05}

	var got []HeaderField
	err := d.Feed(block, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:  append([]byte(nil), f.Name...),
			Value: append([]byte(nil), f.Value...),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Feed = %v, want nil — a truncated trailing literal is buffered, not an error", err)
	}
	if len(got) != 1 || string(got[0].Name) != "a" || string(got[0].Value) != "b" {
		t.Fatalf("decoded = %+v, want exactly one field a=b (the complete field before the truncation)", got)
	}

	// The truncated field completes once its bytes arrive: name "hello", value "x".
	if err := d.Feed([]byte{'h', 'e', 'l', 'l', 'o', 0x01, 'x'}, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:  append([]byte(nil), f.Name...),
			Value: append([]byte(nil), f.Value...),
		})
		return nil
	}); err != nil {
		t.Fatalf("second Feed = %v, want nil — the buffered field resumes cleanly", err)
	}
	if len(got) != 2 || string(got[1].Name) != "hello" || string(got[1].Value) != "x" {
		t.Fatalf("decoded = %+v, want a=b then hello=x — the streaming state survived the truncation", got)
	}
}
