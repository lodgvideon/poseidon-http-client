package hpack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	collect := func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:  append([]byte(nil), f.Name...),
			Value: append([]byte(nil), f.Value...),
		})
		return nil
	}

	err := d.Feed(block, collect)

	require.NoError(t, err, "a truncated trailing literal is buffered, not an error")
	require.Len(t, got, 1,
		"exactly the complete field before the truncation may be emitted; the truncated one is not a field yet")
	assert.Equal(t, "a", string(got[0].Name), "name of the field that completed before the truncation")
	assert.Equal(t, "b", string(got[0].Value), "value of the field that completed before the truncation")

	// The truncated field completes once its bytes arrive: name "hello", value "x".
	err = d.Feed([]byte{'h', 'e', 'l', 'l', 'o', 0x01, 'x'}, collect)

	require.NoError(t, err, "the buffered representation resumes cleanly once its remaining bytes arrive")
	require.Len(t, got, 2, "the resumed field must be emitted — the streaming state survived the truncation")
	assert.Equal(t, "hello", string(got[1].Name), "name of the field that resumed across the Feed boundary")
	assert.Equal(t, "x", string(got[1].Value), "value of the field that resumed across the Feed boundary")
}
