package hpack

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collectFields(t *testing.T, d *Decoder, blockHex string) []HeaderField {
	t.Helper()
	block, err := hex.DecodeString(blockHex)
	require.NoError(t, err, "hex decode of the fixture")
	var got []HeaderField

	err = d.DecodeBlock(block, func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:     append([]byte{}, f.Name...),
			Value:    append([]byte{}, f.Value...),
			Indexing: f.Indexing,
		})
		return nil
	})

	require.NoError(t, err, "DecodeBlock of the fixture")
	return got
}

// RFC 7541 §C.2.1.
func TestDecoder_LiteralIncrementalIndexing(t *testing.T) {
	d := NewDecoder()

	got := collectFields(t, d, "400a637573746f6d2d6b65790d637573746f6d2d686561646572")

	require.Len(t, got, 1, "one representation must emit exactly one field")
	assert.Equal(t, "custom-key", string(got[0].Name), "the literal name")
	assert.Equal(t, "custom-header", string(got[0].Value), "the literal value")
	assert.Equal(t, 1, d.dt.len(),
		"§6.2.1 must insert into the dynamic table; skipping the insert renumbers every later index relative to the peer's table")
}

// RFC 7541 §C.2.2.
func TestDecoder_LiteralWithoutIndexing(t *testing.T) {
	d := NewDecoder()

	got := collectFields(t, d, "040c2f73616d706c652f70617468")

	require.Len(t, got, 1, "one representation must emit exactly one field")
	assert.Equal(t, ":path", string(got[0].Name), "name index 4 resolves to :path")
	assert.Equal(t, "/sample/path", string(got[0].Value), "the literal value")
	assert.Equal(t, 0, d.dt.len(),
		"§6.2.2 must NOT touch the dynamic table; an insert here desynchronises our table from the peer's encoder")
}

// RFC 7541 §C.2.3: never-indexed with literal name "password" + value "secret".
func TestDecoder_LiteralNeverIndexed(t *testing.T) {
	d := NewDecoder()

	got := collectFields(t, d, "100870617373776f72640673656372657420")

	require.Len(t, got, 1, "one field representation must emit exactly one field")
	assert.True(t, got[0].Sensitive(),
		"§6.2.3 must survive the decode as IndexNever; a field that arrives unmarked can be re-indexed by an intermediary that was told never to")
}

// RFC 7541 §C.2.4: indexed header field.
func TestDecoder_IndexedHeaderField(t *testing.T) {
	d := NewDecoder()

	got := collectFields(t, d, "82")

	require.Len(t, got, 1, "one representation must emit exactly one field")
	assert.Equal(t, ":method", string(got[0].Name), "static index 2 is :method")
	assert.Equal(t, "GET", string(got[0].Value), "static index 2 carries the value GET")
}

func BenchmarkDecoder_DecodeBlock_3req_static(b *testing.B) {
	d := NewDecoder()
	block, _ := hex.DecodeString("828784")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.DecodeBlock(block, func(_ HeaderField) error { return nil })
	}
}

func TestDecoder_Streaming_SplitMidField(t *testing.T) {
	full, err := hex.DecodeString("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	require.NoError(t, err, "hex decode of the fixture")

	for splitAt := 1; splitAt < len(full); splitAt++ {
		t.Run(fmt.Sprintf("split_at_%d", splitAt), func(t *testing.T) {
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

			errFeed1 := d.Feed(full[:splitAt], visit)
			errFeed2 := d.Feed(full[splitAt:], visit)
			errFinish := d.Finish()

			require.NoError(t, errFeed1, "Feed of the first fragment — a partial representation is buffered, not an error")
			require.NoError(t, errFeed2, "Feed of the second fragment")
			require.NoError(t, errFinish, "Finish — every byte of the block was consumed")
			require.Len(t, got, 1,
				"the field must be emitted exactly once wherever the fragment boundary falls; twice means the buffered prefix was replayed, none means it was dropped")
			assert.Equal(t, "custom-key", string(got[0].Name), "name reassembled across the fragment boundary")
			assert.Equal(t, "custom-header", string(got[0].Value), "value reassembled across the fragment boundary")
		})
	}
}

func TestDecoder_Streaming_FinishWithoutBegin(t *testing.T) {
	d := NewDecoder()

	err := d.Finish()

	require.ErrorIs(t, err, ErrNotStreaming,
		"Finish without Begin is the caller's sequencing mistake; reporting it as a wire-format sentinel blames the peer for a local bug")
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

	require.ErrorIs(t, err, ErrHeaderListTooLarge,
		"the gate charges a RUNNING total; checking each field on its own lets a peer walk past any limit by splitting one huge list into small fields")
}

func TestDecoder_Reset_ClearsState(t *testing.T) {
	d := NewDecoder()
	d.Begin()
	d.SetMaxDynamicTableSize(2048)
	// The table must actually hold something, or the assertion below is satisfied
	// by a Reset that clears nothing. §6.2.1 literal: custom-key/custom-header.
	require.NoError(t,
		d.DecodeBlock(mustHex(t, "400a637573746f6d2d6b65790d637573746f6d2d686561646572"),
			func(HeaderField) error { return nil }),
		"fixture: populate the dynamic table so Reset has something to clear")
	require.Equal(t, 1, d.dt.len(), "precondition: one entry in the dynamic table")

	d.Reset()

	assert.False(t, d.streaming,
		"Reset must clear the streaming flag, or a later Feed silently resumes a session the caller believes it closed")
	assert.Equal(t, 0, d.dt.len(),
		"Reset must empty the dynamic table, or entries from the previous connection stay indexable on the next one")
}

func TestDecoder_SetMaxDynamicTableSize_AppliesEviction(t *testing.T) {
	d := NewDecoder()
	enc := NewEncoder()
	block := enc.EncodeBlock(nil, []HeaderField{
		{Name: []byte("custom-key"), Value: []byte("custom-header")},
	})
	require.NoError(t, d.DecodeBlock(block, func(HeaderField) error { return nil }),
		"fixture: decode one indexable field")
	require.Equal(t, 1, d.dt.len(), "precondition: 1 entry in dynamic table")

	d.SetMaxDynamicTableSize(0)

	assert.Equal(t, 0, d.dt.len(),
		"a local SETTINGS_HEADER_TABLE_SIZE of 0 must evict everything; a surviving entry is indexable at a size we told the peer we no longer hold")
}

func TestDecoder_MaxHeaderListSize_ZeroDisablesGate(t *testing.T) {
	d := NewDecoder()
	enc := NewEncoder()
	buf := enc.EncodeBlock(nil, []HeaderField{
		{Name: []byte("k"), Value: []byte("v")},
	})

	err := d.DecodeBlock(buf, func(HeaderField) error { return nil })

	require.NoError(t, err,
		"maxListSize 0 means no limit was announced, so the gate is off; charging against a zero limit would reject every field there is")
}

// mustHex decodes a wire fixture or fails the test; a malformed fixture is a
// broken test, not a decoder finding.
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err, "hex decode of the fixture")
	return b
}
