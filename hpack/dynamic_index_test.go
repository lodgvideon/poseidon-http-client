package hpack

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoderDynamicLookup_MatchesNewest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, value string
		index       uint64
		full        bool
	}{
		{"a", "1", 1, true},
		{"a", "3", 2, true},
		{"b", "2", 3, true},
		{"a", "missing", 1, false},
		{"missing", "1", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name+"/"+tt.value, func(t *testing.T) {
			t.Parallel()
			enc := NewEncoder()
			for _, pair := range [][2]string{{"a", "1"}, {"b", "2"}, {"a", "3"}, {"a", "1"}} {
				enc.dt.add([]byte(pair[0]), []byte(pair[1]))
			}

			index, full := enc.dynamicLookup([]byte(tt.name), []byte(tt.value))

			assert.Equal(t, tt.index, index, "duplicates must resolve to the newest matching entry")
			assert.Equal(t, tt.full, full, "a name-only match must not become an indexed field")
		})
	}
}

func TestEncoderDynamicLookup_EmptyFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		fields    [][2]string
		collision bool
		index     uint64
	}{
		{"newest", [][2]string{{"", ""}}, false, 1},
		{"older", [][2]string{{"", ""}, {"", "value"}}, false, 2},
		{"collision", [][2]string{{"", ""}, {"other", "value"}}, true, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			enc := NewEncoder()
			for _, field := range tt.fields {
				enc.dt.add([]byte(field[0]), []byte(field[1]))
			}
			if tt.collision {
				key := enc.dt.index.hashes(nil, nil)
				enc.dt.index.byName[key[0]] = enc.dt.index.newest
			}

			index, full := enc.dynamicLookup(nil, nil)

			assert.Equal(t, tt.index, index, "an empty field must resolve to its live table entry")
			assert.True(t, full, "empty names and values are valid HPACK string literals")
		})
	}
}

func TestEncoderDynamicLookup_TableChanges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*Encoder)
		key    string
		value  string
		index  uint64
	}{
		{"evicted", func(e *Encoder) { e.SetMaxDynamicTableSizeLimit(68) }, "a", "1", 0},
		{"survivor", func(e *Encoder) { e.SetMaxDynamicTableSizeLimit(68) }, "b", "2", 2},
		{"newest", func(e *Encoder) { e.SetMaxDynamicTableSizeLimit(34) }, "c", "3", 1},
		{"zero", func(e *Encoder) { e.SetMaxDynamicTableSizeLimit(0) }, "c", "3", 0},
		{"reset", func(e *Encoder) { e.Reset() }, "c", "3", 0},
		{"oversized", func(e *Encoder) { e.dt.add([]byte("large"), make([]byte, 4096)) }, "c", "3", 0},
		{"grow-after-empty", func(e *Encoder) {
			e.SetMaxDynamicTableSizeLimit(0)
			e.SetMaxDynamicTableSizeLimit(4096)
			e.dt.add([]byte("d"), []byte("4"))
		}, "d", "4", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			enc := NewEncoder()
			for _, pair := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
				enc.dt.add([]byte(pair[0]), []byte(pair[1]))
			}
			tt.change(enc)

			index, full := enc.dynamicLookup([]byte(tt.key), []byte(tt.value))

			assert.Equal(t, tt.index, index, "table changes must invalidate evicted entries and preserve surviving indices")
			assert.Equal(t, tt.index != 0, full, "an evicted field must be emitted as a literal")
			assert.LessOrEqual(t, len(enc.dt.index.byName), enc.dt.len(), "discarded names must not remain in the index")
			assert.Len(t, enc.dt.index.byNameValue, enc.dt.len(), "discarded fields must not remain in the index")
		})
	}
}

func TestEncoderDynamicLookup_OwnsInsertedBytes(t *testing.T) {
	t.Parallel()
	enc := NewEncoder()
	name, value := []byte("x-owned"), []byte("original")
	first := enc.WriteField(nil, name, value, IndexIncremental)
	copy(name, "x-other")
	copy(value, "mutated!")

	second := enc.WriteField(nil, []byte("x-owned"), []byte("original"), IndexIncremental)
	var fields [][2]string
	dec := NewDecoder()
	err := dec.DecodeBlock(append(first, second...), func(f HeaderField) error {
		fields = append(fields, [2]string{string(f.Name), string(f.Value)})
		return nil
	})

	require.NoError(t, err, "reusing caller buffers must leave the compression context valid")
	assert.Equal(t, []byte{0xbe}, second, "the original field must remain indexed after its caller buffers change")
	assert.Equal(t, [][2]string{{"x-owned", "original"}, {"x-owned", "original"}}, fields)
}

func TestEncoderDynamicLookup_ChurnAndCompaction(t *testing.T) {
	t.Parallel()
	enc := NewEncoder()
	enc.SetMaxDynamicTableSizeLimit(4096)
	for i := range 2000 {
		enc.dt.add([]byte(fmt.Sprintf("x-%03d", i%17)), []byte(fmt.Sprintf("v-%04d", i)))
	}
	enc.dt.compactArena()

	indices := make([]uint64, 80)
	matches := make([]bool, len(indices))
	for i := range indices {
		entry := 1999 - i
		indices[i], matches[i] = enc.dynamicLookup([]byte(fmt.Sprintf("x-%03d", entry%17)), []byte(fmt.Sprintf("v-%04d", entry)))
	}

	for i := range indices {
		assert.Equal(t, uint64(i+1), indices[i], "ring wrap and arena compaction must preserve relative indices")
		assert.True(t, matches[i], "a live field must remain a full match after compaction")
	}
	assert.Len(t, enc.dt.index.byName, 17, "evicting an older name must preserve its newest entry")
	assert.Len(t, enc.dt.index.byNameValue, enc.dt.len(), "evicted fields must not accumulate in the index")
}

func TestEncoderDynamicLookup_HashCollision(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, value string
		pair        bool
		candidate   uint64
		index       uint64
		full        bool
	}{
		{"a", "1", true, 3, 3, true},
		{"a", "1", true, 2, 3, true},
		{"a", "missing", false, 3, 2, false},
		{"missing", "1", false, 3, 0, false},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			enc := NewEncoder()
			for _, pair := range [][2]string{{"a", "1"}, {"a", "2"}, {"b", "1"}} {
				enc.dt.add([]byte(pair[0]), []byte(pair[1]))
			}
			name, value := []byte(tt.name), []byte(tt.value)
			key := enc.dt.index.hashes(name, value)
			if tt.pair {
				enc.dt.index.byNameValue[key] = tt.candidate
			} else {
				enc.dt.index.byName[key[0]] = tt.candidate
			}

			index, full := enc.dynamicLookup(name, value)

			assert.Equal(t, tt.index, index, "a hash collision must not substitute a different field")
			assert.Equal(t, tt.full, full, "collision recovery must preserve full and name-only matching")
		})
	}
}

func TestEncoderDynamicLookup_DuplicateSurvivesEviction(t *testing.T) {
	t.Parallel()
	enc := NewEncoder()
	for _, pair := range [][2]string{{"a", "1"}, {"b", "2"}, {"a", "1"}} {
		enc.dt.add([]byte(pair[0]), []byte(pair[1]))
	}
	enc.SetMaxDynamicTableSizeLimit(68)

	index, full := enc.dynamicLookup([]byte("a"), []byte("1"))

	assert.Equal(t, uint64(1), index, "evicting an older duplicate must preserve the newest index entry")
	assert.True(t, full)
	assert.Len(t, enc.dt.index.byNameValue, 2, "both surviving fields must remain indexed")
}
