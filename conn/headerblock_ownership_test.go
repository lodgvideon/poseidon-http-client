package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Growth must rebind earlier fields so a pooled block does not retain detached
// arrays in addition to the buffer it owns and reuses.
func TestHeaderBlock_AppendFieldGrowthKeepsOneBackingBuffer(t *testing.T) {
	t.Parallel()
	want := []header.Field{
		{Name: []byte("x-old"), Value: []byte("a"), Indexing: header.IndexNever},
		{Name: []byte("x-new"), Value: []byte("growth"), Indexing: header.IndexWithout},
	}
	blk := &HeaderBlock{buf: make([]byte, 0, 8)}
	blk.appendField(want[0])
	originalBuffer := &blk.buf[0]
	originalCapacity := cap(blk.buf)

	blk.appendField(want[1])

	assert.Greater(t, cap(blk.buf), originalCapacity, "the fixture must force buffer growth")
	assert.NotSame(t, originalBuffer, &blk.buf[0], "growth must allocate a new backing array")
	require.Equal(t, want, blk.fields, "growth must preserve field order, bytes and indexing modes")
	require.Equal(t, "x-oldax-newgrowth", string(blk.buf))
	offset := 0
	for i, field := range blk.fields {
		assert.Same(t, &blk.buf[offset], &field.Name[0], "field %d's name must use the current owned buffer", i)
		offset += len(field.Name)
		assert.Same(t, &blk.buf[offset], &field.Value[0], "field %d's value must use the current owned buffer", i)
		offset += len(field.Value)
		assert.Equal(t, len(field.Name), cap(field.Name), "appending to field %d's name must not overwrite its value", i)
		assert.Equal(t, len(field.Value), cap(field.Value), "appending to field %d's value must not overwrite its neighbor", i)
	}
}
