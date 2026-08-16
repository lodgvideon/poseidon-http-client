package hpack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamicTable_AddAndAt(t *testing.T) {
	dt := newDynamicTable(4096)

	dt.add([]byte("custom-key"), []byte("custom-header"))

	require.Equal(t, 1, dt.len(), "one add must produce one entry")
	name, value := dt.at(1)
	assert.Equal(t, "custom-key", string(name), "at(1) must be the most recently added entry (§2.3.3)")
	assert.Equal(t, "custom-header", string(value), "at(1) must return the value stored with that name")
	assert.Equal(t, uint32(10+13+32), dt.byteSize(),
		"§4.1 charges name + value + 32 per entry; any other accounting disagrees with the peer about when an eviction happens")
}

func TestDynamicTable_FIFOAddOrder(t *testing.T) {
	dt := newDynamicTable(4096)

	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))
	dt.add([]byte("c"), []byte("3"))

	entry := func(i int) string {
		n, v := dt.at(i)
		return string(n) + "=" + string(v)
	}
	// §2.3.3: index 1 is the NEWEST entry, so the table reads newest-first.
	assert.Equal(t, "c=3", entry(1), "index 1 must be the newest entry (§2.3.3)")
	assert.Equal(t, "b=2", entry(2), "index 2 must be the second-newest entry")
	assert.Equal(t, "a=1", entry(3), "index 3 must be the oldest entry; a table read oldest-first resolves every peer index to the wrong field")
}

func TestDynamicTable_EvictOnSize(t *testing.T) {
	// Each entry: 1+1+32 = 34 bytes. Capacity 70 holds 2.
	dt := newDynamicTable(70)
	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))

	dt.add([]byte("c"), []byte("3")) // evicts oldest (a=1)

	require.Equal(t, 2, dt.len(), "a third 34-byte entry does not fit in 70 bytes, so exactly one must be evicted")
	n, v := dt.at(2)
	assert.Equal(t, "b", string(n), "the OLDEST entry is the one evicted (§4.4 FIFO), leaving b as the oldest survivor")
	assert.Equal(t, "2", string(v), "the surviving oldest entry keeps its value")
}

func TestDynamicTable_AddOversizedClearsAll(t *testing.T) {
	dt := newDynamicTable(50)
	dt.add([]byte("x"), []byte("1"))
	bigVal := make([]byte, 100)

	dt.add([]byte("big"), bigVal)

	assert.Equal(t, 0, dt.len(),
		"§4.4: an entry larger than the maximum size empties the table; leaving the earlier entries in place keeps indices alive that the peer's encoder has already discarded")
}

func TestDynamicTable_SetMaxSizeShrinks(t *testing.T) {
	dt := newDynamicTable(200)
	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))
	dt.add([]byte("c"), []byte("3"))

	dt.setMaxSize(35) // holds at most 1 entry of size 34

	assert.Equal(t, 1, dt.len(),
		"shrinking the cap must evict down to it immediately (§4.3); entries kept above the new cap are indexable at a size we no longer advertise")
}

func TestDynamicTable_CompactArena_TriggersOnGrowth(t *testing.T) {
	// Add and immediately evict to grow arena beyond used*2, triggering
	// compactArena. Entry size 34 (1+1+32). Cap 70 keeps 2 entries; many
	// adds churn the arena.
	dt := newDynamicTable(70)

	for i := 0; i < 200; i++ {
		dt.add([]byte{byte('a' + i%26)}, []byte{byte('0' + i%10)})
	}

	require.Equal(t, 2, dt.len(), "the cap still holds exactly 2 entries after 200 adds")
	// Most-recently-added entry must still resolve correctly.
	n, v := dt.at(1)
	const last = 199
	assert.Equal(t, string([]byte{byte('a' + last%26)}), string(n),
		"compaction rewrites the arena and the entry offsets together; a name that reads back wrong means an offset was left pointing into the old layout")
	assert.Equal(t, string([]byte{byte('0' + last%10)}), string(v),
		"compaction rewrites the arena and the entry offsets together; a value that reads back wrong means an offset was left pointing into the old layout")
}

func TestDynamicTable_Clear(t *testing.T) {
	dt := newDynamicTable(200)
	dt.add([]byte("a"), []byte("1"))

	dt.clear()

	assert.Equal(t, 0, dt.len(), "clear must drop every entry")
	assert.Equal(t, uint32(0), dt.byteSize(),
		"clear must zero the running size too; a size left behind makes the table evict as though entries it no longer holds were still there")
}
