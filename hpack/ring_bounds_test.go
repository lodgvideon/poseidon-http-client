package hpack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// emptyInsert is a literal-with-incremental-indexing field with an empty name
// and an empty value: three wire bytes, and spec-legal (RFC 7541 §6.2.1 puts no
// floor on a string literal's length). Its RFC size is 32 — all overhead, no
// bytes — so it evicts like any other entry but contributes nothing to the arena.
var emptyInsert = []byte{0x40, 0x00, 0x00}

// TestDynamicTable_EmptyInsertsDoNotGrowRing pins the entry ring against a peer
// that drives it with insertion COUNT rather than table size.
//
// The table is bounded at 4096 octets, so at 32 octets of overhead each, at most
// 128 entries are ever live. A ring that reuses evicted slots therefore stays at
// ~128 slots no matter how many insertions arrive. Before this test, add() grew
// the ring by one slot per insertion and only compactArena() ever shrank it —
// and compaction's trigger charges arena bytes, which empty entries never touch,
// so it could not fire. Measured: 3 MB of these on the wire retained 1,000,000
// slots (16 MB), a 5.3x amplification with no ceiling.
func TestDynamicTable_EmptyInsertsDoNotGrowRing(t *testing.T) {
	d := NewDecoder()
	const inserts = 200000
	block := make([]byte, 0, inserts*len(emptyInsert))
	for i := 0; i < inserts; i++ {
		block = append(block, emptyInsert...)
	}

	err := d.DecodeBlock(block, func(HeaderField) error { return nil })

	require.NoError(t, err, "peer input rejected — these insertions are spec-legal and must decode")
	dt := d.dt
	assert.LessOrEqualf(t, dt.count, 128,
		"count=%d exceeds the 128 entries a 4096-octet table can hold", dt.count)
	// Loose on purpose: this pins the absence of unbounded growth, not an exact
	// ring size. Doubling plus compaction leaves slack.
	assert.LessOrEqualf(t, len(dt.entries), 4*128,
		"entry ring grew to %d slots after %d empty insertions (~%d B retained); "+
			"a 4096-octet table holds at most 128 entries, so the ring must not track "+
			"insertion count",
		len(dt.entries), inserts, len(dt.entries)*16)
}

// TestDynamicTable_CompactArenaPreservesWrappedRing pins compaction against a
// ring whose live entries wrap past the end of the slice (head != 0 and
// head+count > len). Compaction rewrites slots [0..count) while reading from
// [head..head+count), so aliasing the source ring lets a write clobber a slot
// that has not been read yet — silently swapping one header's name or value for
// another's. Nothing caught this before because the old add() grew the ring on
// every insertion, which made head+count == len an invariant: the ring could not
// wrap, so the aliasing could not bite. Fixing the growth exposes it.
func TestDynamicTable_CompactArenaPreservesWrappedRing(t *testing.T) {
	d := newDynamicTable(4096)
	// Fill, then evict, then refill, so the live window wraps the slice end.
	for i := 0; i < 40; i++ {
		d.add([]byte("name-aaaaaaaaaaaaaaaa"), []byte("value-bbbbbbbbbbbbbbbb"))
	}
	for i := 0; i < 20; i++ {
		d.evictOldest()
	}
	for i := 0; i < 30; i++ {
		name := []byte("wrapped-name-" + string(rune('a'+i%26)))
		value := []byte("wrapped-value-" + string(rune('a'+i%26)))
		d.add(name, value)
	}
	if d.head+d.count <= len(d.entries) {
		t.Skipf("ring did not wrap (head=%d count=%d len=%d); test premise not met",
			d.head, d.count, len(d.entries))
	}
	// Snapshot every live entry, then force compaction and re-read.
	want := make([][2]string, 0, 40)
	for i := 1; i <= d.count; i++ {
		n, v := d.at(i)
		want = append(want, [2]string{string(n), string(v)})
	}

	d.compactArena()

	require.Lenf(t, want, d.count, "compaction changed count: got %d, want %d", d.count, len(want))
	for i := 1; i <= d.count; i++ {
		n, v := d.at(i)
		assert.Equalf(t, want[i-1][0], string(n),
			"compaction corrupted entry %d name: got %q, want %q; a write into a slot not yet read swaps one header's bytes for another's", i, n, want[i-1][0])
		assert.Equalf(t, want[i-1][1], string(v),
			"compaction corrupted entry %d value: got %q, want %q; a write into a slot not yet read swaps one header's bytes for another's", i, v, want[i-1][1])
	}
}
