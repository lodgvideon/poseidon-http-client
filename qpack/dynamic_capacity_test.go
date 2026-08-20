package qpack

// dynamic_capacity_test.go — the two dynamic-table transitions no test walked
// (#765, #766): a SHRINKING Set Dynamic Table Capacity, and the arena compaction
// that is the only thing reclaiming the bytes evicted entries leave behind.
//
// Both are peer-driven. Every Set Dynamic Table Capacity in the suite either
// raised the capacity or set it on an empty table, so the one transition that
// evicts was never taken; and the entry-ring bound test that has exactly the
// right shape for compaction inserts ("", "") entries, which cost zero arena
// bytes, so its own fixture cannot express the failure.

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setCapacityInstr builds a Set Dynamic Table Capacity instruction (RFC 9204
// §4.3.1): 001xxxxx with a 5-bit-prefix capacity.
func setCapacityInstr(n uint64) []byte { return hpack.EncodeInteger(nil, 5, 0x20, n) }

// TestQPACK_SetDynamicTableCapacity_ShrinkEvicts walks the missing arm of the
// §4.3.1 state transition. Growing and setting-on-empty are covered; shrinking a
// NON-EMPTY table below its current size is the only transition that runs the
// eviction loop, and §3.2.2 requires entries be evicted until the table fits.
// Skipping it leaves Size() > Capacity() — our table then holds entries the
// peer's has dropped, and every later reference resolves against a table that
// has silently diverged.
func TestQPACK_SetDynamicTableCapacity_ShrinkEvicts(t *testing.T) {
	// Each entry is 1+1+32 = 34 bytes (§3.2.1), so 102 holds exactly three.
	const filled = 102
	newFilled := func(t *testing.T) *DynamicTable {
		t.Helper()
		dt := NewDynamicTable(4096)
		mustApply(t, dt, setCapacityInstr(filled))
		applyInserts(t, dt, insertLiteral("a", "1"), insertLiteral("b", "2"), insertLiteral("c", "3"))
		require.Equal(t, 3, dt.Len(),
			"fixture: the table must actually hold three entries, or the shrink below has nothing to evict and the test passes without reaching the loop")
		require.Equal(t, uint64(filled), dt.Size(),
			"fixture: the table must be exactly full, so the capacities chosen below sit where this test says they do")
		return dt
	}

	t.Run("below the current size evicts down to it", func(t *testing.T) {
		dt := newFilled(t)

		mustApply(t, dt, setCapacityInstr(68)) // room for two entries

		require.Equal(t, 2, dt.Len(),
			"§3.2.2: a capacity that no longer fits every entry must evict until it does; keeping them leaves us resolving indices the peer's table has already dropped")
		assert.Equal(t, uint64(68), dt.Size(), "the surviving entries must be re-accounted, or the next insert evicts at the wrong point")
		assert.LessOrEqual(t, dt.Size(), dt.Capacity(), "the live size must never exceed the capacity in force")
		assert.Equal(t, uint64(3), dt.InsertCount(),
			"absolute indices are permanent (§3.2.4): eviction must not rewind the insert count, or every later relative reference is resolved against the wrong anchor")
		_, _, ok := dt.at(0)
		assert.False(t, ok, "the oldest entry was evicted, and an evicted absolute index must stay unresolvable")
		wantEntry(t, dt, 1, "b", "2")
		wantEntry(t, dt, 2, "c", "3")
	})

	t.Run("to exactly the current size evicts nothing", func(t *testing.T) {
		dt := newFilled(t)

		mustApply(t, dt, setCapacityInstr(filled))

		require.Equal(t, 3, dt.Len(),
			"the loop runs while size > capacity, so a capacity EQUAL to the current size must evict nothing; dropping an entry here discards one the peer's encoder still indexes")
		assert.Equal(t, uint64(filled), dt.Size(), "nothing was evicted, so the accounted size must be unchanged")
		wantEntry(t, dt, 0, "a", "1")
	})

	t.Run("to zero empties the table and refuses the next insert", func(t *testing.T) {
		dt := newFilled(t)

		mustApply(t, dt, setCapacityInstr(0))

		require.Equal(t, 0, dt.Len(),
			"a capacity of zero fits no entry at all, so every one must go; §3.2.3 lets the decoder shut the table off mid-connection and the peer stops referencing it immediately")
		assert.Equal(t, uint64(0), dt.Size(), "an emptied table must account nothing")
		assert.Equal(t, uint64(3), dt.InsertCount(), "absolute indices survive an emptying (§3.2.4)")

		_, err := dt.ParseEncoderInstructions(insertLiteral("d", "4"))

		require.ErrorIs(t, err, ErrEncoderStream,
			"an insert into a zero-capacity table cannot fit even in an empty table, which §4.3.3 makes an encoder-stream error rather than a silently dropped entry")
	})
}

// TestQPACK_Arena_StaysBoundedUnderEvictionChurn pins the arena as bounded by the
// CAPACITY rather than by the peer's instruction count. evictOldest reclaims the
// arena only when the table empties (count == 0), which a busy connection never
// does, so the compaction trigger is the only thing standing between a peer and
// an unbounded allocation it drives with wire bytes — the same shape the entry
// ring already has a test for.
//
// The bound is deliberately loose and stated in absolute terms: it pins the
// property (a two-entry table keeps a small arena however many instructions
// arrive), not today's growth policy.
func TestQPACK_Arena_StaysBoundedUnderEvictionChurn(t *testing.T) {
	const capacity = 76 // exactly two 38-byte entries: 3 + 3 + 32
	const inserts = 4096
	dt := NewDynamicTable(4096)
	mustApply(t, dt, setCapacityInstr(capacity))
	var instr []byte
	for i := 0; i < inserts; i++ {
		instr = append(instr, insertLiteral(churnName(i), churnValue(i))...)
	}

	n, err := dt.ParseEncoderInstructions(instr)

	require.NoError(t, err, "every instruction is well formed, so all of them must apply")
	require.Equal(t, len(instr), n, "a short consume would mean the churn below never happened")
	require.Equal(t, 2, dt.Len(),
		"the capacity affords exactly two entries, so eviction churned on every insert past the second — and never emptied the table, which is the only other path that reclaims arena bytes")
	require.Equal(t, uint64(inserts), dt.InsertCount(), "absolute indices are permanent (§3.2.4)")
	assert.LessOrEqualf(t, len(dt.arena), 1024,
		"arena grew to %d bytes for a two-entry table after %d instructions; storage must track SETTINGS_QPACK_MAX_TABLE_CAPACITY, not the peer's instruction count",
		len(dt.arena), inserts)
	// Reclamation must not cost correctness: the live entries still resolve, and
	// the evicted ones still do not.
	wantEntry(t, dt, uint64(inserts-1), churnName(inserts-1), churnValue(inserts-1))
	wantEntry(t, dt, uint64(inserts-2), churnName(inserts-2), churnValue(inserts-2))
	_, _, ok := dt.at(uint64(inserts - 3))
	assert.False(t, ok, "an entry evicted by the churn must stay unresolvable, whatever compaction did to the bytes behind it")
}

// TestQPACK_CompactArena_PreservesAWrappedRing exercises compactArena on a ring
// whose live entries wrap past the end of the slot array (head + count >
// len(entries)). That is the case its own comment names as the reason it builds
// a FRESH ring instead of rewriting in place — writing slot i can clobber a
// source slot not yet read — and nothing in the package reached it, because
// nothing in the package called compactArena at all.
func TestQPACK_CompactArena_PreservesAWrappedRing(t *testing.T) {
	const capacity = 152 // exactly four 38-byte entries
	dt := NewDynamicTable(4096)
	mustApply(t, dt, setCapacityInstr(capacity))
	// Insert until the ring genuinely wraps. It does so shortly after an automatic
	// compaction, which leaves len(entries) == count: the next eviction moves head
	// off zero while the ring is exactly full.
	wrapped := false
	for i := 0; i < 200 && !wrapped; i++ {
		mustApply(t, dt, insertLiteral(churnName(i), churnValue(i)))
		wrapped = dt.head+dt.count > len(dt.entries)
	}
	require.Truef(t, wrapped,
		"fixture: the ring never wrapped (head=%d count=%d slots=%d), so this test would exercise the easy case and pass whatever compactArena did with the hard one",
		dt.head, dt.count, len(dt.entries))
	live := make(map[uint64][2]string, dt.count)
	for i := 0; i < dt.count; i++ {
		abs := dt.oldestAbs() + uint64(i)
		name, value, ok := dt.at(abs)
		require.Truef(t, ok, "fixture: live absolute index %d must resolve before compaction", abs)
		live[abs] = [2]string{string(name), string(value)}
	}
	evicted := dt.oldestAbs() - 1

	dt.compactArena()

	require.Equal(t, len(live), dt.count, "compaction must not change how many entries are live")
	assert.Equal(t, 0, dt.head, "compaction re-bases the ring, so the oldest live entry sits at slot 0")
	for abs, want := range live {
		name, value, ok := dt.at(abs)

		require.Truef(t, ok, "absolute index %d stopped resolving after compaction; absolute indices are permanent (§3.2.4) and compaction is invisible to the peer", abs)
		assert.Equalf(t, want[0], string(name),
			"absolute index %d name changed across compaction; on a wrapped ring a slot rewritten in place clobbers a source slot not yet read, and the peer resolves that index to a header nobody sent", abs)
		assert.Equalf(t, want[1], string(value), "absolute index %d value changed across compaction", abs)
	}
	_, _, ok := dt.at(evicted)
	assert.False(t, ok, "compaction must not resurrect an evicted absolute index")
}

// churnName / churnValue are fixed-width three-byte entries, so every entry is
// exactly 38 accounting bytes and the capacities above hold the stated count.
func churnName(i int) string {
	return string([]byte{'n', byte('0' + i%10), byte('0' + (i/10)%10)})
}

func churnValue(i int) string {
	return string([]byte{'v', byte('0' + i%10), byte('0' + (i/10)%10)})
}
