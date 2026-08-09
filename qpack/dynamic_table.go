package qpack

import "bytes"

// entryOverhead is the per-entry accounting overhead RFC 9204 §3.2.1 adds to
// name+value length. The same number also fixes the smallest possible entry,
// which is why maxEntries divides by it and why an encoder configured below it
// is rejected — those are one constant in three roles, not three constants.
//
// HPACK defines the same overhead in RFC 7541 §4.1. That is deliberately a
// separate constant in a separate package: the two are frozen by different
// specs and by interop with different peers, so QPACK must not inherit a change
// to HPACK's, and hpack is an A-layer package this one should not depend on.
const entryOverhead = 32

// dtEntry stores offsets into the arena for one dynamic-table entry. Each
// entry's RFC size is nameLen + valueLen + entryOverhead (RFC 9204 §3.2.1,
// identical to HPACK §4.1).
type dtEntry struct {
	nameOff, nameLen   uint32
	valueOff, valueLen uint32
}

// initialEntryRing is the slot count the entry ring is first sized to; it then
// doubles as needed. The capacity bounds how many entries can be live at once
// (every entry is at least entryOverhead bytes), so the ring stops doubling at
// maxCapacity/entryOverhead slots however many instructions the peer sends.
const initialEntryRing = 32

// DynamicTable is a QPACK dynamic table (RFC 9204 §3.2) for one direction of one
// connection. Entries are appended at monotonically increasing ABSOLUTE indices
// (§3.2.4): the first insertion has absolute index 0, and InsertCount is the
// total number of insertions performed. When an insertion would exceed the
// current capacity the oldest entries are evicted (§3.2.2). References are
// resolved by absolute index, by an index relative to the current insert count
// (encoder-stream instructions, §3.2.5), by an index relative to a field
// section's Base (§3.2.5), or as a post-Base index (§3.2.6).
//
// The http3 client sizes one of these to the SETTINGS_QPACK_MAX_TABLE_CAPACITY it
// advertises and shares it across the connection under its own lock; this type is
// not safe for concurrent use on its own.
type DynamicTable struct {
	// entries is a FIFO ring of len(entries) slots holding count live entries at
	// slots head..head+count (mod len). It grows only when the ring is full, so an
	// evicted entry's slot is reused and the ring's size tracks the CAPACITY rather
	// than the number of instructions the peer has sent.
	entries []dtEntry
	head    int // slot of the oldest live entry
	count   int // number of live entries

	arena []byte // packed name+value bytes for live entries
	used  uint32 // arena bytes in active use (excludes dead gaps)

	// maxCapacity is SETTINGS_QPACK_MAX_TABLE_CAPACITY: the largest capacity the
	// decoder permits. It bounds Set Dynamic Table Capacity and fixes MaxEntries
	// (RFC 9204 §3.2.2, §4.5.1.1) for the Required Insert Count wraparound.
	maxCapacity uint64
	// capacity is the current table capacity set by Set Dynamic Table Capacity
	// (§4.3.1); it governs eviction and starts at 0.
	capacity uint64
	size     uint64 // sum of live entry sizes (§3.2.1)

	// insertCount is the total number of insertions performed over the table's
	// lifetime — the absolute index that the next inserted entry will take, and
	// the reference point for encoder-stream relative indices (§3.2.5).
	insertCount uint64

	nameScratch []byte // reused copy/Huffman buffer for an instruction name
	valScratch  []byte // reused copy/Huffman buffer for an instruction value
}

// NewDynamicTable returns an empty QPACK dynamic table whose maximum capacity
// (SETTINGS_QPACK_MAX_TABLE_CAPACITY) is maxCapacity. The current capacity
// starts at 0; the peer's Set Dynamic Table Capacity instruction raises it, up
// to maxCapacity.
func NewDynamicTable(maxCapacity uint64) *DynamicTable {
	// The entry ring is left nil: the first insert sizes it (growEntries), so a
	// table that never inserts costs nothing.
	return &DynamicTable{
		arena:       make([]byte, 0, 1024),
		maxCapacity: maxCapacity,
	}
}

// Len reports the number of live entries.
func (dt *DynamicTable) Len() int { return dt.count }

// Size reports the sum of live entry sizes (RFC 9204 §3.2.1).
func (dt *DynamicTable) Size() uint64 { return dt.size }

// Capacity reports the current table capacity (Set Dynamic Table Capacity).
func (dt *DynamicTable) Capacity() uint64 { return dt.capacity }

// InsertCount reports the total number of insertions performed — the absolute
// index the next inserted entry will take (RFC 9204 §3.2, "Insert Count").
func (dt *DynamicTable) InsertCount() uint64 { return dt.insertCount }

// maxEntries is floor(maxCapacity / entryOverhead): the largest possible number
// of entries, since entryOverhead is also the smallest an entry can be. It sizes
// the Required Insert Count wraparound window (RFC 9204 §4.5.1.1).
func (dt *DynamicTable) maxEntries() uint64 { return dt.maxCapacity / entryOverhead }

// entrySize is the RFC 9204 §3.2.1 accounting size of an entry.
func entrySize(nameLen, valueLen int) uint64 {
	return uint64(nameLen) + uint64(valueLen) + entryOverhead
}

// oldestAbs is the absolute index of the oldest live entry. Valid only when
// count > 0.
func (dt *DynamicTable) oldestAbs() uint64 { return dt.insertCount - uint64(dt.count) }

// at returns the name and value for absolute index abs, or ok=false if abs is
// not currently in the table. The returned slices alias the arena and are valid
// only until the next mutation; the caller MUST NOT modify them.
func (dt *DynamicTable) at(abs uint64) (name, value []byte, ok bool) {
	if dt.count == 0 || abs < dt.oldestAbs() || abs >= dt.insertCount {
		return nil, nil, false
	}
	offset := int(abs - dt.oldestAbs())
	pos := (dt.head + offset) % len(dt.entries)
	e := dt.entries[pos]
	return dt.arena[e.nameOff : e.nameOff+e.nameLen],
		dt.arena[e.valueOff : e.valueOff+e.valueLen], true
}

// canInsertWithoutEvict reports whether an entry of the given name and value
// lengths fits under the current capacity without evicting any live entry
// (RFC 9204 §3.2.2). The encode side uses it to stay eviction-free, so an entry
// referenced by an in-flight (unacknowledged) request is never dropped.
func (dt *DynamicTable) canInsertWithoutEvict(nameLen, valueLen int) bool {
	return dt.size+entrySize(nameLen, valueLen) <= dt.capacity
}

// lookupDynamic returns the absolute index of the oldest live entry equal to
// (name, value), or ok=false when none matches. The oldest match is preferred
// because it is the most likely to already have been acknowledged by the peer's
// decoder, and thus safe for the encoder to reference. The scan is linear over
// the live entries; the table is small (bounded by the capacity, ~a few dozen
// entries), so this stays cheap and allocation-free.
func (dt *DynamicTable) lookupDynamic(name, value []byte) (abs uint64, ok bool) {
	oldest := dt.oldestAbs()
	for i := 0; i < dt.count; i++ {
		a := oldest + uint64(i)
		n, v, present := dt.at(a)
		if present && bytes.Equal(n, name) && bytes.Equal(v, value) {
			return a, true
		}
	}
	return 0, false
}

// atInsertCountRelative resolves an encoder-stream relative index (RFC 9204
// §3.2.5): relative index 0 is the most recently inserted entry, so the absolute
// index is InsertCount - relIndex - 1.
func (dt *DynamicTable) atInsertCountRelative(relIndex uint64) (name, value []byte, ok bool) {
	if relIndex >= dt.insertCount {
		return nil, nil, false
	}
	return dt.at(dt.insertCount - relIndex - 1)
}

// atBaseRelative resolves a field-line relative index against a section Base
// (RFC 9204 §3.2.5, §4.5.2/§4.5.3): the absolute index is Base - relIndex - 1.
func (dt *DynamicTable) atBaseRelative(base, relIndex uint64) (name, value []byte, ok bool) {
	if relIndex >= base {
		return nil, nil, false
	}
	return dt.at(base - relIndex - 1)
}

// atPostBase resolves a field-line post-Base index (RFC 9204 §3.2.6,
// §4.5.4/§4.5.5): the absolute index is Base + postIndex.
func (dt *DynamicTable) atPostBase(base, postIndex uint64) (name, value []byte, ok bool) {
	abs := base + postIndex
	if abs < base { // uint64 overflow
		return nil, nil, false
	}
	return dt.at(abs)
}

// setCapacity applies a Set Dynamic Table Capacity instruction (RFC 9204
// §4.3.1). The new capacity MUST NOT exceed maxCapacity (§3.2.3); a larger value
// is an encoder-stream error. Entries are evicted until the table fits.
func (dt *DynamicTable) setCapacity(n uint64) error {
	if n > dt.maxCapacity {
		return ErrEncoderStream
	}
	dt.capacity = n
	for dt.size > dt.capacity && dt.count > 0 {
		dt.evictOldest()
	}
	return nil
}

// insert appends (name, value) at the next absolute index, evicting the oldest
// entries first as needed (RFC 9204 §3.2.2). It reports false without mutating
// the table if the entry cannot fit even in an empty table (size > capacity),
// which the encoder MUST NOT emit. The name and value bytes are copied, so the
// caller may reuse or free them afterwards.
func (dt *DynamicTable) insert(name, value []byte) bool {
	es := entrySize(len(name), len(value))
	if es > dt.capacity {
		return false
	}
	for dt.size+es > dt.capacity && dt.count > 0 {
		dt.evictOldest()
	}
	nameOff := uint32(len(dt.arena))
	dt.arena = append(dt.arena, name...)
	valueOff := uint32(len(dt.arena))
	dt.arena = append(dt.arena, value...)
	dt.used += uint32(len(name) + len(value))

	// Take the slot after the newest live entry, growing the ring only when every
	// slot is live. Growing per insertion instead would let a peer drive the ring's
	// size with its instruction COUNT — an evicted entry's slot would never be
	// reused — which the capacity is supposed to bound.
	if dt.count == len(dt.entries) {
		dt.growEntries()
	}
	pos := (dt.head + dt.count) % len(dt.entries)
	dt.entries[pos] = dtEntry{nameOff, uint32(len(name)), valueOff, uint32(len(value))}
	dt.count++
	dt.size += es
	dt.insertCount++

	if uint32(len(dt.arena)) > dt.used*2 && dt.count > 0 {
		dt.compactArena()
	}
	return true
}

// growEntries doubles the entry ring and re-bases it so the oldest live entry
// sits at slot 0. It is called only when the ring is full, so it runs at most
// log2(maxCapacity/32) times over a table's life — the capacity caps how many
// entries can be live, so the doubling stops there no matter how many insertions
// the peer drives.
func (dt *DynamicTable) growEntries() {
	n := len(dt.entries) * 2
	if n < initialEntryRing {
		n = initialEntryRing
	}
	next := make([]dtEntry, n)
	// count == 0 implies the ring is empty and the loop does not run, so the
	// modulus is never evaluated against a zero-length ring.
	for i := 0; i < dt.count; i++ {
		next[i] = dt.entries[(dt.head+i)%len(dt.entries)]
	}
	dt.entries = next
	dt.head = 0
}

// evictOldest removes the entry with the lowest absolute index (RFC 9204
// §3.2.2). InsertCount is unchanged: absolute indices are permanent.
func (dt *DynamicTable) evictOldest() {
	if dt.count == 0 {
		return
	}
	e := dt.entries[dt.head]
	dt.size -= entrySize(int(e.nameLen), int(e.valueLen))
	dt.used -= e.nameLen + e.valueLen
	dt.head = (dt.head + 1) % len(dt.entries)
	dt.count--
	if dt.count == 0 {
		// The table is empty: re-base it so the next insert starts at slot 0 and
		// reuses the arena from the front.
		dt.arena = dt.arena[:0]
		dt.used = 0
		dt.head = 0
	}
}

// compactArena rewrites the arena and the entry ring so all live entries are
// densely packed from the front, reclaiming the gaps left by evicted entries.
// Amortised O(n); absolute indices are preserved.
func (dt *DynamicTable) compactArena() {
	if dt.count == 0 {
		dt.arena = dt.arena[:0]
		dt.used = 0
		dt.entries = dt.entries[:0]
		dt.head = 0
		return
	}
	newArena := make([]byte, 0, dt.used*2)
	// A fresh ring: aliasing dt.entries[:count] would corrupt a wrapped ring
	// (head != 0) because a write to slot i can clobber a source slot not yet
	// read. Compaction is rare, so the allocation is immaterial.
	newEntries := make([]dtEntry, dt.count)
	for i := 0; i < dt.count; i++ {
		pos := (dt.head + i) % len(dt.entries)
		e := dt.entries[pos]
		nameOff := uint32(len(newArena))
		newArena = append(newArena, dt.arena[e.nameOff:e.nameOff+e.nameLen]...)
		valueOff := uint32(len(newArena))
		newArena = append(newArena, dt.arena[e.valueOff:e.valueOff+e.valueLen]...)
		newEntries[i] = dtEntry{nameOff, e.nameLen, valueOff, e.valueLen}
	}
	dt.arena = newArena
	dt.used = uint32(len(newArena))
	dt.entries = newEntries
	dt.head = 0
}
