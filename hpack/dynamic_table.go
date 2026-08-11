package hpack

import "github.com/lodgvideon/poseidon-http-client/header"

// initialEntryRing is the slot count the entry ring is first sized to; it then
// doubles on demand. maxSize caps how many entries can be live, so the doubling
// terminates regardless of how many headers a peer sends.
const initialEntryRing = 32

// entryOverhead is the per-entry accounting overhead RFC 7541 §4.1 adds to
// name+value length. It is not a tunable: the insert side (entrySize) and the
// evict side (evictOldest) are the two halves of one running total, so a value
// used by one and not the other either inflates d.size until the table wedges
// permanently evicting, or underflows it past zero.
//
// QPACK defines the same overhead in RFC 9204 §3.2.1 and http3 defines it again
// for field sections in RFC 9114 §4.2.2. Those are deliberately NOT this
// constant — each is frozen by its own spec and by interop with the peer, so
// they must be free to be reasoned about separately.
const entryOverhead = header.EntryOverhead

// dynEntry stores offsets into the arena. Each entry's RFC size is
// nameLen + valueLen + entryOverhead (RFC 7541 §4.1).
type dynEntry struct {
	nameOff, nameLen   uint32
	valueOff, valueLen uint32
}

// dynamicTable holds HPACK dynamic table state for one direction of one
// connection. NOT goroutine-safe.
type dynamicTable struct {
	entries []dynEntry // logical FIFO ring; entries[head..head+count) wraps
	head    int        // index of the oldest entry
	count   int

	arena []byte // packed name+value bytes
	used  uint32 // arena bytes in active use

	maxSize uint32 // current SETTINGS_HEADER_TABLE_SIZE limit
	size    uint32 // sum of entry sizes (RFC §4.1)
}

func newDynamicTable(maxSize uint32) *dynamicTable {
	return &dynamicTable{
		entries: make([]dynEntry, 0, initialEntryRing),
		arena:   make([]byte, 0, 4096),
		maxSize: maxSize,
	}
}

func (d *dynamicTable) len() int         { return d.count }
func (d *dynamicTable) byteSize() uint32 { return d.size }

// at returns name and value for index i, where i=1 is the most recently
// added entry (HPACK §2.3.3). Returned slices alias the arena.
// Caller must NOT modify them.
func (d *dynamicTable) at(i int) (name, value []byte) {
	if i < 1 || i > d.count {
		return nil, nil
	}
	pos := (d.head + d.count - i) % len(d.entries)
	if pos < 0 {
		pos += len(d.entries)
	}
	e := d.entries[pos]
	return d.arena[e.nameOff : e.nameOff+e.nameLen],
		d.arena[e.valueOff : e.valueOff+e.valueLen]
}

func entrySize(name, value []byte) uint32 {
	return uint32(len(name)) + uint32(len(value)) + entryOverhead
}

// add inserts (name, value). If the entry exceeds maxSize, the table is
// emptied per RFC §4.4. Otherwise older entries are evicted until size fits.
func (d *dynamicTable) add(name, value []byte) {
	es := entrySize(name, value)
	if es > d.maxSize {
		d.clear()
		return
	}
	for d.size+es > d.maxSize && d.count > 0 {
		d.evictOldest()
	}
	nameOff := uint32(len(d.arena))
	d.arena = append(d.arena, name...)
	valueOff := uint32(len(d.arena))
	d.arena = append(d.arena, value...)
	d.used += uint32(len(name) + len(value))

	// Take the slot after the newest live entry, growing the ring only when every
	// slot is live. Growing per insertion instead would let a peer drive the ring's
	// size with its header COUNT — an evicted entry's slot would never be reused —
	// which maxSize is supposed to bound.
	if d.count == len(d.entries) {
		d.growEntries()
	}
	pos := (d.head + d.count) % len(d.entries)
	d.entries[pos] = dynEntry{nameOff, uint32(len(name)), valueOff, uint32(len(value))}
	d.count++
	d.size += es

	if uint32(len(d.arena)) > d.used*2 && d.count > 0 {
		d.compactArena()
	}
}

// growEntries doubles the entry ring and re-bases it so the oldest live entry
// sits at slot 0. It is called only when the ring is full, so it runs at most
// log2(maxSize/32) times over a table's life — maxSize caps how many entries can
// be live, so the doubling stops there no matter how many headers the peer sends.
func (d *dynamicTable) growEntries() {
	n := len(d.entries) * 2
	if n < initialEntryRing {
		n = initialEntryRing
	}
	next := make([]dynEntry, n)
	// count == 0 implies the ring is empty and the loop does not run, so the
	// modulus is never evaluated against a zero-length ring.
	for i := 0; i < d.count; i++ {
		next[i] = d.entries[(d.head+i)%len(d.entries)]
	}
	d.entries = next
	d.head = 0
}

func (d *dynamicTable) evictOldest() {
	if d.count == 0 {
		return
	}
	e := d.entries[d.head]
	d.size -= e.nameLen + e.valueLen + entryOverhead
	d.used -= e.nameLen + e.valueLen
	d.head = (d.head + 1) % len(d.entries)
	d.count--
	if d.count == 0 {
		d.arena = d.arena[:0]
	}
}

func (d *dynamicTable) clear() {
	d.head = 0
	d.count = 0
	d.entries = d.entries[:0]
	d.arena = d.arena[:0]
	d.used = 0
	d.size = 0
}

func (d *dynamicTable) setMaxSize(n uint32) {
	d.maxSize = n
	for d.size > d.maxSize && d.count > 0 {
		d.evictOldest()
	}
}

// compactArena rewrites d.arena so that all live entries are densely packed
// at the front. Updates entry offsets in place. Amortised O(n).
//
// As of W2, also compacts d.entries: live entries are moved to slots
// [0..count-1] and head is reset to 0. Otherwise long-lived connections
// (many adds + evictions but bounded by maxSize) accumulate dead slots
// in d.entries, leaking ~16 bytes per evicted header indefinitely.
func (d *dynamicTable) compactArena() {
	if d.count == 0 {
		d.arena = d.arena[:0]
		d.used = 0
		d.entries = d.entries[:0]
		d.head = 0
		return
	}
	newArena := make([]byte, 0, d.used*2)
	// A fresh ring: aliasing d.entries[:count] would corrupt a wrapped ring
	// (head != 0) because a write to slot i can clobber a source slot not yet
	// read. Compaction is rare, so the allocation is immaterial.
	newEntries := make([]dynEntry, d.count)
	for i := 0; i < d.count; i++ {
		pos := (d.head + i) % len(d.entries)
		e := d.entries[pos]
		nameOff := uint32(len(newArena))
		newArena = append(newArena, d.arena[e.nameOff:e.nameOff+e.nameLen]...)
		valueOff := uint32(len(newArena))
		newArena = append(newArena, d.arena[e.valueOff:e.valueOff+e.valueLen]...)
		newEntries[i] = dynEntry{nameOff, e.nameLen, valueOff, e.valueLen}
	}
	d.arena = newArena
	d.used = uint32(len(newArena))
	d.entries = newEntries
	d.head = 0
}
