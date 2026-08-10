package hpack

import "bytes"

// Encoder encodes HPACK header blocks. Holds a dynamic table per HTTP/2
// connection. NOT goroutine-safe.
type Encoder struct {
	dt          *dynamicTable
	peerMaxSize uint32 // most recent SETTINGS_HEADER_TABLE_SIZE from peer
	callerLimit uint32 // caller-configured cap (SetMaxDynamicTableSizeLimit)
	localLimit  uint32 // effective limit = min(peerMaxSize, callerLimit)
	// pendingSizeUpdate, if set, makes the next encode emit a
	// "Dynamic Table Size Update" representation (RFC §6.3) at the head.
	pendingSizeUpdate uint32
	hasPendingUpdate  bool
}

// NewEncoder returns a fresh HPACK encoder with the default dynamic
// table size of 4096 octets (RFC 7541).
func NewEncoder() *Encoder {
	return &Encoder{
		dt:          newDynamicTable(defaultMaxDynamicTableSize),
		peerMaxSize: defaultMaxDynamicTableSize,
		callerLimit: defaultMaxDynamicTableSize,
		localLimit:  defaultMaxDynamicTableSize,
	}
}

// SetMaxDynamicTableSize handles a peer SETTINGS_HEADER_TABLE_SIZE update.
// The encoder recomputes the effective local limit as min(peer, caller)
// and emits a Size Update on the next block (RFC §4.2). Peer increases
// are honored — earlier versions silently capped at the first observed
// peer value, leaving compression ratio degraded for the connection's
// lifetime.
func (e *Encoder) SetMaxDynamicTableSize(n uint32) {
	e.peerMaxSize = n
	e.recomputeLocalLimit()
}

// SetMaxDynamicTableSizeLimit caps the local table size below the peer
// limit. The effective local limit is min(peer, caller).
func (e *Encoder) SetMaxDynamicTableSizeLimit(n uint32) {
	e.callerLimit = n
	e.recomputeLocalLimit()
}

// recomputeLocalLimit applies localLimit = min(peerMaxSize, callerLimit)
// and schedules a SETTINGS-update emit if it changed.
func (e *Encoder) recomputeLocalLimit() {
	newLimit := e.peerMaxSize
	if e.callerLimit < newLimit {
		newLimit = e.callerLimit
	}
	if newLimit == e.localLimit {
		return
	}
	e.localLimit = newLimit
	e.pendingSizeUpdate = newLimit
	e.hasPendingUpdate = true
	e.dt.setMaxSize(newLimit)
}

// Reset returns the encoder to its as-new state: the dynamic table is emptied,
// the peer's advertised limit is forgotten, and a cap set with
// SetMaxDynamicTableSizeLimit is discarded. The encoder can then be reused on a
// new connection as if freshly constructed.
//
// That last part is a deliberate asymmetry with Decoder.Reset, which preserves
// its caller configuration. Reset here means NewEncoder, so a caller reusing the
// object must re-apply its cap; a caller that wants the cap to survive should
// keep the setting and call SetMaxDynamicTableSizeLimit again.
//
// The setMaxSize call is what makes the wipe honest. dynamicTable.clear resets
// the entries and the arena but not maxSize, so resetting localLimit alone left
// the encoder indexing against a budget of 4096 into a table that still evicted
// at the old cap — compression quietly degraded for the life of the connection,
// with no size update pending to tell anyone. Every other path keeps
// localLimit == dt.maxSize through recomputeLocalLimit; this one has to too.
func (e *Encoder) Reset() {
	e.dt.clear()
	e.peerMaxSize = defaultMaxDynamicTableSize
	e.callerLimit = defaultMaxDynamicTableSize
	e.localLimit = defaultMaxDynamicTableSize
	e.pendingSizeUpdate = 0
	e.hasPendingUpdate = false
	e.dt.setMaxSize(defaultMaxDynamicTableSize)
}

// EncodeBlock encodes a slice of fields and appends the result to dst.
func (e *Encoder) EncodeBlock(dst []byte, fields []HeaderField) []byte {
	dst = e.maybeEmitSizeUpdate(dst)
	for i := range fields {
		dst = e.writeFieldAlreadyFlushedSize(dst, fields[i].Name, fields[i].Value, fields[i].Indexing)
	}
	return dst
}

// WriteField encodes a single field and appends to dst. mode selects the
// literal representation — see IndexingMode.
func (e *Encoder) WriteField(dst, name, value []byte, mode IndexingMode) []byte {
	dst = e.maybeEmitSizeUpdate(dst)
	return e.writeFieldAlreadyFlushedSize(dst, name, value, mode)
}

func (e *Encoder) maybeEmitSizeUpdate(dst []byte) []byte {
	if !e.hasPendingUpdate {
		return dst
	}
	dst = EncodeInteger(dst, 5, 0x20, uint64(e.pendingSizeUpdate))
	e.hasPendingUpdate = false
	return dst
}

func (e *Encoder) writeFieldAlreadyFlushedSize(dst, name, value []byte, mode IndexingMode) []byte {
	// A full match still collapses to an indexed field under IndexWithout:
	// referencing an existing entry inserts nothing and evicts nothing, so it
	// respects the caller's "keep this out of the table" intent while being
	// strictly smaller. IndexNever is excluded because §7.1.3 requires the
	// never-indexed representation be preserved.
	indexable := mode != IndexNever
	staticIdx, fullStatic := staticIndex(name, value)
	if fullStatic && indexable {
		return EncodeInteger(dst, 7, 0x80, staticIdx)
	}

	dynIdx, fullDyn := e.dynamicLookup(name, value)
	if fullDyn && indexable {
		return EncodeInteger(dst, 7, 0x80, dynIdx+uint64(staticTableLen))
	}

	var nameIdx uint64
	if staticIdx != 0 {
		nameIdx = staticIdx
	} else if dynIdx != 0 {
		nameIdx = dynIdx + uint64(staticTableLen)
	}

	switch mode {
	case IndexNever:
		dst = EncodeInteger(dst, 4, 0x10, nameIdx) // §6.2.3 literal never indexed
	case IndexWithout:
		dst = EncodeInteger(dst, 4, 0x00, nameIdx) // §6.2.2 literal without indexing
	default:
		dst = EncodeInteger(dst, 6, 0x40, nameIdx) // §6.2.1 literal with incremental indexing
	}
	if nameIdx == 0 {
		dst = encodeStringLiteral(dst, name, false)
	}
	dst = encodeStringLiteral(dst, value, false)
	if mode == IndexIncremental {
		e.dt.add(name, value)
	}
	return dst
}

func (e *Encoder) dynamicLookup(name, value []byte) (uint64, bool) {
	var nameOnly uint64
	for i := 1; i <= e.dt.len(); i++ {
		n, v := e.dt.at(i)
		if !bytes.Equal(n, name) {
			continue
		}
		if bytes.Equal(v, value) {
			return uint64(i), true
		}
		if nameOnly == 0 {
			nameOnly = uint64(i)
		}
	}
	return nameOnly, false
}
