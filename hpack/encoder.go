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

// Reset clears the dynamic table and pending size update.
func (e *Encoder) Reset() {
	e.dt.clear()
	e.peerMaxSize = defaultMaxDynamicTableSize
	e.callerLimit = defaultMaxDynamicTableSize
	e.localLimit = defaultMaxDynamicTableSize
	e.pendingSizeUpdate = 0
	e.hasPendingUpdate = false
}

// EncodeBlock encodes a slice of fields and appends the result to dst.
func (e *Encoder) EncodeBlock(dst []byte, fields []HeaderField) []byte {
	dst = e.maybeEmitSizeUpdate(dst)
	for i := range fields {
		dst = e.writeFieldAlreadyFlushedSize(dst, fields[i].Name, fields[i].Value, fields[i].Sensitive)
	}
	return dst
}

// WriteField encodes a single field and appends to dst.
func (e *Encoder) WriteField(dst, name, value []byte, sensitive bool) []byte {
	dst = e.maybeEmitSizeUpdate(dst)
	return e.writeFieldAlreadyFlushedSize(dst, name, value, sensitive)
}

func (e *Encoder) maybeEmitSizeUpdate(dst []byte) []byte {
	if !e.hasPendingUpdate {
		return dst
	}
	dst = EncodeInteger(dst, 5, 0x20, uint64(e.pendingSizeUpdate))
	e.hasPendingUpdate = false
	return dst
}

func (e *Encoder) writeFieldAlreadyFlushedSize(dst, name, value []byte, sensitive bool) []byte {
	staticIdx, fullStatic := staticIndex(name, value)
	if fullStatic && !sensitive {
		return EncodeInteger(dst, 7, 0x80, staticIdx)
	}

	dynIdx, fullDyn := e.dynamicLookup(name, value)
	if fullDyn && !sensitive {
		return EncodeInteger(dst, 7, 0x80, dynIdx+uint64(staticTableLen))
	}

	var nameIdx uint64
	if staticIdx != 0 {
		nameIdx = staticIdx
	} else if dynIdx != 0 {
		nameIdx = dynIdx + uint64(staticTableLen)
	}

	switch {
	case sensitive:
		dst = EncodeInteger(dst, 4, 0x10, nameIdx)
	default:
		dst = EncodeInteger(dst, 6, 0x40, nameIdx)
	}
	if nameIdx == 0 {
		dst = encodeStringLiteral(dst, name, false)
	}
	dst = encodeStringLiteral(dst, value, false)
	if !sensitive {
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
