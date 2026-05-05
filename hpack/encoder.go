package hpack

// Encoder encodes HPACK header blocks. Holds a dynamic table per HTTP/2
// connection. NOT goroutine-safe.
type Encoder struct {
	dt          *dynamicTable
	peerMaxSize uint32
	localLimit  uint32
	// pendingSizeUpdate, if set, makes the next encode emit a
	// "Dynamic Table Size Update" representation (RFC §6.3) at the head.
	pendingSizeUpdate uint32
	hasPendingUpdate  bool
}

func NewEncoder() *Encoder {
	return &Encoder{
		dt:          newDynamicTable(defaultMaxDynamicTableSize),
		peerMaxSize: defaultMaxDynamicTableSize,
		localLimit:  defaultMaxDynamicTableSize,
	}
}

// SetMaxDynamicTableSize handles a peer SETTINGS_HEADER_TABLE_SIZE update.
// The encoder MUST emit a Size Update on the next block (RFC §4.2).
func (e *Encoder) SetMaxDynamicTableSize(n uint32) {
	e.peerMaxSize = n
	if n < e.localLimit {
		e.localLimit = n
	}
	e.pendingSizeUpdate = e.localLimit
	e.hasPendingUpdate = true
	e.dt.setMaxSize(e.localLimit)
}

// SetMaxDynamicTableSizeLimit caps the local table size below the peer limit.
func (e *Encoder) SetMaxDynamicTableSizeLimit(n uint32) {
	if n > e.peerMaxSize {
		n = e.peerMaxSize
	}
	if n != e.localLimit {
		e.localLimit = n
		e.pendingSizeUpdate = n
		e.hasPendingUpdate = true
		e.dt.setMaxSize(n)
	}
}

// Reset clears the dynamic table and pending size update.
func (e *Encoder) Reset() {
	e.dt.clear()
	e.peerMaxSize = defaultMaxDynamicTableSize
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
		if !bytesEqual(n, name) {
			continue
		}
		if bytesEqual(v, value) {
			return uint64(i), true
		}
		if nameOnly == 0 {
			nameOnly = uint64(i)
		}
	}
	return nameOnly, false
}
