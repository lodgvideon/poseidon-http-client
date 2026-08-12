package hpack

import "errors"

// Decoder decodes HPACK header blocks. Holds a dynamic table per HTTP/2
// connection. NOT goroutine-safe.
type Decoder struct {
	dt          *dynamicTable
	maxLocal    uint32
	scratch     []byte
	maxListSize uint32
	listTotal   uint64 // running header-list size for the active block
	streaming   bool
	pending     []byte
}

// NewDecoder returns a fresh HPACK decoder with the default dynamic
// table size of 4096 octets (RFC 7541).
func NewDecoder() *Decoder {
	return &Decoder{
		dt:       newDynamicTable(defaultMaxDynamicTableSize),
		maxLocal: defaultMaxDynamicTableSize,
		scratch:  make([]byte, 0, 4096),
	}
}

// SetMaxDynamicTableSize updates the local SETTINGS_HEADER_TABLE_SIZE.
func (d *Decoder) SetMaxDynamicTableSize(n uint32) {
	d.maxLocal = n
	d.dt.setMaxSize(n)
}

// SetMaxHeaderListSize sets the local SETTINGS_MAX_HEADER_LIST_SIZE
// announced to the peer.
func (d *Decoder) SetMaxHeaderListSize(n uint32) { d.maxListSize = n }

// Reset clears the dynamic table and any streaming-decode state.
func (d *Decoder) Reset() {
	d.dt.clear()
	d.scratch = d.scratch[:0]
	d.streaming = false
	d.pending = d.pending[:0]
	d.listTotal = 0
}

// DecodeBlock parses a complete header block fragment and emits one call per
// field via visit. Field slices alias d.scratch and are valid only for the
// duration of the visit call. When SetMaxHeaderListSize was called with
// a non-zero value, the running cumulative HeaderField.Size() is checked
// per visit and ErrHeaderListTooLarge is returned if exceeded
// (RFC 7541 §6.5.2 / RFC 7540 §6.5.2 — DoS gate).
func (d *Decoder) DecodeBlock(block []byte, visit FieldVisitor) error {
	d.scratch = d.scratch[:0]
	d.listTotal = 0
	return d.decodeFragment(block, d.guardedVisitor(visit))
}

// guardedVisitor wraps visit with a running-total check against
// maxListSize. If maxListSize == 0 the original visit is returned
// unwrapped (no overhead). The running total is held on the Decoder
// itself (d.listTotal) so streaming Feed calls accumulate across
// fragments within a single Begin/Finish session.
func (d *Decoder) guardedVisitor(visit FieldVisitor) FieldVisitor {
	if d.maxListSize == 0 {
		return visit
	}
	limit := uint64(d.maxListSize)
	return func(f HeaderField) error {
		d.listTotal += uint64(f.Size())
		if d.listTotal > limit {
			return ErrHeaderListTooLarge
		}
		return visit(f)
	}
}

// decodeFragment decodes every representation in src, one after another.
//
// The representation switch itself lives in decodeOne. It used to live here too,
// byte for byte — the same five cases (§6.1 indexed, §6.2.1, §6.3 size update,
// §6.2.3 never-indexed, §6.2.2 without-indexing) written twice, differing only in
// loop-versus-single control flow, with a comment in one pointing at the other as
// the place to keep in sync by hand. A new representation, or a fix to a check,
// had to land in both.
func (d *Decoder) decodeFragment(src []byte, visit FieldVisitor) error {
	for len(src) > 0 {
		n, err := d.decodeOne(src, visit)
		if err != nil {
			return err
		}
		src = src[n:]
	}
	return nil
}

func (d *Decoder) lookup(idx uint64) (name, value []byte, err error) {
	if idx == 0 {
		return nil, nil, ErrInvalidIndex
	}
	if idx <= staticTableLen {
		e := staticTable[idx]
		return e.name, e.value, nil
	}
	dynIdx := int(idx - staticTableLen)
	if dynIdx > d.dt.len() {
		return nil, nil, ErrInvalidIndex
	}
	n, v := d.dt.at(dynIdx)
	return n, v, nil
}

func (d *Decoder) parseLiteral(src []byte, namePrefixBits uint8) (name, value []byte, consumed int, err error) {
	idx, n, err := DecodeInteger(src, namePrefixBits)
	if err != nil {
		return nil, nil, 0, err
	}
	consumed = n
	if idx == 0 {
		nameStart := len(d.scratch)
		// Assign d.scratch only on success. decodeStringLiteral returns a nil
		// slice on truncation/error; writing that back would replace d.scratch
		// (len N) with nil (cap 0), and decodePartial's rollback
		// `d.scratch = d.scratch[:scratchSnap]` then panics with a slice bounds
		// error on a peer-crafted truncated literal. It never mutates the existing
		// bytes before erroring, so keeping the old d.scratch on error is correct.
		grown, nb, serr := decodeStringLiteral(d.scratch, src[consumed:])
		if serr != nil {
			return nil, nil, 0, serr
		}
		d.scratch = grown
		consumed += nb
		name = d.scratch[nameStart:]
	} else {
		var refName []byte
		refName, _, err = d.lookup(idx)
		if err != nil {
			return nil, nil, 0, err
		}
		nameStart := len(d.scratch)
		d.scratch = append(d.scratch, refName...)
		name = d.scratch[nameStart:]
	}
	valueStart := len(d.scratch)
	// Same as the name above: keep d.scratch intact on a truncated value so the
	// streaming rollback in decodePartial cannot reslice a nil scratch.
	grown, vb, serr := decodeStringLiteral(d.scratch, src[consumed:])
	if serr != nil {
		return nil, nil, 0, serr
	}
	d.scratch = grown
	consumed += vb
	value = d.scratch[valueStart:]
	return name, value, consumed, nil
}

// Begin starts a streaming decode session.
func (d *Decoder) Begin() {
	d.streaming = true
	d.pending = d.pending[:0]
	d.scratch = d.scratch[:0]
	d.listTotal = 0
}

// Feed appends fragment, then decodes and emits as many complete
// representations as possible. Truncated tail remains buffered.
//
// Returns ErrNotStreaming if Begin has not opened a session — a caller
// sequencing error, distinct from the wire-format errors the decode itself
// returns.
func (d *Decoder) Feed(fragment []byte, visit FieldVisitor) error {
	if !d.streaming {
		return ErrNotStreaming
	}
	d.pending = append(d.pending, fragment...)
	consumed, err := d.decodePartial(d.pending, d.guardedVisitor(visit))
	if err != nil {
		return err
	}
	d.pending = append(d.pending[:0], d.pending[consumed:]...)
	return nil
}

// Finish validates that the streaming buffer is drained and resets state.
//
// Returns ErrNotStreaming if no session is open, and ErrTruncated if one is but
// the peer's field block ended mid-representation — the first is the caller's
// mistake, the second is the wire's.
func (d *Decoder) Finish() error {
	if !d.streaming {
		return ErrNotStreaming
	}
	defer func() {
		d.streaming = false
		d.pending = d.pending[:0]
	}()
	if len(d.pending) > 0 {
		return ErrTruncated
	}
	return nil
}

// decodePartial consumes as many complete representations as fit in src.
// Truncation in the middle of a representation is NOT an error.
func (d *Decoder) decodePartial(src []byte, visit FieldVisitor) (int, error) {
	consumed := 0
	for consumed < len(src) {
		scratchSnap := len(d.scratch)
		n, err := d.decodeOne(src[consumed:], visit)
		if errors.Is(err, ErrTruncated) {
			d.scratch = d.scratch[:scratchSnap]
			return consumed, nil
		}
		if err != nil {
			return consumed, err
		}
		consumed += n
	}
	return consumed, nil
}

// decodeOne decodes a single representation; returns bytes consumed.
func (d *Decoder) decodeOne(src []byte, visit FieldVisitor) (int, error) {
	if len(src) == 0 {
		return 0, ErrTruncated
	}
	b := src[0]
	switch {
	case b&0x80 != 0:
		idx, n, err := DecodeInteger(src, 7)
		if err != nil {
			return 0, err
		}
		name, value, err := d.lookup(idx)
		if err != nil {
			return 0, err
		}
		if err := visit(HeaderField{Name: name, Value: value}); err != nil {
			return 0, err
		}
		return n, nil
	case b&0xc0 == 0x40:
		name, value, n, err := d.parseLiteral(src, 6)
		if err != nil {
			return 0, err
		}
		d.dt.add(name, value)
		if err := visit(HeaderField{Name: name, Value: value}); err != nil {
			return 0, err
		}
		return n, nil
	case b&0xe0 == 0x20:
		nval, consumed, err := DecodeInteger(src, 5)
		if err != nil {
			return 0, err
		}
		if uint32(nval) > d.maxLocal {
			return 0, ErrTableSizeUpdate
		}
		d.dt.setMaxSize(uint32(nval))
		return consumed, nil
	case b&0xf0 == 0x10:
		name, value, n, err := d.parseLiteral(src, 4)
		if err != nil {
			return 0, err
		}
		if err := visit(HeaderField{Name: name, Value: value, Indexing: IndexNever}); err != nil {
			return 0, err
		}
		return n, nil
	case b&0xf0 == 0x00:
		// 6.2.2: literal without indexing. Reported as IndexWithout so the
		// representation the peer chose survives the decode; before IndexingMode
		// existed this was indistinguishable from §6.2.1.
		name, value, n, err := d.parseLiteral(src, 4)
		if err != nil {
			return 0, err
		}
		if err := visit(HeaderField{Name: name, Value: value, Indexing: IndexWithout}); err != nil {
			return 0, err
		}
		return n, nil
	default:
		return 0, ErrInvalidPrefix
	}
}
