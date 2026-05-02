package hpack

// HeaderField represents a single (name, value) pair as it appears on the
// wire or in a decoded HPACK block. Slices are NOT owned by HeaderField:
//   - For values produced by Decoder, slices alias the decoder's scratch
//     arena and are valid only for the lifetime of the FieldVisitor call.
//   - For values supplied to Encoder, the encoder copies bytes into wire
//     output and does not retain references.
type HeaderField struct {
	Name      []byte
	Value     []byte
	Sensitive bool // forces never-indexed (RFC §6.2.3)
}

// Size returns the entry size as defined in RFC 7541 §4.1 (used for
// dynamic table accounting).
func (f HeaderField) Size() uint32 {
	return uint32(len(f.Name)) + uint32(len(f.Value)) + 32
}

// FieldVisitor is invoked once per decoded field. f.Name and f.Value are
// only valid for the duration of the call.
type FieldVisitor func(f HeaderField) error

// Default initial dynamic-table size per RFC 7540 §6.5.2 SETTINGS_HEADER_TABLE_SIZE.
const defaultMaxDynamicTableSize uint32 = 4096
