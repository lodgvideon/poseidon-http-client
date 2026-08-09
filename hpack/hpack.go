package hpack

// HeaderField represents a single (name, value) pair as it appears on the
// wire or in a decoded HPACK block. Slices are NOT owned by HeaderField:
//   - For values produced by Decoder, slices alias the decoder's scratch
//     arena and are valid only for the lifetime of the FieldVisitor call.
//   - For values supplied to Encoder, the encoder copies bytes into wire
//     output and does not retain references.
type HeaderField struct {
	Name  []byte
	Value []byte
	// Indexing selects the literal representation, and therefore whether this
	// field enters the dynamic table. The zero value indexes incrementally,
	// which is the right default for any field whose value repeats.
	Indexing IndexingMode
}

// IndexingMode selects which of RFC 7541 §6.2's three literal representations
// the encoder emits for a field, and therefore whether the field is inserted
// into the dynamic table.
//
// A field that matches an existing static or dynamic entry in full is still
// encoded as an indexed field (§6.1) under IndexIncremental and IndexWithout:
// referencing an entry inserts nothing, evicts nothing, and is strictly
// smaller. IndexNever is the exception — §7.1.3 requires its representation be
// preserved, so a never-indexed field is never collapsed to an index.
type IndexingMode uint8

const (
	// IndexIncremental encodes a literal with incremental indexing (§6.2.1) and
	// inserts the field into the dynamic table, so later occurrences of the same
	// name and value compress to a single index. The zero value.
	IndexIncremental IndexingMode = iota

	// IndexWithout encodes a literal without indexing (§6.2.2): the field is not
	// inserted, so it evicts nothing. This is for a field whose value varies per
	// request — a timeout, a request or trace id, an ETag, a Date — where
	// inserting would evict an entry that could still be matched in exchange for
	// one that never will. It carries no security meaning; use IndexNever for
	// that.
	IndexWithout

	// IndexNever encodes a literal never indexed (§6.2.3). Like IndexWithout it
	// does not insert, but it additionally signals to intermediaries that they
	// must not index the field either, and must preserve this representation when
	// forwarding (§7.1.3). Reserved for values whose exposure to an intermediary
	// matters — credentials, cookies, authorization tokens. Do not use it merely
	// to avoid an insertion; that is what IndexWithout is for.
	IndexNever
)

// Sensitive reports whether the field is never-indexed (§6.2.3).
func (f HeaderField) Sensitive() bool { return f.Indexing == IndexNever }

// Size returns the entry size as defined in RFC 7541 §4.1 (used for
// dynamic table accounting).
func (f HeaderField) Size() uint32 {
	return uint32(len(f.Name)) + uint32(len(f.Value)) + entryOverhead
}

// FieldVisitor is invoked once per decoded field. f.Name and f.Value are
// only valid for the duration of the call.
type FieldVisitor func(f HeaderField) error

// Default initial dynamic-table size per RFC 7540 §6.5.2 SETTINGS_HEADER_TABLE_SIZE.
const defaultMaxDynamicTableSize uint32 = 4096
