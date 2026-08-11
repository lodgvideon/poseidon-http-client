// Package header holds the header vocabulary the HTTP versions share.
//
// It exists because the type lived in hpack, and hpack is RFC 7541 — HTTP/2
// header compression. http1 imported it for one symbol and http3 for two, so
// `go list -deps ./http1 | grep hpack` printed a line and RFC 9112 appeared,
// from its import list alone, to depend on HPACK. It never did. It borrowed a
// struct, because HTTP/2 was written first and there was nothing else to share
// one with.
//
// Nothing here encodes or decodes anything. A package that compresses headers
// may import this; this imports nothing.
package header

// Field is a single (name, value) pair as it appears on the wire or after
// decoding.
//
// Slices are NOT owned by Field:
//   - values produced by a decoder alias that decoder's scratch and are valid
//     only for the lifetime of the visitor call;
//   - values handed to an encoder are copied into wire output and not retained.
type Field struct {
	Name  []byte
	Value []byte
	// Indexing selects the literal representation, and therefore whether this
	// field enters a dynamic table. The zero value indexes incrementally, which
	// is the right default for any field whose value repeats.
	//
	// It is HPACK and QPACK vocabulary rather than HTTP/1.1 vocabulary, and it
	// lives here anyway: it is a property a caller sets on a field it is about to
	// send, and splitting the struct so HTTP/1.1 could avoid one unused byte
	// would give every other package two types to convert between.
	Indexing IndexingMode
}

// IndexingMode selects how a field is represented in a compressed header block.
type IndexingMode uint8

const (
	// IndexIncremental encodes a literal with incremental indexing (RFC 7541
	// §6.2.1) and inserts the field into the dynamic table, so later occurrences
	// of the same name and value compress to a single index. The zero value.
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

// EntryOverhead is the per-entry accounting overhead a dynamic table adds to
// the raw name and value bytes: 32, for the entry structure and the table's own
// references.
//
// The same number and the same formula in both compressed HTTP versions —
// RFC 7541 §4.1 for HPACK and RFC 9204 §3.2.1 for QPACK — which is why the size
// rule is vocabulary rather than codec detail.
const EntryOverhead = 32

// Size returns the field's dynamic-table entry size: name + value +
// EntryOverhead.
func (f Field) Size() uint32 {
	return uint32(len(f.Name)) + uint32(len(f.Value)) + EntryOverhead
}

// Sensitive reports whether the field is marked never-indexed, the
// representation that tells an intermediary it must not index the field either
// (RFC 7541 §6.2.3, RFC 9204 §4.5.4).
func (f Field) Sensitive() bool { return f.Indexing == IndexNever }
