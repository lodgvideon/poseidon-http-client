package hpack

import "github.com/lodgvideon/poseidon-http-client/header"

// HeaderField is a single (name, value) pair as it appears on the wire or in a
// decoded HPACK block.
//
// An ALIAS of header.Field, not a distinct type: the vocabulary moved to the
// header package because http1 and http3 were importing this one — RFC 7541
// header compression — for nothing but this struct. An alias keeps every
// existing caller compiling and every existing value assignable.
//
// Slices are NOT owned by it:
//   - values produced by Decoder alias the decoder's scratch arena and are valid
//     only for the lifetime of the FieldVisitor call;
//   - values supplied to Encoder are copied into wire output and not retained.
type HeaderField = header.Field

// IndexingMode selects which of RFC 7541 §6.2's three literal representations
// the encoder emits for a field, and therefore whether the field is inserted
// into the dynamic table.
//
// A field that matches an existing static or dynamic entry in full is still
// encoded as an indexed field (§6.1) under IndexIncremental and IndexWithout:
// referencing an entry inserts nothing, evicts nothing, and is strictly
// smaller. IndexNever is the exception — §7.1.3 requires its representation be
// preserved, so a never-indexed field is never collapsed to an index.
// IndexingMode selects how a field is represented. An alias of
// header.IndexingMode; see HeaderField for why the vocabulary lives there.
type IndexingMode = header.IndexingMode

// The indexing modes, re-exported so existing callers keep compiling. See the
// header package for what each one means and when to reach for it.
const (
	IndexIncremental = header.IndexIncremental
	IndexWithout     = header.IndexWithout
	IndexNever       = header.IndexNever
)

// FieldVisitor is invoked once per decoded field. f.Name and f.Value are
// only valid for the duration of the call.
type FieldVisitor func(f HeaderField) error

// Default initial dynamic-table size per RFC 7540 §6.5.2 SETTINGS_HEADER_TABLE_SIZE.
const defaultMaxDynamicTableSize uint32 = 4096
