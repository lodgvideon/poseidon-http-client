package conn

import (
	"slices"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// HeaderBlock owns one decoded header block: the bytes behind every field's
// Name and Value, AND the field slice viewing them. It is what a StreamEvent
// carrying headers hands to its consumer, and Release is how the consumer gives
// it back.
//
// The two used to be separate things with one owner. The bytes were a pooled
// *[]byte the caller had to Put into a pool it fetched through
// conn.GetHeaderSlabPool(), and the field slice was a plain make() per block —
// heap memory nobody had to think about. That split cost an allocation per
// header block (one per request in conn, two per RPC in grpc) for no benefit,
// because the two already had the same required lifetime everywhere: every
// Name and Value points INTO the bytes, so a consumer keeping the fields had to
// keep the bytes anyway. Pooling them as one object changes when nothing may be
// touched; it only stops one of the two being rebuilt from the heap each time.
//
// The lifetime rule is unchanged and is the same one the old slab carried:
// everything reachable from a block — the fields, their names, their values —
// is valid until Release, and belongs to whoever draws the block next
// afterwards.
type HeaderBlock struct {
	buf    []byte
	fields []header.Field
}

// headerBlockPool recycles decoded header blocks. Consumers return them through
// StreamEvent.Release or HeaderBlock.Release; the pool itself is not exported,
// which is the point — it used to be, as GetHeaderSlabPool, and "Put the
// pointer, not the slice, into this specific pool" was a rule three separate
// callers had to reimplement. The one that got it wrong was conn's own
// benchmark suite, which silently misreported every allocation figure conn had
// ever published about itself (#574).
var headerBlockPool = sync.Pool{
	New: func() any {
		return &HeaderBlock{
			// Sized so a typical response header block settles without a
			// warm-up regrow, matching what the byte slab pool used to seed.
			buf:    make([]byte, 0, 512),
			fields: make([]header.Field, 0, 16),
		}
	},
}

// copyFieldsToBlock copies fields into a pooled block and returns it. The
// returned block's Fields are the caller's to deliver; Release returns
// everything.
//
// Every Name and Value is a THREE-index slice, capacity clamped to its own
// length. That is the load-bearing detail: the bytes of every field share one
// backing array, so a two-index slice would leave capacity running to the end of
// the buffer, and a caller appending to one header's value would silently
// overwrite the next header's bytes — no copy, no error, just another request's
// headers rewritten in place.
//
// It exists because that clamp was applied to the response path and not to the
// push-promise path, which was written as "same pattern as emitHeaderBlock" and
// then diverged in two ways: two-index slices, and Indexing dropped on the
// floor. One function, so the third caller cannot pick the wrong one to imitate.
//
// Both buffers are grown to fit the whole block BEFORE anything is appended, so
// a reallocation halfway through cannot leave the first fields viewing one array
// and the rest viewing another. A split block would still READ correctly — each
// field keeps its own array alive — but one block in one array is what makes the
// clamp above easy to check.
func copyFieldsToBlock(fields []header.Field) *HeaderBlock {
	b, _ := headerBlockPool.Get().(*HeaderBlock)
	b.buf, b.fields = b.buf[:0], b.fields[:0]

	n := 0
	for i := range fields {
		n += len(fields[i].Name) + len(fields[i].Value)
	}
	b.buf = slices.Grow(b.buf, n)
	b.fields = slices.Grow(b.fields, len(fields))

	for _, f := range fields {
		nameOff := len(b.buf)
		b.buf = append(b.buf, f.Name...)
		valOff := len(b.buf)
		b.buf = append(b.buf, f.Value...)
		endOff := len(b.buf)
		b.fields = append(b.fields, header.Field{
			Name:     b.buf[nameOff:valOff:valOff],
			Value:    b.buf[valOff:endOff:endOff],
			Indexing: f.Indexing,
		})
	}
	return b
}

// NewHeaderBlock copies fields into a pooled block, for a caller synthesising a
// StreamEvent this package did not decode. client's HTTP/1.1 transport builds
// EventHeaders that way, and the protoStream interface it satisfies is open to
// other implementations.
//
// A synthesised event may equally leave Block nil and point Headers at storage
// the caller owns — Release is nil-safe, so a consumer written against the
// decoded path works either way. Use this when the storage should be pooled and
// the consumer's Release should reclaim it.
func NewHeaderBlock(fields []header.Field) *HeaderBlock {
	return copyFieldsToBlock(fields)
}

// Fields returns the decoded fields. They view the block's own bytes, so they
// are valid until Release and no longer.
func (b *HeaderBlock) Fields() []header.Field {
	if b == nil {
		return nil
	}
	return b.fields
}

// Release returns the block to the pool. It is nil-safe, and it is the ONLY
// return site — which is what rules out a double-Put, since sync.Pool accepts
// the same pointer twice without complaint and the race detector sees nothing
// either. A block Released twice is not a data race; it is one buffer with two
// live owners, and the damage lands later as one response's headers appearing
// inside another's.
//
// Releasing is a consumer's obligation only for a block it was DELIVERED.
// Blocks on events that were queued and never delivered are abandoned to the
// garbage collector by resetForPoolLocked rather than returned here, precisely
// so a delivered event's consumer stays the single owner.
func (b *HeaderBlock) Release() {
	if b == nil {
		return
	}
	// Clear the field headers before parking them. They point at whatever the
	// peer sent — a set-cookie or an authorization value among it — and a
	// truncated-but-uncleared slice would leave a struct sitting in the pool
	// holding live references to those bytes for as long as it sat unclaimed.
	//
	// The bytes themselves are truncated rather than zeroed. Nothing can read
	// them: every Name and Value is clamped to its own length, so the next
	// block's fields view only what that block appended.
	clear(b.fields)
	b.buf, b.fields = b.buf[:0], b.fields[:0]
	headerBlockPool.Put(b)
}
