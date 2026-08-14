package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// Every header field delivered to a caller is a view into one shared byte
// buffer. That is cheap and it is safe only while each view's capacity stops at
// its own end: a two-index slice would run to the end of the buffer, so
// appending to one header's value would overwrite the next header's bytes in
// place — silently, with no copy and no error.
//
// The response path clamped the capacity. The push-promise path, written as
// "same pattern as emitHeaderBlock", did not, and also dropped Indexing. Both
// now go through copyFieldsToBlock, and these are the gates on what that
// function has to guarantee. The buffer is pooled alongside the field slice
// viewing it now rather than on its own, which changes where the bytes live and
// nothing at all about this requirement.

// TestCopyFieldsToBlock_AppendCannotReachTheNextField is the gate for the bug. It
// does what a caller is entitled to do — append to a header it was given — and
// checks the neighbour is untouched.
func TestCopyFieldsToBlock_AppendCannotReachTheNextField(t *testing.T) {
	in := []header.Field{
		{Name: []byte("first"), Value: []byte("AAAA")},
		{Name: []byte("second"), Value: []byte("BBBB")},
		{Name: []byte("third"), Value: []byte("CCCC")},
	}
	blk := copyFieldsToBlock(in)
	defer blk.Release()
	copied := blk.Fields()

	// A caller appending to the first field's value. With a clamped capacity this
	// allocates a fresh array; without one it writes straight over "second".
	_ = append(copied[0].Value, "XXXXXXXXXXXXXXXX"...) //nolint:gocritic // the point is the side effect on the block

	for i, want := range in {
		if string(copied[i].Name) != string(want.Name) {
			t.Errorf("field %d name = %q after a neighbour was appended to, want %q — the "+
				"append reached into the shared buffer", i, copied[i].Name, want.Name)
		}
		if string(copied[i].Value) != string(want.Value) {
			t.Errorf("field %d value = %q after a neighbour was appended to, want %q",
				i, copied[i].Value, want.Value)
		}
	}
}

// TestCopyFieldsToBlock_CapacityIsClamped states the same requirement directly,
// so a failure names the cause rather than the symptom.
func TestCopyFieldsToBlock_CapacityIsClamped(t *testing.T) {
	in := []header.Field{
		{Name: []byte("a"), Value: []byte("bb")},
		{Name: []byte("ccc"), Value: []byte("dddd")},
	}
	blk := copyFieldsToBlock(in)
	defer blk.Release()
	copied := blk.Fields()

	for i := range copied {
		if got, want := cap(copied[i].Name), len(copied[i].Name); got != want {
			t.Errorf("field %d Name has cap %d for len %d — an append would spill into the "+
				"next field's bytes", i, got, want)
		}
		if got, want := cap(copied[i].Value), len(copied[i].Value); got != want {
			t.Errorf("field %d Value has cap %d for len %d", i, got, want)
		}
	}
}

// TestCopyFieldsToBlock_PreservesIndexing covers the other half of the
// divergence: the push path set only Name and Value, so a promised header
// arrived with its indexing disposition zeroed.
func TestCopyFieldsToBlock_PreservesIndexing(t *testing.T) {
	in := []header.Field{
		{Name: []byte("plain"), Value: []byte("1")},
		{Name: []byte("sensitive"), Value: []byte("2"), Indexing: header.IndexNever},
	}
	blk := copyFieldsToBlock(in)
	defer blk.Release()
	copied := blk.Fields()

	for i := range in {
		if copied[i].Indexing != in[i].Indexing {
			t.Errorf("field %d Indexing = %v, want %v — a never-indexed header that arrives "+
				"without that mark can be re-emitted into a dynamic table",
				i, copied[i].Indexing, in[i].Indexing)
		}
	}
}

// TestCopyFieldsToBlock_CopiesTheBytes is the basic contract: the result must not
// alias the input, or the caller would be handed the decoder's scratch.
func TestCopyFieldsToBlock_CopiesTheBytes(t *testing.T) {
	name := []byte("k")
	value := []byte("v")
	blk := copyFieldsToBlock([]header.Field{{Name: name, Value: value}})
	defer blk.Release()
	copied := blk.Fields()

	name[0] = 'Z'
	value[0] = 'Z'
	if string(copied[0].Name) != "k" || string(copied[0].Value) != "v" {
		t.Errorf("got (%q, %q) after the source was overwritten, want (\"k\", \"v\") — the "+
			"fields alias the decoder's scratch instead of the block",
			copied[0].Name, copied[0].Value)
	}
}

// TestCopyFieldsToBlock_Empty pins that no fields is not a special case for the
// caller: it still gets a block it can give back.
func TestCopyFieldsToBlock_Empty(t *testing.T) {
	blk := copyFieldsToBlock(nil)
	if blk == nil {
		t.Fatal("no block returned for an empty field list; the caller has nothing to give back")
	}
	defer blk.Release()
	if got := blk.Fields(); len(got) != 0 {
		t.Errorf("got %d fields for an empty input", len(got))
	}
}

// --- ownership -------------------------------------------------------------
//
// The block is pooled, so the questions worth pinning are all about who gives it
// back and how often. sync.Pool accepts the same pointer twice without
// complaint, and -race sees nothing either — a double-Put is not a data race,
// just one buffer with two live owners whose damage lands later as one
// response's headers appearing inside another's.

// TestHeaderBlock_ReleaseIsNilSafe covers the shape that makes `defer
// ev.Release()` usable on every arm of a receive loop: most events carry no
// header block at all, and an arm that had to know which is which would be the
// per-caller bookkeeping this type exists to delete.
func TestHeaderBlock_ReleaseIsNilSafe(t *testing.T) {
	var b *HeaderBlock
	b.Release()            // must not panic
	if b.Fields() != nil { // and must stay readable
		t.Error("a nil block returned non-nil Fields")
	}
	ev := StreamEvent{Type: EventData} // no block on this one
	ev.Release()
}

// TestStreamEvent_ReleaseTwiceIsNotADoublePut pins the one mistake the new API
// invites and the old one did not. `defer ev.Release()` beside an explicit
// release on an early return is an ordinary thing to write; with the pointer
// left in place the second call would hand the same block to the pool twice.
func TestStreamEvent_ReleaseTwiceIsNotADoublePut(t *testing.T) {
	blk := copyFieldsToBlock([]header.Field{{Name: []byte("k"), Value: []byte("v")}})
	ev := StreamEvent{Type: EventHeaders, Headers: blk.Fields(), Block: blk}

	ev.Release()
	if ev.Block != nil {
		t.Error("Release left Block set — a second Release would put the same block twice")
	}
	if ev.Headers != nil {
		t.Error("Release left Headers pointing into a block the pool now owns")
	}
	ev.Release() // must be a no-op, not a second Put
}

// TestHeaderBlock_ReleaseClearsTheFields pins that a parked block holds no
// references to what the peer sent. The bytes are truncated rather than zeroed —
// nothing can read them through a clamped field — but the field headers are
// pointers, and leaving them set would keep a response's set-cookie or
// authorization value reachable from the pool for as long as the block sat
// unclaimed.
func TestHeaderBlock_ReleaseClearsTheFields(t *testing.T) {
	blk := copyFieldsToBlock([]header.Field{
		{Name: []byte("authorization"), Value: []byte("Bearer hunter2")},
	})
	blk.Release()

	if n := len(blk.Fields()); n != 0 {
		t.Fatalf("Fields still has length %d after Release", n)
	}
	for i, f := range blk.fields[:cap(blk.fields)] {
		if f.Name != nil || f.Value != nil {
			t.Errorf("parked fields[%d] still points at %q/%q — the last response's headers "+
				"are reachable from the pool", i, f.Name, f.Value)
		}
	}
}

// TestHeaderBlock_ReleaseParksTheCapacity is the half the allocation gate cannot
// see. Nilling a buffer instead of truncating it on the way into the pool is
// still CORRECT — the next block allocates and carries on — so no behavioural
// test can fail on it. Only a capacity check catches it, which is the same
// reason roundtripAllocCeiling exists next to the aliasing tests above.
func TestHeaderBlock_ReleaseParksTheCapacity(t *testing.T) {
	blk := copyFieldsToBlock([]header.Field{
		{Name: []byte("content-type"), Value: []byte("application/grpc")},
		{Name: []byte("x-request-id"), Value: []byte("0123456789abcdef")},
	})
	bytesGrown, fieldsGrown := cap(blk.buf), cap(blk.fields)
	if bytesGrown == 0 || fieldsGrown == 0 {
		t.Fatalf("block has capacity %d/%d after a copy went into it", bytesGrown, fieldsGrown)
	}

	blk.Release()

	if cap(blk.buf) < bytesGrown || cap(blk.fields) < fieldsGrown {
		t.Errorf("block went into the pool with capacity %d/%d, want the %d/%d it grew to — "+
			"Release is nilling instead of truncating, so every header block regrows it",
			cap(blk.buf), cap(blk.fields), bytesGrown, fieldsGrown)
	}
}

// TestResetForPool_AbandonsQueuedBlocks is the invariant #577 had to preserve
// rather than improve on: exactly ONE return site per block.
//
// Events still queued when a stream is recycled were never delivered, so the
// consumer that would have released them does not exist. resetForPoolLocked
// drops them to the garbage collector instead of releasing them — sync.Pool
// tolerates a buffer that is never Put — and that is what keeps the delivered
// event's consumer the single owner. Releasing here to reclaim the memory would
// trade one allocation for a double-Put the moment the two paths ever overlap.
//
// Release clears the field slice, so a block that survives the reset with its
// fields intact is a block that was abandoned rather than returned.
func TestResetForPool_AbandonsQueuedBlocks(t *testing.T) {
	s := newStream(1, 4, &fakeStreamWriter{}, 65535)
	blk := copyFieldsToBlock([]header.Field{{Name: []byte("k"), Value: []byte("v")}})
	if !s.push(StreamEvent{Type: EventHeaders, Headers: blk.Fields(), Block: blk}) {
		t.Fatal("push refused the event")
	}

	s.mu.Lock()
	s.resetForPoolLocked()
	s.mu.Unlock()

	if len(blk.Fields()) == 0 {
		t.Error("resetForPoolLocked released a queued block. It was never delivered, so " +
			"releasing it here adds a second return site for a buffer whose whole " +
			"discipline is having exactly one")
	}
}
