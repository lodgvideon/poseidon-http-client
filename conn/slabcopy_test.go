package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Every header field delivered to a caller is a view into one shared byte slab.
// That is cheap and it is safe only while each view's capacity stops at its own
// end: a two-index slice would run to the end of the slab, so appending to one
// header's value would overwrite the next header's bytes in place — silently,
// with no copy and no error.
//
// The response path clamped the capacity. The push-promise path, written as
// "same pattern as emitHeaderBlock", did not, and also dropped Indexing. Both
// now go through copyFieldsToSlab, and these are the gates on what that function
// has to guarantee.

// TestCopyFieldsToSlab_AppendCannotReachTheNextField is the gate for the bug. It
// does what a caller is entitled to do — append to a header it was given — and
// checks the neighbour is untouched.
func TestCopyFieldsToSlab_AppendCannotReachTheNextField(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte("first"), Value: []byte("AAAA")},
		{Name: []byte("second"), Value: []byte("BBBB")},
		{Name: []byte("third"), Value: []byte("CCCC")},
	}
	copied, slabPtr := copyFieldsToSlab(in)
	defer func() { *slabPtr = (*slabPtr)[:0]; headerSlabPool.Put(slabPtr) }()

	// A caller appending to the first field's value. With a clamped capacity this
	// allocates a fresh array; without one it writes straight over "second".
	_ = append(copied[0].Value, "XXXXXXXXXXXXXXXX"...) //nolint:gocritic // the point is the side effect on the slab

	for i, want := range in {
		if string(copied[i].Name) != string(want.Name) {
			t.Errorf("field %d name = %q after a neighbour was appended to, want %q — the "+
				"append reached into the shared slab", i, copied[i].Name, want.Name)
		}
		if string(copied[i].Value) != string(want.Value) {
			t.Errorf("field %d value = %q after a neighbour was appended to, want %q",
				i, copied[i].Value, want.Value)
		}
	}
}

// TestCopyFieldsToSlab_CapacityIsClamped states the same requirement directly,
// so a failure names the cause rather than the symptom.
func TestCopyFieldsToSlab_CapacityIsClamped(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte("a"), Value: []byte("bb")},
		{Name: []byte("ccc"), Value: []byte("dddd")},
	}
	copied, slabPtr := copyFieldsToSlab(in)
	defer func() { *slabPtr = (*slabPtr)[:0]; headerSlabPool.Put(slabPtr) }()

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

// TestCopyFieldsToSlab_PreservesIndexing covers the other half of the
// divergence: the push path set only Name and Value, so a promised header
// arrived with its indexing disposition zeroed.
func TestCopyFieldsToSlab_PreservesIndexing(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte("plain"), Value: []byte("1")},
		{Name: []byte("sensitive"), Value: []byte("2"), Indexing: hpack.IndexNever},
	}
	copied, slabPtr := copyFieldsToSlab(in)
	defer func() { *slabPtr = (*slabPtr)[:0]; headerSlabPool.Put(slabPtr) }()

	for i := range in {
		if copied[i].Indexing != in[i].Indexing {
			t.Errorf("field %d Indexing = %v, want %v — a never-indexed header that arrives "+
				"without that mark can be re-emitted into a dynamic table",
				i, copied[i].Indexing, in[i].Indexing)
		}
	}
}

// TestCopyFieldsToSlab_CopiesTheBytes is the basic contract: the result must not
// alias the input, or the caller would be handed the decoder's scratch.
func TestCopyFieldsToSlab_CopiesTheBytes(t *testing.T) {
	name := []byte("k")
	value := []byte("v")
	copied, slabPtr := copyFieldsToSlab([]hpack.HeaderField{{Name: name, Value: value}})
	defer func() { *slabPtr = (*slabPtr)[:0]; headerSlabPool.Put(slabPtr) }()

	name[0] = 'Z'
	value[0] = 'Z'
	if string(copied[0].Name) != "k" || string(copied[0].Value) != "v" {
		t.Errorf("got (%q, %q) after the source was overwritten, want (\"k\", \"v\") — the "+
			"fields alias the decoder's scratch instead of the slab",
			copied[0].Name, copied[0].Value)
	}
}

// TestCopyFieldsToSlab_Empty pins that no fields is not a special case for the
// caller: it still gets a slab it can return to the pool.
func TestCopyFieldsToSlab_Empty(t *testing.T) {
	copied, slabPtr := copyFieldsToSlab(nil)
	if slabPtr == nil {
		t.Fatal("no slab returned for an empty field list; the caller has nothing to put back")
	}
	defer func() { *slabPtr = (*slabPtr)[:0]; headerSlabPool.Put(slabPtr) }()
	if len(copied) != 0 {
		t.Errorf("got %d fields for an empty input", len(copied))
	}
}
