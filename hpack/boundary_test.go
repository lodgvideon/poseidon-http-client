package hpack

// boundary_test.go — the accepting side of five guards that were only ever
// tested past their limit (#755, #756, #771, #772, #776). Each of these
// comparisons is strict on purpose, and every one of them was reachable through
// the public surface with a mutant that narrowed it to >=: relaxing or
// tightening any of them turns a conformant peer's largest legal input into a
// connection error, or lets an unbounded one through.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecodeBlock_SizeUpdateAtExactlyTheAdvertisedLimit pins the accepting side
// of the §6.3 Dynamic Table Size Update gate. Two tests already cover a value
// strictly past the limit; nothing sat AT it, so tightening the comparison to
// >= left the suite green.
func TestDecodeBlock_SizeUpdateAtExactlyTheAdvertisedLimit(t *testing.T) {
	d := NewDecoder()
	d.SetMaxDynamicTableSize(20)

	// 0x20|20 = 0x34: §6.3 size update to exactly SETTINGS_HEADER_TABLE_SIZE.
	err := d.DecodeBlock([]byte{0x34}, func(HeaderField) error { return nil })

	require.NoError(t, err,
		"§6.3 permits a size update up to the value the decoder advertised, inclusive; rejecting the boundary turns conformant peer output into a connection error")
	assert.Equal(t, uint32(20), d.dt.maxSize,
		"the accepted update must actually resize the table — an update reported as applied but not applied leaves the two sides evicting at different points")
}

// TestDecodeBlock_LastStaticIndexResolvesToTheStaticTable pins the one line that
// decides whether a §6.1 index names the static table or the dynamic one. Every
// other test sits well inside one side; index 61 is the last static entry and 62
// the first dynamic one, and nothing pinned that the split falls between them.
//
// A decoder that draws the line one slot away from its peer still round-trips
// perfectly through this repository's own encoder while producing blocks no
// other HTTP/2 implementation can read.
func TestDecodeBlock_LastStaticIndexResolvesToTheStaticTable(t *testing.T) {
	d := NewDecoder()
	var got []HeaderField

	// 0x80|61 = 0xbd: §6.1 indexed header field naming the LAST static entry.
	err := d.DecodeBlock([]byte{0xbd}, func(f HeaderField) error {
		got = append(got, HeaderField{Name: append([]byte(nil), f.Name...)})
		return nil
	})

	require.NoError(t, err,
		"index 61 is the last static entry; a decoder that sends it to the dynamic table splits the two tables one slot away from every peer")
	require.Len(t, got, 1,
		"one indexed representation must emit exactly one field, or the name compared below came from a different octet")
	assert.Equal(t, "www-authenticate", string(got[0].Name),
		"static index 61 is www-authenticate (RFC 7541 Appendix A); resolving it anywhere else hands the caller a header the peer did not send")
}

// TestDynamicTable_EntryOfExactlyMaxSizeIsInserted pins the first §4.4 eviction
// rule at its boundary. The table is emptied for an entry LARGER than the
// maximum size; one of exactly the maximum fits and must stay indexable.
// The existing test uses 135 bytes against 50 — far past the comparison.
func TestDynamicTable_EntryOfExactlyMaxSizeIsInserted(t *testing.T) {
	// §4.1 charges len(name)+len(value)+32, so ("a","1") is exactly 34.
	dt := newDynamicTable(34)

	dt.add([]byte("a"), []byte("1"))

	require.Equal(t, 1, dt.len(),
		"§4.4 empties the table only for an entry larger than the maximum size; one of exactly the maximum fits, and discarding it drops an index the peer's encoder still believes is live")
	assert.Equal(t, uint32(34), dt.byteSize(),
		"the inserted entry must be accounted at its full §4.1 size, or the two sides disagree about when the next eviction fires")
	name, value := dt.at(1)
	assert.Equal(t, "a", string(name), "the entry kept at the boundary must still resolve by index")
	assert.Equal(t, "1", string(value), "the entry kept at the boundary must still carry its value")
}

// TestDynamicTable_EntriesExactlyFillingTheTableAreKept pins the second §4.4
// rule at its boundary: eviction runs only until the new entry fits, so a set of
// entries summing to exactly the maximum fits and nothing may be evicted. The
// existing test adds 102 bytes into 70 — past the comparison, not on it.
func TestDynamicTable_EntriesExactlyFillingTheTableAreKept(t *testing.T) {
	dt := newDynamicTable(68) // exactly two 34-byte entries

	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))

	require.Equal(t, 2, dt.len(),
		"§4.4 evicts only until the entry fits; a second entry that exactly fills the table fits, and evicting for it drops an entry the peer still indexes")
	assert.Equal(t, uint32(68), dt.byteSize(),
		"both entries must be accounted; a size that disagrees with the entries held makes every later eviction fire at the wrong moment")
	oldest, _ := dt.at(2)
	assert.Equal(t, "a", string(oldest),
		"the first entry must survive: it is the one an over-eager eviction would take, and index 2 is what the peer would send to reference it")
}

// TestDecoder_MaxHeaderListSize_ExactlyAtLimitAccepted pins the accepting side of
// the SETTINGS_MAX_HEADER_LIST_SIZE gate. RFC 7540 §6.5.2 defines the setting as
// the maximum size the sender is prepared to ACCEPT, so exactly that size must
// decode. The existing tests cover 220 bytes against 100, and the gate switched
// off entirely; a list sitting on the negotiated ceiling was never decoded.
func TestDecoder_MaxHeaderListSize_ExactlyAtLimitAccepted(t *testing.T) {
	d := NewDecoder()
	d.SetMaxHeaderListSize(34) // exactly one field of Size() == 1+1+32

	// 0x00 = §6.2.2 literal without indexing, new name "a", value "b".
	err := d.DecodeBlock([]byte{0x00, 0x01, 'a', 0x01, 'b'}, func(HeaderField) error { return nil })

	require.NoError(t, err,
		"the setting is the largest list we said we would ACCEPT (RFC 7540 §6.5.2); rejecting exactly that size refuses a conformant peer's largest legal request, and only on the requests that sit on the ceiling")
}

// TestDecodeInteger_ZeroPaddedContinuationRunIsBounded pins the §5.1 run-length
// bound, which is a different guard from the value bound beside it and the only
// one that can stop this input. Continuation octets carrying no payload leave
// the running value untouched, so neither `add>>m != chunk` nor `add > max-v`
// can ever fire: without the shift ceiling the peer decides how long the loop
// runs, which is exactly the unbounded-by-construction shape this repository's
// peer-input policy forbids.
func TestDecodeInteger_ZeroPaddedContinuationRunIsBounded(t *testing.T) {
	// m advances by 7 per octet against a ceiling of 32, so the sixth
	// continuation octet is the first that must be refused.
	src := []byte{0x1f, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}

	_, _, err := DecodeInteger(src, 5)

	require.ErrorIs(t, err, ErrIntegerOverflow,
		"a run of zero-payload continuation octets adds nothing to the value, so only the run-length bound can stop it; without that bound a peer decides how many octets this loop reads")
}

// TestDecodeInteger_ContinuationRunAtTheBoundaryDecodes is the partner of the
// test above: the longest legal run must still decode, so the pair pins a bound
// rather than a decoder that refuses continuation octets in general.
func TestDecodeInteger_ContinuationRunAtTheBoundaryDecodes(t *testing.T) {
	src := []byte{0x1f, 0x80, 0x80, 0x80, 0x80, 0x00} // one octet inside the bound

	v, n, err := DecodeInteger(src, 5)

	require.NoError(t, err,
		"the longest legal continuation run must still decode; refusing it turns a peer's well-formed §5.1 integer into a connection error")
	assert.Equal(t, uint64(31), v,
		"zero-payload continuation octets add nothing to the prefix value, so the result is the prefix itself")
	assert.Equal(t, 6, n,
		"every octet of the run must be consumed, or the next representation is parsed from the middle of this integer")
}
