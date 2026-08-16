package header

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestField_Size pins the dynamic-table entry-size rule. The same formula and
// the same 32 bytes appear in RFC 7541 §4.1 and RFC 9204 §3.2.1, which is why
// it lives here rather than in either codec — and why getting it wrong would
// mis-account both tables at once.
func TestField_Size(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    Field
		want uint32
	}{
		{"empty", Field{}, EntryOverhead},
		{"name only", Field{Name: []byte("abc")}, EntryOverhead + 3},
		{"value only", Field{Value: []byte("de")}, EntryOverhead + 2},
		{"both", Field{Name: []byte("content-type"), Value: []byte("text/plain")}, EntryOverhead + 22},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.f

			got := f.Size()

			assert.Equalf(t, tc.want, got,
				"Size() must be name %d + value %d + %d overhead; a table accounting with anything else disagrees with the peer about when an eviction happens",
				len(f.Name), len(f.Value), EntryOverhead)
		})
	}
}

// TestEntryOverhead pins the constant itself. It is not a tuning knob: both
// specifications state 32, and a table that accounts with a different number
// disagrees with the peer about when an eviction happens.
func TestEntryOverhead(t *testing.T) {
	const perSpec = 32

	got := EntryOverhead

	assert.Equal(t, perSpec, got,
		"EntryOverhead is fixed at 32 by RFC 7541 §4.1 and RFC 9204 §3.2.1; it is not a tuning knob")
}

// TestField_Sensitive covers the never-indexed mark, which is the one indexing
// mode with a security meaning: it tells an intermediary it must not index the
// field either and must preserve the representation when forwarding.
func TestField_Sensitive(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode IndexingMode
		want bool
	}{
		{"incremental", IndexIncremental, false},
		{"without", IndexWithout, false},
		{"never", IndexNever, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := Field{Indexing: tc.mode}

			got := f.Sensitive()

			assert.Equalf(t, tc.want, got,
				"Sensitive() must be true for IndexNever alone (%v here); a field wrongly reported sensitive is dropped from the dynamic table for no reason, and one wrongly reported insensitive is handed to an intermediary that may index it",
				tc.mode)
		})
	}
}

// TestIndexIncrementalIsTheZeroValue pins that a Field built without naming an
// indexing mode indexes incrementally. Callers construct Field literals
// everywhere without setting it, so the zero value is the one every one of them
// gets.
func TestIndexIncrementalIsTheZeroValue(t *testing.T) {
	var f Field

	got := f.Indexing

	assert.Equal(t, IndexIncremental, got,
		"the zero Field must index incrementally; every caller that builds a Field literal without naming a mode gets this one")
}
