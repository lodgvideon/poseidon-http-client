package header

import "testing"

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
			if got := tc.f.Size(); got != tc.want {
				t.Errorf("Size() = %d, want %d (name %d + value %d + %d overhead)",
					got, tc.want, len(tc.f.Name), len(tc.f.Value), EntryOverhead)
			}
		})
	}
}

// TestEntryOverhead pins the constant itself. It is not a tuning knob: both
// specifications state 32, and a table that accounts with a different number
// disagrees with the peer about when an eviction happens.
func TestEntryOverhead(t *testing.T) {
	if EntryOverhead != 32 {
		t.Errorf("EntryOverhead = %d, want 32 (RFC 7541 §4.1, RFC 9204 §3.2.1)", EntryOverhead)
	}
}

// TestField_Sensitive covers the never-indexed mark, which is the one indexing
// mode with a security meaning: it tells an intermediary it must not index the
// field either and must preserve the representation when forwarding.
func TestField_Sensitive(t *testing.T) {
	for _, tc := range []struct {
		mode IndexingMode
		want bool
	}{
		{IndexIncremental, false},
		{IndexWithout, false},
		{IndexNever, true},
	} {
		if got := (Field{Indexing: tc.mode}).Sensitive(); got != tc.want {
			t.Errorf("Field{Indexing: %v}.Sensitive() = %v, want %v", tc.mode, got, tc.want)
		}
	}
}

// TestIndexIncrementalIsTheZeroValue pins that a Field built without naming an
// indexing mode indexes incrementally. Callers construct Field literals
// everywhere without setting it, so the zero value is the one every one of them
// gets.
func TestIndexIncrementalIsTheZeroValue(t *testing.T) {
	var f Field
	if f.Indexing != IndexIncremental {
		t.Errorf("zero Field has Indexing %v, want IndexIncremental", f.Indexing)
	}
}
